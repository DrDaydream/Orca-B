use super::*;
use crate::common::{committee, keys, proposal};
use crate::messages::{batch_digest, local_votes_key, GrbcPhase};
use crate::primary::PrimaryMessage;
use bytes::Bytes;
use crypto::Signature;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

fn store(label: &str) -> Store {
    Store::new(&format!(
        ".db_test_proposal_storage_{}_{}",
        label,
        std::process::id()
    ))
    .unwrap()
}

fn with_payload() -> Proposal {
    let mut p = proposal();
    for bytes in [vec![7; 1024 * 1024], vec![9; 128 * 1024]] {
        let digest = batch_digest(&bytes);
        p.header.payload.insert(digest.clone(), 0);
        p.batches.insert(digest, bytes);
    }
    p.header.id = p.header.digest();
    p.header.signature = Signature::new(&p.header.id, &keys().pop().unwrap().1);
    p.verify(&committee()).unwrap();
    p
}

#[tokio::test]
async fn compact_header_and_shared_batches_reconstruct_complete_wire_proposal() {
    let mut store = store("roundtrip");
    let p = with_payload();
    let digest = p.block().digest();
    store_verified_proposal(&mut store, p.clone())
        .await
        .unwrap();
    let metadata = store
        .read(proposal_header_key(&digest))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata, bincode::serialize(&p.header).unwrap());
    assert!(metadata.len() < 1024);
    let old_key = [b"orca-proposal-v2/".as_ref(), digest.as_ref()].concat();
    assert!(store.read(old_key).await.unwrap().is_none());

    // A second signed proposal can reference exactly the same stored batches.
    let mut other = p.clone();
    other.header.round += 1;
    other.header.id = other.header.digest();
    other.header.signature = Signature::new(&other.header.id, &keys().pop().unwrap().1);
    other.verify(&committee()).unwrap();
    store_verified_proposal(&mut store, other.clone())
        .await
        .unwrap();
    let batch_entries = p
        .batches
        .iter()
        .map(|(d, b)| (batch_key(d), b.clone()))
        .collect();
    assert_eq!(store.write_batch_if_absent(batch_entries).await.unwrap(), 0);
    for expected in [p, other] {
        let rebuilt = load_proposal(&mut store, &expected.block().digest())
            .await
            .unwrap()
            .unwrap();
        rebuilt.verify(&committee()).unwrap();
        assert_eq!(
            bincode::serialize(&PrimaryMessage::Proposal(rebuilt)).unwrap(),
            bincode::serialize(&PrimaryMessage::Proposal(expected)).unwrap()
        );
    }
}

#[tokio::test]
async fn complete_header_notification_exposes_all_payload_bytes() {
    let mut store = store("notification");
    let p = with_payload();
    let digest = p.block().digest();
    let mut reader = store.clone();
    let expected = p.clone();
    let waiting = async move {
        reader
            .notify_read(proposal_header_key(&digest))
            .await
            .unwrap();
        let rebuilt = load_proposal(&mut reader, &digest).await.unwrap().unwrap();
        assert_eq!(rebuilt.batches, expected.batches);
    };
    let writing = store_verified_proposal(&mut store, p);
    let (_, result) = tokio::join!(waiting, writing);
    result.unwrap();
}

#[tokio::test]
async fn virtual_dependency_uses_header_without_loading_batch_bytes() {
    let mut store = store("metadata_only");
    let dependency = with_payload();
    let digest = dependency.block().digest();
    // Install only the trusted metadata fixture. Production writes this marker
    // atomically with data; omitting data here detects accidental payload reads.
    store
        .write(
            proposal_header_key(&digest),
            bincode::serialize(&dependency.header).unwrap(),
        )
        .await;
    let c = committee();
    let (tx_headers, _headers) = channel(10);
    let mut sync = crate::synchronizer::Synchronizer::new(&c, store.clone(), tx_headers);
    let mut child = proposal().header;
    child.virtual_edges.insert(digest.clone());
    let parents = sync
        .get_parents(&child, &Default::default(), &Default::default())
        .await
        .unwrap();
    assert!(parents
        .iter()
        .any(|parent| parent.header.id == dependency.header.id));
    // A recovery reply must still refuse to send an incomplete proposal.
    assert!(matches!(
        load_proposal(&mut store, &digest).await,
        Err(DagError::MissingBatch(_))
    ));
}

#[tokio::test]
async fn helper_recovers_complete_proposal_before_local_ready() {
    let mut store = store("helper");
    let p = with_payload();
    let digest = p.block().digest();
    store_verified_proposal(&mut store, p.clone())
        .await
        .unwrap();
    let vote = crate::common::votes(&p.header, GrbcPhase::Echo).remove(0);
    store
        .write(
            local_votes_key(&digest),
            bincode::serialize(&vec![vote.clone()]).unwrap(),
        )
        .await;
    let mut c = committee();
    let requestor = keys()[1].0;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    c.authorities
        .get_mut(&requestor)
        .unwrap()
        .primary
        .primary_to_primary = listener.local_addr().unwrap();
    let receiver = tokio::spawn(async move {
        let mut frames = Vec::new();
        let mut streams = Vec::new();
        // Both responses must arrive on separate connections, in either order.
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
            frames.push(stream.next().await.unwrap().unwrap());
            stream.send(Bytes::from_static(b"Ack")).await.unwrap();
            // Keep it alive: ReliableSender otherwise reconnects immediately,
            // and that idle reconnect could be mistaken for the data connection.
            streams.push(stream);
        }
        frames
    });
    let (tx, rx) = channel(10);
    let (tx_votes, rx_votes) = channel(10);
    crate::vote_sender::spawn_vote_sender(
        keys()[0].0,
        c.clone(),
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        50,
        rx_votes,
    );
    crate::helper::Helper::spawn(c, store, rx, tx_votes);
    tx.send((vec![digest], requestor)).await.unwrap();
    let frames = tokio::time::timeout(std::time::Duration::from_secs(5), receiver)
        .await
        .unwrap()
        .unwrap();
    let (mut saw_proposal, mut saw_vote) = (false, false);
    for bytes in frames {
        match bincode::deserialize::<PrimaryMessage>(&bytes).unwrap() {
            PrimaryMessage::Proposal(rebuilt) => {
                rebuilt.verify(&committee()).unwrap();
                assert_eq!(rebuilt.header, p.header);
                assert_eq!(rebuilt.batches, p.batches);
                saw_proposal = true;
            }
            PrimaryMessage::GrbcVoteBatch(batch) => {
                let recovered = batch.into_votes();
                assert_eq!(recovered.len(), 1);
                recovered[0].verify(&committee()).unwrap();
                assert_eq!(
                    bincode::serialize(&recovered[0]).unwrap(),
                    bincode::serialize(&vote).unwrap()
                );
                saw_vote = true;
            }
            _ => panic!("Unexpected recovery reply"),
        }
    }
    assert!(saw_proposal && saw_vote);
}

#[tokio::test]
async fn proposal_processor_rejects_changed_bytes_even_for_a_known_header() {
    let mut store = store("processor_revalidation");
    let p = with_payload();
    let digest = p.block().digest();
    let prepare = crate::proposal_processor::prepare_proposal;
    let (stored, _) = prepare(p.clone(), committee(), &mut store, false, None)
        .await
        .unwrap();
    assert_eq!(stored.into_header(), p.header);
    let mut forged = p.clone();
    forged.batches.values_mut().next().unwrap()[0] ^= 1;
    assert!(prepare(forged, committee(), &mut store, false, None)
        .await
        .is_err());
    let restored = load_proposal(&mut store, &digest).await.unwrap().unwrap();
    assert_eq!(restored.batches, p.batches);
    restored.verify(&committee()).unwrap();
}
