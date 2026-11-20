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
async fn local_proposal_broadcast_does_not_wait_for_storage_but_delivery_does() {
    use crate::proposal_processor::{spawn_proposal_processors, ProposalTracker};
    use std::sync::{atomic::AtomicU64, Arc};
    use store::StoreCommand;
    use tokio::time::{timeout, Duration};

    let p = with_payload();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = committee();
    for authority in c.authorities.values_mut() {
        authority.primary.primary_to_primary = listener.local_addr().unwrap();
    }
    let (tx_store, mut rx_store) = channel(10);
    let (tx_local, rx_local) = channel(1);
    let (_tx_remote, rx_remote) = channel(1);
    let (tx_stored, mut rx_stored) = channel(1);
    let tracker = ProposalTracker::default();
    spawn_proposal_processors(
        p.header.author,
        c,
        Store::from_channel(tx_store),
        Arc::new(AtomicU64::new(0)),
        50,
        rx_remote,
        rx_local,
        tx_stored,
        tracker.clone(),
    );
    tx_local.send(p.header.clone()).await.unwrap();

    // Serve the already sealed local batches, then hold the atomic write ACK.
    let ack = loop {
        match timeout(Duration::from_secs(5), rx_store.recv())
            .await
            .unwrap()
            .unwrap()
        {
            StoreCommand::Read(key, response) => {
                let bytes = p
                    .batches
                    .iter()
                    .find(|(d, _)| batch_key(d) == key)
                    .unwrap()
                    .1;
                response.send(Ok(Some(bytes.clone()))).unwrap();
            }
            StoreCommand::WriteBatchIfAbsent(entries, response) => {
                assert_eq!(entries.len(), p.batches.len() + 1);
                assert_eq!(
                    entries[&proposal_header_key(&p.block().digest())],
                    bincode::serialize(&p.header).unwrap()
                );
                break response;
            }
            _ => panic!("Unexpected storage operation"),
        }
    };
    let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("Proposal broadcast must not wait for the local storage ACK")
        .unwrap();
    let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
    let wire = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        wire.as_ref(),
        bincode::serialize(&PrimaryMessage::Proposal(p.clone())).unwrap()
    );
    stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    assert!(
        rx_stored.try_recv().is_err(),
        "Core must not ECHO before storage succeeds"
    );
    assert!(tracker.contains(&p.block().digest()));
    ack.send(Ok(p.batches.len() + 1)).unwrap();
    let stored = timeout(Duration::from_secs(2), rx_stored.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.into_header(), p.header);
    assert!(!tracker.contains(&p.block().digest()));
}

#[tokio::test]
async fn runtime_review_two_local_rounds_progress_while_peer_enqueue_is_saturated() {
    use crate::proposal_processor::{prepare_local_proposal, ProposalTracker};
    use network::{DelayBudget, Delivery, DeliveryPolicy, ReliableSender};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct CountProposalDecisions {
        wire: Bytes,
        calls: AtomicUsize,
    }
    impl DeliveryPolicy for CountProposalDecisions {
        fn delivery(&self, _: std::net::SocketAddr, data: &Bytes) -> Delivery {
            if data == &self.wire {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Delivery::Delay(Duration::from_secs(1), String::new())
            } else {
                Delivery::Send
            }
        }
    }
    use store::StoreCommand;
    use tokio::time::{timeout, Duration};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let p = with_payload();
    let mut replay_store = store("released_proposal_retry");
    store_verified_proposal(&mut replay_store, p.clone())
        .await
        .unwrap();
    let policy = Arc::new(CountProposalDecisions {
        wire: Bytes::from(bincode::serialize(&PrimaryMessage::Proposal(p.clone())).unwrap()),
        calls: AtomicUsize::new(0),
    });
    // No delay budget remains, so the policy releases the initial attempt.
    let sender = || ReliableSender::with_schedule(policy.clone(), DelayBudget::new(0, 0));
    let mut network = sender();
    // All queued messages share this allocation. A connected peer which does
    // not read blocks the transport write and then its bounded enqueue channel.
    let filler = Bytes::from(vec![0; 8 * 1024 * 1024]);
    let mut queued = vec![network.send(address, filler.clone()).await];
    let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let peer = Framed::new(socket, LengthDelimitedCodec::new());
    let mut saturated = false;
    for _ in 0..1100 {
        match timeout(
            Duration::from_millis(50),
            network.send(address, filler.clone()),
        )
        .await
        {
            Ok(handler) => queued.push(handler),
            Err(_) => {
                saturated = true;
                break;
            }
        }
    }
    assert!(
        saturated,
        "Fixture must saturate the real peer enqueue channel"
    );

    let round = p.header.round;
    let digest = p.block().digest();
    let tracker = ProposalTracker::default();
    let guard = tracker.track(&p.header);
    let (tx_store, mut rx_store) = channel(1);
    let mut storage = Store::from_channel(tx_store);
    let (tx_stored, mut rx_stored) = channel(1);
    let mut next = p.clone();
    next.header.round += 1;
    next.header.id = next.header.digest();
    next.header.signature = Signature::new(&next.header.id, &keys().pop().unwrap().1);
    let first_header = p.header.clone();
    let second_header = next.header.clone();
    let next_guard = tracker.track(&next.header);
    let prepare = async {
        for (proposal, guard) in [(p, guard), (next, next_guard)] {
            let (accepted, deferred) = prepare_local_proposal(
                proposal,
                committee(),
                &mut storage,
                (&mut network, vec![address]),
                guard,
                &tx_stored,
            )
            .await
            .unwrap();
            assert!(accepted.is_empty());
            assert_eq!(deferred, vec![address]);
        }
    };
    let observe = async {
        for header in [first_header, second_header] {
            let ack = match rx_store.recv().await.unwrap() {
                StoreCommand::WriteBatchIfAbsent(_, ack) => ack,
                _ => panic!("Unexpected storage operation"),
            };
            assert!(rx_stored.try_recv().is_err());
            ack.send(Ok(3)).unwrap();
            assert_eq!(rx_stored.recv().await.unwrap().into_header(), header);
        }
    };
    timeout(Duration::from_secs(3), async {
        tokio::join!(prepare, observe)
    })
    .await
    .expect("Both stored local rounds must progress before the peer queue drains");
    assert!(!tracker.contains(&digest));
    assert_eq!(policy.calls.load(Ordering::Relaxed), 1);
    let retry = crate::proposal_processor::spawn_proposal_retry(
        sender(),
        address,
        replay_store,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        50,
    );
    retry.send((round, digest)).await.unwrap();
    let received = async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
        assert_eq!(frames.next().await.unwrap().unwrap().freeze(), policy.wire);
        frames.send(Bytes::from_static(b"Ack")).await.unwrap();
    };
    timeout(Duration::from_secs(3), received).await.unwrap();
    assert_eq!(
        policy.calls.load(Ordering::Relaxed),
        1,
        "transport retry repeated the released scheduling decision"
    );
    drop(peer);
    drop(queued);
}

#[tokio::test]
async fn local_proposal_storage_failure_returns_while_peer_enqueue_is_saturated() {
    use crate::proposal_processor::{prepare_local_proposal, ProposalTracker};
    use network::ReliableSender;
    use store::StoreCommand;
    use tokio::time::{timeout, Duration};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut network = ReliableSender::new();
    let filler = Bytes::from(vec![0; 8 * 1024 * 1024]);
    let mut queued = vec![network.send(address, filler.clone()).await];
    // Keep the connected peer alive without reading so the real transport
    // write and bounded enqueue channel stay blocked throughout the test.
    let (_socket, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut saturated = false;
    for _ in 0..1100 {
        match timeout(
            Duration::from_millis(50),
            network.send(address, filler.clone()),
        )
        .await
        {
            Ok(handler) => queued.push(handler),
            Err(_) => {
                saturated = true;
                break;
            }
        }
    }
    assert!(
        saturated,
        "Fixture must saturate the real peer enqueue channel"
    );

    let p = with_payload();
    let digest = p.block().digest();
    let tracker = ProposalTracker::default();
    let guard = tracker.track(&p.header);
    let (tx_store, mut rx_store) = channel(1);
    let mut storage = Store::from_channel(tx_store);
    let (tx_stored, mut rx_stored) = channel(1);
    let prepare = prepare_local_proposal(
        p,
        committee(),
        &mut storage,
        (&mut network, vec![address]),
        guard,
        &tx_stored,
    );
    let fail_write = async {
        match rx_store.recv().await.unwrap() {
            StoreCommand::WriteBatchIfAbsent(_, ack) => {
                assert!(rx_stored.try_recv().is_err());
                assert!(tracker.contains(&digest));
                ack.send(Err(Store::new("\0").err().unwrap())).unwrap();
            }
            _ => panic!("Unexpected storage operation"),
        }
    };
    let (result, ()) = timeout(Duration::from_secs(2), async {
        tokio::join!(prepare, fail_write)
    })
    .await
    .expect("Storage failure must return before the saturated peer queue unblocks");
    assert!(matches!(result, Err(DagError::StoreError(_))));
    assert!(
        rx_stored.try_recv().is_err(),
        "Failed storage must never publish"
    );
    assert!(
        !tracker.contains(&digest),
        "Failed storage must release its guard"
    );
    assert!(
        timeout(Duration::from_millis(50), network.send(address, filler))
            .await
            .is_err(),
        "Peer enqueue must still be saturated after the storage error returns"
    );
    drop(queued);
}

#[tokio::test]
async fn local_proposal_rejects_invalid_bytes_before_storage_or_publication() {
    use crate::proposal_processor::{prepare_local_proposal, ProposalTracker};
    use network::ReliableSender;

    let mut p = with_payload();
    p.batches.values_mut().next().unwrap()[0] ^= 1;
    let digest = p.block().digest();
    let tracker = ProposalTracker::default();
    let guard = tracker.track(&p.header);
    let (tx_store, mut rx_store) = channel(1);
    let mut storage = Store::from_channel(tx_store);
    let (tx_stored, mut rx_stored) = channel(1);
    let mut network = ReliableSender::new();
    assert!(prepare_local_proposal(
        p,
        committee(),
        &mut storage,
        (&mut network, Vec::new()),
        guard,
        &tx_stored
    )
    .await
    .is_err());
    assert!(rx_store.try_recv().is_err());
    assert!(rx_stored.try_recv().is_err());
    assert!(!tracker.contains(&digest));
}

#[tokio::test]
async fn local_proposal_failed_storage_does_not_publish() {
    use crate::proposal_processor::{prepare_local_proposal, ProposalTracker};
    use network::ReliableSender;
    use store::StoreCommand;

    let p = with_payload();
    let digest = p.block().digest();
    let tracker = ProposalTracker::default();
    let guard = tracker.track(&p.header);
    let (tx_store, mut rx_store) = channel(1);
    let mut storage = Store::from_channel(tx_store);
    let (tx_stored, mut rx_stored) = channel(1);
    let mut network = ReliableSender::new();
    let prepare = prepare_local_proposal(
        p,
        committee(),
        &mut storage,
        (&mut network, Vec::new()),
        guard,
        &tx_stored,
    );
    let fail_write = async {
        match rx_store.recv().await.unwrap() {
            StoreCommand::WriteBatchIfAbsent(_, ack) => {
                assert!(rx_stored.try_recv().is_err());
                ack.send(Err(Store::new("\0").err().unwrap())).unwrap();
            }
            _ => panic!("Unexpected storage operation"),
        }
    };
    let (result, ()) = tokio::join!(prepare, fail_write);
    assert!(matches!(result, Err(DagError::StoreError(_))));
    assert!(rx_stored.try_recv().is_err());
    assert!(!tracker.contains(&digest));
}

#[tokio::test]
async fn failed_proposal_storage_does_not_deliver_header_and_releases_tracker() {
    use crate::proposal_processor::{prepare_proposal, ProposalTracker};
    use network::ReliableSender;
    use store::StoreCommand;

    let p = with_payload();
    let digest = p.block().digest();
    let tracker = ProposalTracker::default();
    let guard = tracker.track(&p.header);
    let (tx_store, mut rx_store) = channel(1);
    let mut store = Store::from_channel(tx_store);
    let mut network = ReliableSender::new();
    let prepare = prepare_proposal(
        p,
        committee(),
        &mut store,
        Some((&mut network, Vec::new())),
        Some(guard),
    );
    let fail_write = async {
        match rx_store.recv().await.unwrap() {
            StoreCommand::WriteBatchIfAbsent(_, ack) => {
                // RocksDB's error constructor is private; an invalid path
                // supplies a real storage error without creating another DB.
                let error = Store::new("\0").err().unwrap();
                ack.send(Err(error)).unwrap();
            }
            _ => panic!("Unexpected storage operation"),
        }
    };
    let (result, ()) = tokio::join!(prepare, fail_write);
    assert!(matches!(result, Err(DagError::StoreError(_))));
    assert!(!tracker.contains(&digest));
}

#[test]
fn complete_proposal_decoding_reads_payloads_in_bulk() {
    struct CountingReader<'a> {
        bytes: &'a [u8],
        calls: usize,
    }
    impl std::io::Read for CountingReader<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.calls += 1;
            std::io::Read::read(&mut self.bytes, buffer)
        }
    }
    let p = with_payload();
    let wire = bincode::serialize(&p).unwrap();
    let mut reader = CountingReader {
        bytes: &wire,
        calls: 0,
    };
    let decoded: Proposal = bincode::deserialize_from(&mut reader).unwrap();
    assert_eq!(decoded.header, p.header);
    assert_eq!(decoded.batches, p.batches);
    assert!(reader.bytes.is_empty());
    assert!(
        reader.calls < 1024,
        "Payload decoding must not perform one read per byte; used {} reads",
        reader.calls
    );
}

#[test]
fn complete_proposal_codec_preserves_legacy_bytes_and_rejects_truncation() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct LegacyProposal {
        header: Header,
        batches: std::collections::BTreeMap<Digest, Vec<u8>>,
    }
    for p in [proposal(), with_payload()] {
        let legacy = LegacyProposal {
            header: p.header.clone(),
            batches: p.batches.clone(),
        };
        let wire = bincode::serialize(&legacy).unwrap();
        assert_eq!(bincode::serialize(&p).unwrap(), wire);
        assert_eq!(bincode::serialized_size(&p).unwrap(), wire.len() as u64);
        let decoded: Proposal = bincode::deserialize(&wire).unwrap();
        assert_eq!(decoded.header, p.header);
        assert_eq!(decoded.batches, p.batches);
        let restored: LegacyProposal =
            bincode::deserialize(&bincode::serialize(&p).unwrap()).unwrap();
        assert_eq!(restored.header, p.header);
        assert_eq!(restored.batches, p.batches);
        assert!(bincode::deserialize::<Proposal>(&wire[..wire.len() - 1]).is_err());
    }
}

#[test]
fn complete_proposal_codec_preserves_duplicate_key_resolution() {
    // A malformed map can repeat a digest: retain BTreeMap's last-value behavior.
    let mut wire = bincode::serialize(&Header::default()).unwrap();
    wire.extend_from_slice(&2u64.to_le_bytes());
    let digest = Digest([7; 32]);
    for bytes in [vec![1u8; 16], vec![2u8; 32]] {
        wire.extend_from_slice(&bincode::serialize(&(digest.clone(), bytes)).unwrap());
    }
    let decoded: Proposal = bincode::deserialize(&wire).unwrap();
    assert_eq!(decoded.batches.len(), 1);
    assert_eq!(decoded.batches[&digest], vec![2u8; 32]);
}

#[test]
fn complete_proposal_slice_decoder_rejects_an_impossible_payload_length() {
    // Construct the existing layout up to the first batch length. The frame
    // contains no payload bytes; decoding must fail before allocating the claim.
    let mut wire = bincode::serialize(&Header::default()).unwrap();
    wire.extend_from_slice(&1u64.to_le_bytes());
    wire.extend_from_slice(&bincode::serialize(&Digest([0; 32])).unwrap());
    wire.extend_from_slice(&u64::MAX.to_le_bytes());
    assert!(bincode::deserialize::<Proposal>(&wire).is_err());
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
async fn proposal_repair_rejects_metadata_without_batch_bytes() {
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
    tx.send(crate::helper::RepairRequest::Proposals(
        vec![digest],
        requestor,
    ))
    .await
    .unwrap();
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
                let c = committee();
                let verifier = crypto::SignatureVerifier::new(c.authorities.keys().copied());
                recovered[0].verify(&c, &verifier).unwrap();
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
    let (stored, _) = prepare(p.clone(), committee(), &mut store, None, None)
        .await
        .unwrap();
    assert_eq!(stored.into_header(), p.header);
    let mut forged = p.clone();
    forged.batches.values_mut().next().unwrap()[0] ^= 1;
    assert!(prepare(forged, committee(), &mut store, None, None)
        .await
        .is_err());
    let restored = load_proposal(&mut store, &digest).await.unwrap().unwrap();
    assert_eq!(restored.batches, p.batches);
    restored.verify(&committee()).unwrap();
}

#[tokio::test]
async fn remote_processor_delivers_known_proposals_once_and_rejects_invalid_first_copy() {
    use crate::proposal_processor::{spawn_proposal_processors, ProposalTracker};
    use std::sync::{atomic::AtomicU64, Arc};
    use tokio::time::{timeout, Duration};

    let mut storage = store("remote_duplicates");
    let tracker = ProposalTracker::default();
    let (tx_remote, rx_remote) = channel(10);
    let (tx_local, rx_local) = channel(1);
    let (tx_stored, mut rx_stored) = channel(10);
    spawn_proposal_processors(
        keys()[0].0,
        committee(),
        storage.clone(),
        Arc::new(AtomicU64::new(0)),
        50,
        rx_remote,
        rx_local,
        tx_stored,
        tracker.clone(),
    );
    drop(tx_local);
    let p = with_payload();
    let mut invalid = p.clone();
    invalid.batches.values_mut().next().unwrap()[0] ^= 1;
    let mut next = p.clone();
    next.header.round += 1;
    next.header.id = next.header.digest();
    next.header.signature = Signature::new(&next.header.id, &keys().pop().unwrap().1);
    for proposal in [invalid.clone(), p.clone(), p.clone(), invalid, next.clone()] {
        let guard = tracker.track(&proposal.header);
        tx_remote.send((proposal, guard)).await.unwrap();
    }
    drop(tx_remote);
    let mut delivered = Vec::new();
    while let Some(stored) = timeout(Duration::from_secs(5), rx_stored.recv())
        .await
        .unwrap()
    {
        delivered.push(stored.into_header().id);
    }
    assert_eq!(delivered, vec![p.header.id.clone(), next.header.id.clone()]);
    for expected in [p, next] {
        let digest = expected.block().digest();
        assert!(!tracker.contains(&digest));
        let recovered = load_proposal(&mut storage, &digest).await.unwrap().unwrap();
        assert_eq!(recovered.header, expected.header);
        assert_eq!(recovered.batches, expected.batches);
    }
}

#[tokio::test]
async fn remote_duplicate_cache_eviction_and_restart_do_not_prevent_recovery() {
    use crate::proposal_processor::{spawn_proposal_processors, ProposalTracker};
    use std::sync::{atomic::AtomicU64, Arc};
    use tokio::time::{timeout, Duration};

    let storage = store("remote_cache_eviction");
    let tracker = ProposalTracker::default();
    let p = proposal();
    // One round of cache capacity; more distinct proposals must evict old IDs.
    // Recreating the processors against the same store must redeliver metadata.
    for _ in 0..2 {
        let (tx_remote, rx_remote) = channel(1);
        let (tx_local, rx_local) = channel(1);
        let (tx_stored, mut rx_stored) = channel(1);
        spawn_proposal_processors(
            keys()[0].0,
            committee(),
            storage.clone(),
            Arc::new(AtomicU64::new(0)),
            1,
            rx_remote,
            rx_local,
            tx_stored,
            tracker.clone(),
        );
        drop(tx_local);
        let rounds = (1..=committee().size() as u64 + 1).chain(std::iter::once(1));
        for round in rounds {
            let mut expected = p.clone();
            expected.header.round = round;
            expected.header.id = expected.header.digest();
            expected.header.signature =
                Signature::new(&expected.header.id, &keys().pop().unwrap().1);
            let guard = tracker.track(&expected.header);
            tx_remote.send((expected.clone(), guard)).await.unwrap();
            let stored = timeout(Duration::from_secs(5), rx_stored.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.into_header(), expected.header);
            assert!(!tracker.contains(&expected.block().digest()));
        }
        drop(tx_remote);
        assert!(timeout(Duration::from_secs(5), rx_stored.recv())
            .await
            .unwrap()
            .is_none());
    }
}
