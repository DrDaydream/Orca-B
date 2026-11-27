use super::*;
use crate::common::{committee, keys, proposal, votes};
use crate::messages::{batch_digest, GrbcPhase};
use crate::vote_sender::VoteDispatch;
use crypto::{Hash as _, Signature};
use network::ReliableSender;
use tokio::net::TcpListener;

#[tokio::test]
async fn dependency_repair_with_local_payload_does_not_download_it_again() {
    use crate::header_waiter::{HeaderWaiter, WaiterMessage};
    use crate::messages::local_votes_key;
    use crate::proposal_store::store_verified_proposal;
    use futures::{SinkExt, StreamExt};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    let mut c = committee();
    let name = keys()[0].0;
    let peer = keys()[1].0;
    let replies = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let requests = TcpListener::bind("127.0.0.1:0").await.unwrap();
    c.authorities
        .get_mut(&name)
        .unwrap()
        .primary
        .primary_to_primary = replies.local_addr().unwrap();
    c.authorities
        .get_mut(&peer)
        .unwrap()
        .primary
        .primary_to_primary = requests.local_addr().unwrap();
    let mut p = proposal();
    let payload = vec![7; 512 * 1024];
    let batch = batch_digest(&payload);
    p.header.payload.insert(batch.clone(), 0);
    p.header.id = p.header.digest();
    p.header.signature = Signature::new(&p.header.id, &keys().pop().unwrap().1);
    p.verify(&committee()).unwrap();
    let digest = p.block().digest();
    let mut local =
        Store::new(&format!(".db_test_vote_only_local_{}", std::process::id())).unwrap();
    let mut remote =
        Store::new(&format!(".db_test_vote_only_remote_{}", std::process::id())).unwrap();
    store_verified_proposal(&mut local, p.header.clone())
        .await
        .unwrap();
    store_verified_proposal(&mut remote, p.header.clone())
        .await
        .unwrap();
    let vote = votes(&p.header, GrbcPhase::Echo).remove(1);
    remote
        .write(
            local_votes_key(&digest),
            bincode::serialize(&vec![vote.clone()]).unwrap(),
        )
        .await;
    let (tx_cert_requests, rx_requests) = channel(10);
    let (tx_votes, rx_votes) = channel(10);
    spawn_vote_sender(peer, c.clone(), Arc::new(AtomicU64::new(0)), 50, rx_votes);
    crate::helper::Helper::spawn(c.clone(), remote, rx_requests, tx_votes);
    let handler = PrimaryReceiverHandler {
        tx_primary_messages: channel(10).0,
        tx_remote_proposals: channel(10).0,
        in_flight: ProposalTracker::default(),
        tx_cert_requests,
    };
    let ingress = tokio::spawn(async move {
        let (socket, _) = requests.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        while let Some(Ok(bytes)) = reader.next().await {
            handler.dispatch(&mut writer, bytes.freeze()).await.unwrap();
        }
    });
    let (tx_sync, rx_sync) = channel(10);
    let (tx_core, mut rx_core) = channel(10);
    HeaderWaiter::spawn(
        name,
        c,
        local.clone(),
        Arc::new(AtomicU64::new(0)),
        50,
        10_000,
        3,
        rx_sync,
        tx_core,
    );
    let mut child = proposal().header;
    child.author = peer;
    child.parents = [digest.clone()].iter().cloned().collect();
    tx_sync
        .send(WaiterMessage::SyncParents(
            vec![digest.clone()],
            Vec::new(),
            child.clone(),
        ))
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), async {
        let (socket, _) = replies.accept().await.unwrap();
        let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
        let bytes = stream.next().await.unwrap().unwrap();
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
        (
            bincode::deserialize::<PrimaryMessage>(&bytes).unwrap(),
            stream,
        )
    })
    .await
    .unwrap();
    let (message, _keep_connection_alive) = received;
    match message {
        PrimaryMessage::GrbcVoteBatch(batch) => assert_eq!(batch.into_votes(), vec![vote]),
        _ => panic!("A node with complete payload must only receive repair votes"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(200), replies.accept())
            .await
            .is_err(),
        "Dependency repair must not open a second connection to resend the full proposal"
    );
    assert!(
        rx_core.try_recv().is_err(),
        "Payload presence alone must not satisfy FINISH"
    );
    local.write(digest.to_vec(), vec![1]).await;
    let resumed = tokio::time::timeout(Duration::from_secs(2), rx_core.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.id, child.id);
    ingress.abort();
}

#[tokio::test]
async fn echo_and_ready_bypass_a_blocked_proposal_connection() {
    let mut c = committee();
    let peer = keys()[1].0;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    c.authorities
        .get_mut(&peer)
        .unwrap()
        .primary
        .primary_to_primary = address;
    let (tx_primary_messages, mut rx_control) = channel(10);
    let (tx_remote_proposals, mut rx_bulk) = channel(1);
    let (tx_cert_requests, _rx_requests) = channel(10);
    let in_flight = ProposalTracker::default();
    // Fill the bulk queue and deliberately never run its consumer.
    let first = proposal();
    let guard = in_flight.track(&first.header);
    tx_remote_proposals
        .send((first.header, guard))
        .await
        .unwrap();
    let handler = PrimaryReceiverHandler {
        tx_primary_messages,
        tx_remote_proposals,
        tx_cert_requests,
        in_flight: in_flight.clone(),
    };
    // Use the production handler, with an independent dispatch task per socket.
    let accept = tokio::spawn(async move {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::{Framed, LengthDelimitedCodec};
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let handler = handler.clone();
            tokio::spawn(async move {
                let (mut writer, mut reader) =
                    Framed::new(socket, LengthDelimitedCodec::new()).split();
                while let Some(Ok(bytes)) = reader.next().await {
                    handler.dispatch(&mut writer, bytes.freeze()).await.unwrap();
                }
                let _ = writer.close().await;
            });
        }
    });
    let mut bulk_network = ReliableSender::new();
    let mut p = proposal();
    let batch = vec![42; 512 * 1024];
    let digest = batch_digest(&batch);
    p.header.payload.insert(digest.clone(), 0);
    p.header.id = p.header.digest();
    p.header.signature = Signature::new(&p.header.id, &keys().pop().unwrap().1);
    p.verify(&committee()).unwrap();
    let ack = bulk_network
        .send(
            address,
            Bytes::from(bincode::serialize(&PrimaryMessage::Header(p.header.clone())).unwrap()),
        )
        .await;
    // The bulk handler ACKs its frame, then blocks on the full queue.
    tokio::time::timeout(Duration::from_secs(5), ack)
        .await
        .unwrap()
        .unwrap();
    // ACK precedes blocking-task decoding. Wait for decoded ingress so this
    // checks the full-queue case, not an incidental decoding/transport race.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !in_flight.contains(&p.block().digest()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (tx_votes, rx_votes) = channel(10);
    spawn_vote_sender(keys()[0].0, c, Arc::new(AtomicU64::new(0)), 50, rx_votes);
    for phase in [GrbcPhase::Echo, GrbcPhase::Ready] {
        tx_votes
            .send(VoteDispatch {
                requestor: Some(peer),
                votes: vec![votes(&p.header, phase).remove(0)],
            })
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(5), rx_control.recv())
            .await
            .unwrap()
            .unwrap();
        match message {
            PrimaryMessage::GrbcVoteBatch(batch) => {
                assert_eq!(batch.phase, phase);
                let votes = batch.into_votes();
                assert_eq!(votes.len(), 1);
                let c = committee();
                let verifier = crypto::SignatureVerifier::new(c.authorities.keys().copied());
                votes[0].verify(&c, &verifier).unwrap();
            }
            _ => panic!("Expected vote over independent connection"),
        }
    }
    // Only now release the intentionally blocked proposal consumer.
    assert!(in_flight.contains(&p.block().digest()));
    rx_bulk.recv().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), rx_bulk.recv())
        .await
        .unwrap()
        .unwrap();
    accept.abort();
}

#[tokio::test]
async fn queue_invariant_saturated_aba_routes_committed_feedback_and_replays_fifo() {
    let (tx_feedback, rx_feedback) = channel(1);
    let (tx_core, mut rx_core) = channel(1);
    let (tx_aba, mut rx_aba) = channel(1);
    tx_aba.send(vec![vec![0]]).await.unwrap();
    spawn_consensus_router(rx_feedback, tx_core, tx_aba);
    for byte in 1..=3 {
        tx_feedback
            .send(ConsensusCommand::AbaBroadcast(vec![vec![byte]]))
            .await
            .unwrap();
    }
    // The bounded feedback sender has handed its last command to the router.
    drop(tx_feedback.reserve().await.unwrap());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let block = proposal().block();
    let expected = block.digest();
    tx_feedback
        .send(ConsensusCommand::CleanupBatch(vec![block]))
        .await
        .unwrap();
    let feedback = tokio::time::timeout(Duration::from_secs(2), rx_core.recv())
        .await
        .expect("a queued commit waited for outbound ABA capacity")
        .unwrap();
    assert!(
        matches!(feedback, ConsensusCommand::CleanupBatch(blocks) if blocks[0].digest() == expected)
    );
    drop(tx_feedback);
    let replay = tokio::time::timeout(Duration::from_secs(2), async {
        let mut replay = Vec::new();
        while let Some(batch) = rx_aba.recv().await {
            replay.extend(batch);
        }
        replay
    })
    .await
    .unwrap();
    assert_eq!(replay, vec![vec![0], vec![1], vec![2], vec![3]]);
}
