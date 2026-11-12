use super::*;
use crate::common::{committee, keys, proposal, votes};
use crate::messages::{batch_digest, GrbcPhase};
use crate::vote_sender::VoteDispatch;
use crypto::{Hash as _, Signature};
use tokio::net::TcpListener;

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
    // Fill the bulk queue and deliberately never run its consumer.
    tx_remote_proposals.send(proposal()).await.unwrap();
    let handler = PrimaryReceiverHandler {
        tx_primary_messages,
        tx_remote_proposals,
        tx_cert_requests,
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
    p.batches.insert(digest, batch);
    p.header.id = p.header.digest();
    p.header.signature = Signature::new(&p.header.id, &keys().pop().unwrap().1);
    p.verify(&committee()).unwrap();
    let ack = bulk_network
        .send(
            address,
            Bytes::from(bincode::serialize(&PrimaryMessage::Proposal(p.clone())).unwrap()),
        )
        .await;
    // The bulk handler ACKs its frame, then blocks on the full queue.
    tokio::time::timeout(Duration::from_secs(5), ack)
        .await
        .unwrap()
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
                votes[0].verify(&committee()).unwrap();
            }
            _ => panic!("Expected vote over independent connection"),
        }
    }
    // Only now release the intentionally blocked proposal consumer.
    rx_bulk.recv().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), rx_bulk.recv())
        .await
        .unwrap()
        .unwrap();
    accept.abort();
}
