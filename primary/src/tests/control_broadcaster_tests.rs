use super::*;
use crate::common::{committee, headers, keys, votes};
use crate::messages::GrbcPhase;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::channel;
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

fn vote_batch(phase: GrbcPhase) -> GrbcVoteBatch {
    GrbcVoteBatch::from_votes(
        headers()
            .iter()
            .map(|h| votes(h, phase).remove(0))
            .collect(),
    )
    .unwrap()
}

fn aba(value: u8) -> ConsensusNetworkMessage {
    ConsensusNetworkMessage {
        author: keys()[0].0,
        messages: vec![vec![value]],
    }
}

async fn setup() -> (Committee, TcpListener, TcpListener) {
    let mut committee = committee();
    committee
        .authorities
        .retain(|name, _| *name == keys()[0].0 || *name == keys()[1].0);
    let votes = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let aba = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = &mut committee.authorities.get_mut(&keys()[1].0).unwrap().primary;
    peer.primary_to_primary = votes.local_addr().unwrap();
    peer.aba_to_aba = aba.local_addr().unwrap();
    (committee, votes, aba)
}

async fn accept(listener: &TcpListener) -> Framed<TcpStream, LengthDelimitedCodec> {
    let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    Framed::new(socket, LengthDelimitedCodec::new())
}

async fn receive(stream: &mut Framed<TcpStream, LengthDelimitedCodec>) -> Bytes {
    timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .freeze()
}

#[tokio::test]
async fn queued_aba_piggybacks_without_fragmenting_phase_batches() {
    for aba_first in [true, false] {
        let (committee, listener, _aba_listener) = setup().await;
        let (tx, rx) = channel(10);
        if aba_first {
            tx.send(ControlMessage::Aba(aba(1))).await.unwrap();
        }
        tx.send(ControlMessage::Votes(vec![
            vote_batch(GrbcPhase::Echo),
            vote_batch(GrbcPhase::Ready),
        ]))
        .await
        .unwrap();
        if !aba_first {
            tx.send(ControlMessage::Aba(aba(1))).await.unwrap();
        }
        tx.send(ControlMessage::Aba(aba(2))).await.unwrap();
        let task =
            spawn_control_broadcaster(keys()[0].0, committee, Arc::new(AtomicU64::new(0)), 50, rx);
        let mut stream = accept(&listener).await;
        match bincode::deserialize::<PrimaryMessage>(&receive(&mut stream).await).unwrap() {
            PrimaryMessage::GrbcWithAba(batch, aba) => {
                assert_eq!(batch.phase, GrbcPhase::Echo);
                assert_eq!(batch.votes.len(), 4);
                for vote in batch.into_votes() {
                    vote.verify(&crate::common::committee()).unwrap();
                }
                assert_eq!(aba.messages, vec![vec![1], vec![2]]);
            }
            _ => panic!("Expected a mixed frame"),
        }
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
        match bincode::deserialize::<PrimaryMessage>(&receive(&mut stream).await).unwrap() {
            PrimaryMessage::GrbcVoteBatch(batch) => {
                assert_eq!(batch.phase, GrbcPhase::Ready);
                assert_eq!(batch.votes.len(), 4);
            }
            _ => panic!("Expected the remaining READY batch"),
        }
        task.abort();
    }
}

#[tokio::test]
async fn either_protocol_sends_without_a_piggyback_partner() {
    let (committee, vote_listener, aba_listener) = setup().await;
    let (tx, rx) = channel(10);
    let task =
        spawn_control_broadcaster(keys()[0].0, committee, Arc::new(AtomicU64::new(0)), 50, rx);
    tx.send(ControlMessage::Aba(aba(7))).await.unwrap();
    let mut stream = accept(&aba_listener).await;
    let received: ConsensusNetworkMessage =
        bincode::deserialize(&receive(&mut stream).await).unwrap();
    assert_eq!(received.messages, vec![vec![7]]);
    stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    tx.send(ControlMessage::Votes(vec![vote_batch(GrbcPhase::Ready)]))
        .await
        .unwrap();
    let mut stream = accept(&vote_listener).await;
    assert!(matches!(
        bincode::deserialize::<PrimaryMessage>(&receive(&mut stream).await).unwrap(),
        PrimaryMessage::GrbcVoteBatch(_)
    ));
    task.abort();
}

#[test]
fn large_aba_stays_separate_without_losing_the_vote_batch() {
    let mut ready = ReadyMessages::default();
    ready.push(ControlMessage::Votes(vec![vote_batch(GrbcPhase::Echo)]));
    let mut message = aba(3);
    message.messages = vec![vec![3; MAX_PIGGYBACK_BYTES as usize]];
    ready.push(ControlMessage::Aba(message));
    assert!(matches!(ready.next_packet(), Some(Packet::Votes(_))));
    match ready.next_packet().unwrap() {
        Packet::Aba(message) => assert_eq!(message.messages[0].len(), MAX_PIGGYBACK_BYTES as usize),
        _ => panic!("Expected standalone large ABA"),
    }
    assert!(ready.next_packet().is_none());
}

#[tokio::test]
async fn mixed_frame_retries_even_after_the_vote_round_is_garbage_collected() {
    let (committee, listener, _aba_listener) = setup().await;
    let (tx, rx) = channel(10);
    tx.send(ControlMessage::Votes(vec![vote_batch(GrbcPhase::Echo)]))
        .await
        .unwrap();
    tx.send(ControlMessage::Aba(aba(9))).await.unwrap();
    let round = Arc::new(AtomicU64::new(0));
    let task = spawn_control_broadcaster(keys()[0].0, committee, round.clone(), 50, rx);
    let mut stream = accept(&listener).await;
    let original = receive(&mut stream).await;
    round.store(100, Ordering::Relaxed);
    sleep(Duration::from_millis(1100)).await;
    // Lose the first connection before ACK, after vote-only GC would cancel it.
    drop(stream);
    let mut stream = accept(&listener).await;
    assert_eq!(receive(&mut stream).await, original);
    stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    task.abort();
}
