use super::*;
use crate::common::{committee, headers, keys, votes};
use crate::messages::GrbcPhase;
use crypto::Hash;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::channel;
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

fn vote_batch(phase: GrbcPhase) -> GrbcVoteBatch {
    vote_batch_at(1, phase)
}

fn vote_batch_at(round: Round, phase: GrbcPhase) -> GrbcVoteBatch {
    GrbcVoteBatch::from_votes(
        headers()
            .into_iter()
            .map(|mut h| {
                h.round = round;
                h.id = h.digest();
                votes(&h, phase).remove(0)
            })
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
async fn vector_combines_rounds_phases_and_aba_in_either_arrival_order() {
    for aba_first in [true, false] {
        let (committee, listener, _aba_listener) = setup().await;
        let (tx, rx) = channel(10);
        if aba_first {
            tx.send(ControlMessage::Aba(aba(1))).await.unwrap();
        }
        tx.send(ControlMessage::Votes(vec![
            vote_batch(GrbcPhase::Echo),
            vote_batch(GrbcPhase::Ready),
            vote_batch_at(2, GrbcPhase::Echo),
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
            PrimaryMessage::Control(frame) => {
                assert_eq!(
                    frame
                        .grbc
                        .iter()
                        .map(|b| (b.round, b.phase))
                        .collect::<Vec<_>>(),
                    vec![
                        (1, GrbcPhase::Echo),
                        (1, GrbcPhase::Ready),
                        (2, GrbcPhase::Echo)
                    ]
                );
                for batch in frame.grbc {
                    assert_eq!(batch.votes.len(), 4);
                    for vote in batch.into_votes() {
                        vote.verify(&crate::common::committee()).unwrap();
                    }
                }
                assert_eq!(frame.aba.unwrap().messages, vec![vec![1], vec![2]]);
            }
            _ => panic!("Expected a mixed vector"),
        }
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
        task.abort();
    }
}

#[tokio::test]
async fn either_protocol_and_single_late_votes_send_without_more_input() {
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
    tx.send(ControlMessage::Votes(vec![
        vote_batch(GrbcPhase::Echo),
        vote_batch(GrbcPhase::Ready),
    ]))
    .await
    .unwrap();
    let mut stream = accept(&vote_listener).await;
    match bincode::deserialize::<PrimaryMessage>(&receive(&mut stream).await).unwrap() {
        PrimaryMessage::Control(frame) => assert_eq!(frame.grbc.len(), 2),
        _ => panic!("Expected standalone vote vector"),
    }
    stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    // Both phases already sent once. A single late vote must send by itself,
    // with the channel still open and no future phase/round traffic.
    for phase in [GrbcPhase::Echo, GrbcPhase::Ready] {
        let mut late = vote_batch(phase);
        late.votes.truncate(1);
        tx.send(ControlMessage::Votes(vec![late])).await.unwrap();
        match bincode::deserialize::<PrimaryMessage>(&receive(&mut stream).await).unwrap() {
            PrimaryMessage::Control(frame) => {
                assert!(frame.aba.is_none());
                assert_eq!(frame.grbc.len(), 1);
                assert_eq!(frame.grbc[0].phase, phase);
                assert_eq!(frame.grbc[0].votes.len(), 1);
            }
            _ => panic!("Expected independent late vote"),
        }
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    }
    task.abort();
}

#[test]
fn large_aba_stays_separate_without_losing_the_vote_batch() {
    let mut ready = ReadyMessages::default();
    ready.push(ControlMessage::Votes(vec![vote_batch(GrbcPhase::Echo)]));
    let mut message = aba(3);
    message.messages = vec![vec![3; MAX_CONTROL_BYTES as usize]];
    ready.push(ControlMessage::Aba(message));
    match ready.next_packet().unwrap() {
        Packet::Aba(message) => assert_eq!(message.messages[0].len(), MAX_CONTROL_BYTES as usize),
        _ => panic!("Expected standalone large ABA"),
    }
    assert!(matches!(ready.next_packet(), Some(Packet::Control(_))));
    assert!(ready.next_packet().is_none());
}

#[tokio::test]
async fn vectors_retain_aba_or_newer_votes_after_old_round_gc() {
    for with_aba in [true, false] {
        let (committee, listener, _aba_listener) = setup().await;
        let (tx, rx) = channel(10);
        let mut batches = vec![vote_batch(GrbcPhase::Echo)];
        if with_aba {
            tx.send(ControlMessage::Aba(aba(9))).await.unwrap();
        } else {
            batches.push(vote_batch_at(60, GrbcPhase::Ready));
        }
        tx.send(ControlMessage::Votes(batches)).await.unwrap();
        let round = Arc::new(AtomicU64::new(0));
        let task = spawn_control_broadcaster(keys()[0].0, committee, round.clone(), 50, rx);
        let mut stream = accept(&listener).await;
        let original = receive(&mut stream).await;
        round.store(100, Ordering::Relaxed);
        sleep(Duration::from_millis(1100)).await;
        // Lose the connection before ACK, after the oldest vote has expired.
        drop(stream);
        let mut stream = accept(&listener).await;
        assert_eq!(receive(&mut stream).await, original);
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
        task.abort();
    }
}

#[tokio::test]
async fn oversized_batches_split_and_send_all_tails_without_new_input() {
    let (committee, listener, _aba_listener) = setup().await;
    let (tx, rx) = channel(10);
    let mut batch = vote_batch_at(7, GrbcPhase::Ready);
    // Distinct transport entries detect loss, duplication and reordering;
    // signature preservation is checked byte-for-byte across the split.
    let template = batch.votes[0].clone();
    batch.votes = (0..1500u64)
        .map(|i| {
            let mut entry = template.clone();
            entry.0 .0[..8].copy_from_slice(&i.to_le_bytes());
            entry
        })
        .collect();
    let expected = bincode::serialize(&batch.votes).unwrap();
    tx.send(ControlMessage::Votes(vec![batch])).await.unwrap();
    let task =
        spawn_control_broadcaster(keys()[0].0, committee, Arc::new(AtomicU64::new(0)), 50, rx);
    let mut stream = accept(&listener).await;
    let mut entries = Vec::new();
    let mut frames = 0;
    while entries.len() < 1500 {
        let wire = receive(&mut stream).await;
        assert!(wire.len() as u64 <= MAX_CONTROL_BYTES);
        match bincode::deserialize::<PrimaryMessage>(&wire).unwrap() {
            PrimaryMessage::Control(frame) => {
                assert!(frame.aba.is_none());
                assert!(!frame.grbc.is_empty());
                for batch in frame.grbc {
                    assert_eq!((batch.round, batch.phase), (7, GrbcPhase::Ready));
                    assert!(!batch.votes.is_empty());
                    entries.extend(batch.votes);
                }
            }
            _ => panic!("Expected bounded vector"),
        }
        frames += 1;
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    }
    assert!(frames > 1);
    assert_eq!(bincode::serialize(&entries).unwrap(), expected);
    task.abort();
}

#[test]
fn packing_respects_exact_size_boundaries_and_empty_inputs() {
    let mut one = vote_batch(GrbcPhase::Ready);
    one.votes.truncate(1);
    let mut empty_aba = aba(4);
    empty_aba.messages[0].clear();
    let base = bincode::serialized_size(&PrimaryMessage::Control(ControlFrame {
        grbc: vec![one],
        aba: Some(empty_aba),
    }))
    .unwrap();
    let exact_padding = (MAX_CONTROL_BYTES - base) as usize;
    for padding in [
        0,
        1,
        exact_padding - 1,
        exact_padding,
        exact_padding + 1,
        65_536,
    ] {
        let mut ready = ReadyMessages::default();
        let mut batch = vote_batch(GrbcPhase::Echo);
        batch.votes.clear();
        ready.push(ControlMessage::Votes(vec![batch]));
        assert!(ready.next_packet().is_none());
        let mut batch = vote_batch(GrbcPhase::Ready);
        batch.votes = vec![batch.votes[0].clone(); 600];
        ready.push(ControlMessage::Votes(vec![
            batch,
            vote_batch_at(2, GrbcPhase::Echo),
        ]));
        let mut message = aba(4);
        message.messages = vec![vec![4; padding]];
        ready.push(ControlMessage::Aba(message));
        let (mut received_votes, mut received_aba) = (0, 0);
        while let Some(packet) = ready.next_packet() {
            match packet {
                Packet::Control(frame) => {
                    let wire = bincode::serialize(&PrimaryMessage::Control(frame.clone())).unwrap();
                    assert!(wire.len() as u64 <= MAX_CONTROL_BYTES);
                    if padding == exact_padding && frame.aba.is_some() {
                        assert_eq!(wire.len() as u64, MAX_CONTROL_BYTES);
                    }
                    received_votes += frame
                        .grbc
                        .iter()
                        .map(|batch| batch.votes.len())
                        .sum::<usize>();
                    received_aba += usize::from(frame.aba.is_some());
                }
                Packet::Aba(_) => received_aba += 1,
            }
        }
        assert_eq!((received_votes, received_aba), (604, 1));
    }
}
