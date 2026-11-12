//! Opportunistically pack already-ready ABA messages with whole GRBC vote batches.
//! No timer or protocol event is awaited to obtain a piggyback partner.
use crate::messages::{ConsensusNetworkMessage, GrbcVoteBatch};
use crate::primary::{PrimaryMessage, Round};
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use futures::stream::{FuturesUnordered, StreamExt};
use log::debug;
use network::{CancelHandler, ReliableSender};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};

// Avoid turning small control traffic into a large frame. Larger ABA batches
// retain their standalone path; this limit adds no new restriction on them.
const MAX_PIGGYBACK_BYTES: u64 = 64 * 1024;
const MAX_DRAIN: usize = 32;

pub(crate) enum ControlMessage {
    Votes(Vec<GrbcVoteBatch>),
    Aba(ConsensusNetworkMessage),
}

enum Packet {
    Votes(GrbcVoteBatch),
    Aba(ConsensusNetworkMessage),
    Combined(GrbcVoteBatch, ConsensusNetworkMessage),
}

#[derive(Default)]
struct ReadyMessages {
    votes: VecDeque<GrbcVoteBatch>,
    aba: VecDeque<ConsensusNetworkMessage>,
}

impl ReadyMessages {
    fn push(&mut self, message: ControlMessage) {
        match message {
            ControlMessage::Votes(votes) => self.votes.extend(votes),
            ControlMessage::Aba(mut message) => {
                if let Some(last) = self.aba.back_mut() {
                    if last.author == message.author
                        && bincode::serialized_size(last).unwrap()
                            + bincode::serialized_size(&message).unwrap()
                            <= MAX_PIGGYBACK_BYTES
                    {
                        last.messages.append(&mut message.messages);
                        return;
                    }
                }
                self.aba.push_back(message);
            }
        }
    }

    fn next_packet(&mut self) -> Option<Packet> {
        if let Some(votes) = self.votes.pop_front() {
            if self.aba.front().map_or(false, |aba| {
                // A bincode enum tag followed by the two unchanged protocol parts.
                4 + bincode::serialized_size(&votes).expect("Invalid vote batch")
                    + bincode::serialized_size(aba).expect("Invalid ABA batch")
                    <= MAX_PIGGYBACK_BYTES
            }) {
                return Some(Packet::Combined(votes, self.aba.pop_front().unwrap()));
            }
            return Some(Packet::Votes(votes));
        }
        self.aba.pop_front().map(Packet::Aba)
    }
}

pub(crate) fn spawn_control_broadcaster(
    name: PublicKey,
    committee: Committee,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    mut rx_messages: Receiver<ControlMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let peers = committee.others_primaries(&name);
        let vote_addresses: Vec<_> = peers.iter().map(|(_, a)| a.primary_to_primary).collect();
        let aba_addresses: Vec<_> = peers.iter().map(|(_, a)| a.aba_to_aba).collect();
        // This sender never carries complete proposals.
        let mut network = ReliableSender::new();
        let mut pending_aba = FuturesUnordered::new();
        let mut pending_votes = BTreeMap::<Round, Vec<CancelHandler>>::new();
        let mut cleanup = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                message = rx_messages.recv() => {
                    let mut ready = ReadyMessages::default();
                    match message {
                        Some(message) => ready.push(message),
                        None => break,
                    }
                    // Only inspect messages already enqueued. Keep Core's
                    // per-round/per-phase batches intact, even on a quiet network.
                    for _ in 1..MAX_DRAIN {
                        match rx_messages.try_recv() {
                            Ok(message) => ready.push(message),
                            Err(_) => break,
                        }
                    }
                    while let Some(packet) = ready.next_packet() {
                        let (bytes, addresses, vote_round) = match packet {
                            Packet::Votes(votes) => {
                                let round = votes.round;
                                (bincode::serialize(&PrimaryMessage::GrbcVoteBatch(votes)),
                                 &vote_addresses, Some(round))
                            }
                            Packet::Aba(aba) => {
                                debug!("Sending standalone ABA control frame");
                                (bincode::serialize(&aba), &aba_addresses, None)
                            }
                            Packet::Combined(votes, aba) => {
                                debug!("Sending combined GRBC/ABA control frame");
                                (bincode::serialize(&PrimaryMessage::GrbcWithAba(votes, aba)),
                                 &vote_addresses, None)
                            }
                        };
                        let handlers = network.broadcast(
                            addresses.clone(),
                            Bytes::from(bytes.expect("Failed to serialize control frame")),
                        ).await;
                        if let Some(round) = vote_round {
                            pending_votes.entry(round).or_default().extend(handlers);
                        } else {
                            // A mixed frame's ABA may refer to a different/older
                            // leader. Never cancel it using the GRBC vote's round.
                            pending_aba.extend(handlers);
                        }
                    }
                }
                Some(_) = pending_aba.next(), if !pending_aba.is_empty() => {}
                _ = cleanup.tick() => {
                    let oldest = consensus_round.load(Ordering::Relaxed).saturating_sub(gc_depth);
                    pending_votes.retain(|round, handlers| {
                        if *round < oldest {
                            return false;
                        }
                        handlers.retain_mut(|handler| matches!(handler.try_recv(), Err(TryRecvError::Empty)));
                        !handlers.is_empty()
                    });
                }
            }
        }
    })
}

#[cfg(test)]
#[path = "tests/control_broadcaster_tests.rs"]
mod tests;
