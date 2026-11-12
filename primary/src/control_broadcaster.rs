//! Send queued GRBC votes as bounded vectors across instances, rounds and phases.
//! No timer or protocol event is awaited to obtain a piggyback partner.
use crate::messages::{ConsensusNetworkMessage, ControlFrame, GrbcVoteBatch};
use crate::primary::{PrimaryMessage, Round};
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey, Signature};
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
const MAX_CONTROL_BYTES: u64 = 64 * 1024;
const MAX_DRAIN: usize = 32;

/// Local inputs to the shared control sender. Consensus sends ABA directly;
/// Core sends GRBC votes through the same queue for opportunistic packing.
pub enum ControlMessage {
    Votes(Vec<GrbcVoteBatch>),
    Aba(ConsensusNetworkMessage),
}

enum Packet {
    Control(ControlFrame),
    Aba(ConsensusNetworkMessage),
}

// Move entries out of a large batch once; splitting must not repeatedly copy
// its remaining suffix. All vote entries have the same encoded width (fixed
// digests/signatures and fixed-length base64 public keys).
struct QueuedVotes {
    header: GrbcVoteBatch,
    votes: std::vec::IntoIter<(Digest, PublicKey, Signature)>,
    header_bytes: u64,
    vote_bytes: u64,
}

impl QueuedVotes {
    fn new(mut batch: GrbcVoteBatch) -> Option<Self> {
        let vote_bytes = bincode::serialized_size(batch.votes.first()?).unwrap();
        let votes = std::mem::take(&mut batch.votes).into_iter();
        let header_bytes = bincode::serialized_size(&batch).unwrap();
        Some(Self {
            header: batch,
            votes,
            header_bytes,
            vote_bytes,
        })
    }

    fn take(&mut self, budget: u64) -> Option<(GrbcVoteBatch, u64)> {
        let count = ((budget.saturating_sub(self.header_bytes) / self.vote_bytes) as usize)
            .min(self.votes.len());
        if count == 0 {
            return None;
        }
        let batch = GrbcVoteBatch {
            round: self.header.round,
            author: self.header.author,
            phase: self.header.phase,
            votes: self.votes.by_ref().take(count).collect(),
        };
        Some((batch, self.header_bytes + count as u64 * self.vote_bytes))
    }
}

#[derive(Default)]
struct ReadyMessages {
    votes: VecDeque<QueuedVotes>,
    aba: VecDeque<ConsensusNetworkMessage>,
}

impl ReadyMessages {
    fn push(&mut self, message: ControlMessage) {
        match message {
            ControlMessage::Votes(votes) => self
                .votes
                .extend(votes.into_iter().filter_map(QueuedVotes::new)),
            ControlMessage::Aba(mut message) => {
                if let Some(last) = self.aba.back_mut() {
                    if last.author == message.author
                        && bincode::serialized_size(last).unwrap()
                            + bincode::serialized_size(&message).unwrap()
                            <= MAX_CONTROL_BYTES
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
        let first = match self.votes.front() {
            Some(first) => first,
            None => return self.aba.pop_front().map(Packet::Aba),
        };
        // Include the enum tag, Vec length and Option tag in the size budget.
        let mut size =
            bincode::serialized_size(&PrimaryMessage::Control(ControlFrame::default())).unwrap();
        let mut frame = ControlFrame::default();
        if let Some(aba) = self.aba.front() {
            let aba_bytes = bincode::serialized_size(aba).unwrap();
            if size + aba_bytes + first.header_bytes + first.vote_bytes > MAX_CONTROL_BYTES {
                // A large ABA is sent independently, never held behind votes.
                return self.aba.pop_front().map(Packet::Aba);
            }
            size += aba_bytes;
            frame.aba = self.aba.pop_front();
        }
        while let Some(queued) = self.votes.front_mut() {
            match queued.take(MAX_CONTROL_BYTES - size) {
                Some((batch, bytes)) => {
                    frame.grbc.push(batch);
                    size += bytes;
                }
                None => break,
            }
            if queued.votes.len() == 0 {
                self.votes.pop_front();
            }
        }
        assert!(
            !frame.grbc.is_empty(),
            "A control frame must fit at least one vote"
        );
        Some(Packet::Control(frame))
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
                    // Only inspect already-enqueued messages. A single late vote
                    // suffices; never wait for READY, a new proposal or a full vector.
                    for _ in 1..MAX_DRAIN {
                        match rx_messages.try_recv() {
                            Ok(message) => ready.push(message),
                            Err(_) => break,
                        }
                    }
                    while let Some(packet) = ready.next_packet() {
                        let (bytes, addresses, vote_round) = match packet {
                            Packet::Control(frame) => {
                                let round = if frame.aba.is_some() {
                                    None
                                } else {
                                    frame.grbc.iter().map(|batch| batch.round).max()
                                };
                                debug!("Sending GRBC vector: batches {} votes {} aba {}",
                                    frame.grbc.len(), frame.grbc.iter().map(|batch| batch.votes.len()).sum::<usize>(), frame.aba.is_some());
                                (bincode::serialize(&PrimaryMessage::Control(frame)),
                                 &vote_addresses, round)
                            }
                            Packet::Aba(aba) => {
                                debug!("Sending standalone ABA control frame");
                                (bincode::serialize(&aba), &aba_addresses, None)
                            }
                        };
                        let handlers = network.broadcast(
                            addresses.clone(),
                            Bytes::from(bytes.expect("Failed to serialize control frame")),
                        ).await;
                        if let Some(round) = vote_round {
                            // Retain a cross-round vector while ANY vote is live.
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
