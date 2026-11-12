//! Small GRBC messages have their own task and TCP connections, including repair.
use crate::messages::{GrbcPhase, GrbcVote, GrbcVoteBatch};
use crate::primary::{PrimaryMessage, Round};
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use network::{CancelHandler, ReliableSender};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub(crate) struct VoteDispatch {
    /// None broadcasts; Some sends this node's saved votes to a repair requestor.
    pub requestor: Option<PublicKey>,
    pub votes: Vec<GrbcVote>,
}

pub(crate) fn spawn_vote_sender(
    name: PublicKey,
    committee: Committee,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    mut rx: Receiver<VoteDispatch>,
) {
    tokio::spawn(async move {
        let mut network = ReliableSender::new();
        let addresses: Vec<_> = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, a)| a.primary_to_primary)
            .collect();
        let mut pending = BTreeMap::<Round, Vec<CancelHandler>>::new();
        let mut cleanup = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                message = rx.recv() => {
                    let first = match message { Some(message) => message, None => break };
                    let mut grouped = BTreeMap::<(Option<PublicKey>, Round, GrbcPhase, PublicKey), Vec<GrbcVote>>::new();
                    let mut add = |dispatch: VoteDispatch| {
                        for vote in dispatch.votes {
                            grouped.entry((dispatch.requestor, vote.round, vote.phase, vote.author))
                                .or_default().push(vote);
                        }
                    };
                    add(first);
                    // Coalesce only work already queued; never delay a vote to fill a batch.
                    for _ in 1..32 {
                        match rx.try_recv() { Ok(message) => add(message), Err(_) => break }
                    }
                    let oldest = consensus_round.load(Ordering::Relaxed).saturating_sub(gc_depth);
                    for ((requestor, round, _, _), votes) in grouped {
                        if round < oldest { continue; }
                        let destinations = match requestor {
                            Some(peer) => match committee.primary(&peer) {
                                Ok(a) => vec![a.primary_to_primary],
                                Err(_) => continue,
                            },
                            None => addresses.clone(),
                        };
                        let batch = GrbcVoteBatch::from_votes(votes).expect("Mixed GRBC vote batch");
                        let bytes = bincode::serialize(&PrimaryMessage::GrbcVoteBatch(batch))
                            .expect("Failed to serialize GRBC votes");
                        pending.entry(round).or_default().extend(
                            network.broadcast(destinations, Bytes::from(bytes)).await
                        );
                    }
                }
                _ = cleanup.tick() => {}
            }
            let oldest = consensus_round
                .load(Ordering::Relaxed)
                .saturating_sub(gc_depth);
            pending.retain(|round, handlers| {
                if *round < oldest {
                    return false;
                }
                handlers.retain_mut(|handler| {
                    matches!(
                        handler.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                    )
                });
                !handlers.is_empty()
            });
        }
    });
}
