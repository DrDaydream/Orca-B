//! Complete payload work stays outside the GRBC vote processing loop.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Proposal};
use crate::primary::{PrimaryMessage, Round};
use crate::proposal_store::{assemble_proposal, store_verified_proposal};
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, Hash as _, PublicKey};
use log::{debug, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Presence means work is queued/in flight, never that the proposal is valid.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProposalTracker(Arc<Mutex<HashMap<Digest, usize>>>);

impl ProposalTracker {
    pub(crate) fn track(&self, header: &Header) -> ProposalGuard {
        let digest = Certificate {
            header: Header {
                id: header.id.clone(),
                round: header.round,
                author: header.author,
                ..Header::default()
            },
            votes: Vec::new(),
        }
        .digest();
        *self.0.lock().unwrap().entry(digest.clone()).or_default() += 1;
        ProposalGuard {
            tracker: self.clone(),
            digest,
        }
    }

    pub(crate) fn contains(&self, digest: &Digest) -> bool {
        self.0.lock().unwrap().contains_key(digest)
    }
}

/// Travels with the proposal through queueing, validation, storage and Core
/// registration. Failure, cancellation and closed queues release it too.
#[derive(Debug)]
pub(crate) struct ProposalGuard {
    tracker: ProposalTracker,
    digest: Digest,
}

impl Drop for ProposalGuard {
    fn drop(&mut self) {
        let mut pending = self.tracker.0.lock().unwrap();
        let count = pending.get_mut(&self.digest).expect("Untracked proposal");
        *count -= 1;
        if *count == 0 {
            pending.remove(&self.digest);
        }
    }
}

pub(crate) type QueuedProposal = (Proposal, ProposalGuard);

/// Created only after complete authentication, payload checks and atomic storage.
pub(crate) struct StoredProposalHeader(Header, Option<ProposalGuard>);

impl StoredProposalHeader {
    pub(crate) fn into_header(self) -> Header {
        let Self(header, _guard) = self;
        header
    }
}

pub(crate) async fn prepare_proposal(
    proposal: Proposal,
    committee: Committee,
    store: &mut Store,
    outbound: bool,
    in_flight: Option<ProposalGuard>,
) -> DagResult<(StoredProposalHeader, Option<Bytes>)> {
    let (proposal, bytes) = tokio::task::spawn_blocking(move || {
        proposal.verify(&committee)?;
        let message = PrimaryMessage::Proposal(proposal);
        let bytes = if outbound {
            Some(Bytes::from(bincode::serialize(&message)?))
        } else {
            None
        };
        match message {
            PrimaryMessage::Proposal(proposal) => Ok::<_, DagError>((proposal, bytes)),
            _ => unreachable!(),
        }
    })
    .await
    .expect("Proposal validation task failed")?;
    let header = store_verified_proposal(store, proposal).await?;
    Ok((StoredProposalHeader(header, in_flight), bytes))
}

fn report(result: DagResult<()>) {
    match result {
        Ok(()) => (),
        Err(DagError::StoreError(e)) => panic!("Storage failure: {}", e),
        Err(e @ DagError::TooOld(..)) => debug!("{}", e),
        Err(e) => warn!("Ignoring invalid proposal: {}", e),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_proposal_processors(
    name: PublicKey,
    committee: Committee,
    store: Store,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    mut rx_remote: Receiver<QueuedProposal>,
    mut rx_local: Receiver<Header>,
    tx: Sender<StoredProposalHeader>,
    in_flight: ProposalTracker,
) {
    // Separate bounded consumers prevent a remote payload backlog from blocking
    // local proposals. At most one blocking validation job per consumer is active.
    let remote_committee = committee.clone();
    let mut remote_store = store.clone();
    let remote_round = consensus_round.clone();
    let remote_tx = tx.clone();
    tokio::spawn(async move {
        let capacity = remote_committee
            .size()
            .saturating_mul(gc_depth as usize)
            .max(1);
        let mut delivered = HashSet::new();
        let mut delivery_order = VecDeque::new();
        while let Some((proposal, guard)) = rx_remote.recv().await {
            if proposal.header.round
                < remote_round
                    .load(Ordering::Relaxed)
                    .saturating_sub(gc_depth)
                || delivered.contains(&proposal.header.id)
            {
                continue;
            }
            report(
                async {
                    let id = proposal.header.id.clone();
                    let (header, _) = prepare_proposal(
                        proposal,
                        remote_committee.clone(),
                        &mut remote_store,
                        false,
                        Some(guard),
                    )
                    .await?;
                    remote_tx
                        .send(header)
                        .await
                        .expect("Failed to deliver stored proposal");
                    // Remember only fully verified, stored and enqueued values.
                    // Cache hits discard the incoming copy without applying it.
                    delivered.insert(id.clone());
                    delivery_order.push_back(id);
                    if delivery_order.len() > capacity {
                        delivered.remove(&delivery_order.pop_front().unwrap());
                    }
                    Ok(())
                }
                .await,
            );
        }
    });
    tokio::spawn(async move {
        let mut store = store;
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
                header = rx_local.recv() => {
                    let header = match header { Some(header) => header, None => break };
                    if header.round < consensus_round.load(Ordering::Relaxed).saturating_sub(gc_depth) { continue; }
                    report(async {
                        let round = header.round;
                        let guard = in_flight.track(&header);
                        let proposal = assemble_proposal(&mut store, header).await?;
                        let (header, bytes) = prepare_proposal(proposal, committee.clone(), &mut store, true, Some(guard)).await?;
                        pending.entry(round).or_default().extend(
                            network.broadcast(addresses.clone(), bytes.expect("Missing outbound proposal")).await
                        );
                        tx.send(header).await.expect("Failed to deliver local proposal");
                        Ok(())
                    }.await);
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
