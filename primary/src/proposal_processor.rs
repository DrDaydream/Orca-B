//! Authenticate and relay digest proposals outside the GRBC vote processing loop.
use crate::error::{DagError, DagResult};
use crate::messages::Header;
#[cfg(test)]
use crate::messages::Proposal;
use crate::primary::{PrimaryMessage, Round};
use crate::proposal_store::{load_proposal, store_verified_proposal};
use crate::synchronizer::Synchronizer;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use futures::FutureExt;
use log::{debug, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Presence means work is queued/in flight, never that the proposal is valid.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProposalTracker(Arc<Mutex<HashMap<Digest, usize>>>);

impl ProposalTracker {
    pub(crate) fn track(&self, header: &Header) -> ProposalGuard {
        let digest = header.block_digest();
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

pub(crate) type QueuedProposal = (Header, ProposalGuard);

/// Authenticated header whose payload checks passed and whose Store write is queued.
pub(crate) struct StoredProposalHeader(Header, Option<ProposalGuard>);

impl StoredProposalHeader {
    pub(super) async fn store(
        store: &mut Store,
        header: Header,
        guard: Option<ProposalGuard>,
    ) -> DagResult<Self> {
        Ok(Self(store_verified_proposal(store, header).await?, guard))
    }

    pub(crate) fn into_header(self) -> Header {
        let Self(header, _guard) = self;
        header
    }
}

async fn verify_and_serialize_proposal(
    header: Header,
    committee: Committee,
) -> DagResult<(Header, Bytes)> {
    tokio::task::spawn_blocking(move || {
        header.verify(&committee)?;
        let message = PrimaryMessage::Header(header);
        let bytes = Bytes::from(bincode::serialize(&message)?);
        match message {
            PrimaryMessage::Header(header) => Ok::<_, DagError>((header, bytes)),
            _ => unreachable!(),
        }
    })
    .await
    .expect("Proposal validation task failed")
}

#[cfg(test)]
pub(crate) async fn prepare_proposal(
    proposal: Proposal,
    committee: Committee,
    store: &mut Store,
    in_flight: Option<ProposalGuard>,
) -> DagResult<StoredProposalHeader> {
    let header = proposal.header;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut sync = Synchronizer::new(PublicKey::default(), store.clone(), tx);
    match prepare_remote_header(header, &committee, &mut sync, store, in_flight).await? {
        Some(header) => Ok(header),
        None => match rx.try_recv().unwrap() {
            crate::header_waiter::WaiterMessage::SyncBatches(missing, _) => {
                Err(DagError::MissingBatch(missing.into_keys().next().unwrap()))
            }
            _ => unreachable!(),
        },
    }
}

/// Authenticate a network header before scheduling recovery of any missing batches.
pub(crate) async fn prepare_remote_header(
    header: Header,
    committee: &Committee,
    synchronizer: &mut Synchronizer,
    store: &mut Store,
    guard: Option<ProposalGuard>,
) -> DagResult<Option<StoredProposalHeader>> {
    header.verify(committee)?;
    if synchronizer.missing_payload(&header).await? {
        return Ok(None);
    }
    Ok(Some(
        StoredProposalHeader::store(store, header, guard).await?,
    ))
}

/// The reliable sender takes ownership on its first successful enqueue. Poll
/// each peer independently so a full connection cannot stall the remaining peers.
pub(crate) fn try_broadcast(
    network: &mut ReliableSender,
    addresses: Vec<SocketAddr>,
    bytes: Bytes,
) -> (Vec<CancelHandler>, Vec<SocketAddr>) {
    let mut handlers = Vec::new();
    let mut deferred = Vec::new();
    for address in addresses {
        // Only poll the bounded enqueue. Existing connection tasks own the
        // socket writes and retries. Bypass the task's cooperative budget so
        // only actual enqueue backpressure causes a deferred send.
        match tokio::task::unconstrained(network.send(address, bytes.clone())).now_or_never() {
            Some(handler) => handlers.push(handler),
            None => deferred.push(address),
        }
    }
    (handlers, deferred)
}

pub(crate) async fn prepare_local_proposal(
    header: Header,
    committee: Committee,
    store: &mut Store,
    outbound: (&mut ReliableSender, Vec<SocketAddr>),
    in_flight: ProposalGuard,
    tx: &Sender<StoredProposalHeader>,
) -> DagResult<(Vec<CancelHandler>, Vec<SocketAddr>)> {
    let (header, bytes) = verify_and_serialize_proposal(header, committee).await?;
    let (network, addresses) = outbound;
    let dispatch = try_broadcast(network, addresses, bytes);
    let header = StoredProposalHeader::store(store, header, Some(in_flight)).await?;
    tx.send(header)
        .await
        .expect("Failed to deliver local proposal");
    Ok(dispatch)
}

/// One recovery worker per peer. Its queue holds only on-disk digests and its
/// single active value is cancelled when the round expires or the primary stops.
pub(crate) fn spawn_proposal_retry(
    mut network: ReliableSender,
    address: SocketAddr,
    mut store: Store,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
) -> Sender<(Round, Digest)> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Round, Digest)>(gc_depth.max(1) as usize);
    tokio::spawn(async move {
        let mut cleanup = tokio::time::interval(std::time::Duration::from_secs(1));
        while let Some((round, digest)) = rx.recv().await {
            let is_old = || {
                round
                    < consensus_round
                        .load(Ordering::Relaxed)
                        .saturating_sub(gc_depth)
            };
            if is_old() {
                continue;
            }
            let send = async {
                if let Some(proposal) = load_proposal(&mut store, &digest).await? {
                    let bytes = tokio::task::spawn_blocking(move || {
                        bincode::serialize(&PrimaryMessage::Header(proposal))
                    })
                    .await
                    .expect("Proposal serialization task failed")?;
                    // Only an already-released frame can be deferred by try_broadcast.
                    let _ = network.send_released(address, Bytes::from(bytes)).await;
                }
                Ok(())
            };
            tokio::pin!(send);
            loop {
                tokio::select! {
                    result = &mut send => { report(result); break; }
                    _ = cleanup.tick() => {
                        if rx.is_closed() { return; }
                        if is_old() { break; }
                    }
                }
            }
        }
    });
    tx
}

pub(super) fn report(result: DagResult<()>) {
    match result {
        Ok(()) => (),
        Err(DagError::StoreError(e)) => panic!("Storage failure: {}", e),
        Err(e @ DagError::TooOld(..)) => debug!("{}", e),
        Err(e) => warn!("Ignoring invalid proposal: {}", e),
    }
}

/// HeaderWaiter returns authenticated headers once every missing worker receipt is in the store.
pub(crate) fn spawn_repaired_headers(
    mut store: Store,
    mut repaired: Receiver<Header>,
    tx: Sender<StoredProposalHeader>,
) {
    tokio::spawn(async move {
        while let Some(header) = repaired.recv().await {
            let stored = StoredProposalHeader::store(&mut store, header, None)
                .await
                .expect("Failed to store repaired header");
            tx.send(stored).await.expect("Core stopped");
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_proposal_processors(
    name: PublicKey,
    committee: Committee,
    store: Store,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    mut rx_remote: Receiver<QueuedProposal>,
    mut synchronizer: Synchronizer,
    mut rx_local: Receiver<Header>,
    tx: Sender<StoredProposalHeader>,
    in_flight: ProposalTracker,
) {
    // A remote payload check cannot block local proposals or signed GRBC votes.
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
        while let Some((header, guard)) = rx_remote.recv().await {
            if header.round
                < remote_round
                    .load(Ordering::Relaxed)
                    .saturating_sub(gc_depth)
                || delivered.contains(&header.id)
            {
                continue;
            }
            report(
                async {
                    let id = header.id.clone();
                    let stored = match prepare_remote_header(
                        header,
                        &remote_committee,
                        &mut synchronizer,
                        &mut remote_store,
                        Some(guard),
                    )
                    .await?
                    {
                        Some(stored) => stored,
                        None => return Ok(()),
                    };
                    remote_tx
                        .send(stored)
                        .await
                        .expect("Failed to deliver stored proposal");
                    // Both the header write and delivery to Core are now queued.
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
        let mut network = crate::adversary::reliable_sender();
        let addresses: Vec<_> = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, a)| a.primary_to_primary)
            .collect();
        let retry: HashMap<_, _> = addresses
            .iter()
            .copied()
            .map(|address| {
                (
                    address,
                    spawn_proposal_retry(
                        ReliableSender::new(),
                        address,
                        store.clone(),
                        consensus_round.clone(),
                        gc_depth,
                    ),
                )
            })
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
                        let digest = guard.digest.clone();
                        let (handlers, deferred) = prepare_local_proposal(
                            header, committee.clone(), &mut store,
                            (&mut network, addresses.clone()), guard, &tx,
                        ).await?;
                        pending.entry(round).or_default().extend(handlers);
                        for address in deferred {
                            // A saturated replay queue leaves recovery to the
                            // normal signed-vote/missing-proposal repair path.
                            match retry[&address].try_send((round, digest.clone())) {
                                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => panic!("Proposal retry worker stopped"),
                            }
                        }
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
