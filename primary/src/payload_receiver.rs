//! Gate remote GRBC delivery on local Narwhal worker receipts.
use crate::error::DagResult;
use crate::primary::{PrimaryWorkerMessage, Round};
use crate::proposal_processor::{report, QueuedProposal, StoredProposalHeader};
use crate::proposal_store::verify_worker_payload;
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::FutureExt as _;
use network::SimpleSender;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval_at, Duration, Instant};

struct Pending {
    queued: QueuedProposal,
    missing: BTreeMap<Digest, WorkerId>,
}

pub(crate) struct PayloadReceiver {
    name: PublicKey,
    committee: Committee,
    store: Store,
    round: Arc<AtomicU64>,
    gc_depth: Round,
    pending: HashMap<Digest, Pending>,
    delivered: HashSet<Digest>,
    order: VecDeque<Digest>,
    tx: Sender<StoredProposalHeader>,
    network: SimpleSender,
}

impl PayloadReceiver {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        round: Arc<AtomicU64>,
        gc_depth: Round,
        retry_ms: u64,
        mut proposals: Receiver<QueuedProposal>,
        mut receipts: Receiver<(Digest, WorkerId)>,
        tx: Sender<StoredProposalHeader>,
    ) {
        tokio::spawn(async move {
            let mut receiver = Self {
                name,
                committee,
                store,
                round,
                gc_depth,
                pending: HashMap::new(),
                delivered: HashSet::new(),
                order: VecDeque::new(),
                tx,
                network: SimpleSender::new(),
            };
            let period = Duration::from_millis(retry_ms.max(1));
            let mut repair = interval_at(Instant::now() + period, period);
            let mut incoming = true;
            let mut workers = true;
            loop {
                if !incoming && (receiver.pending.is_empty() || !workers) {
                    break;
                }
                tokio::select! {
                    proposal = proposals.recv(), if incoming => match proposal {
                        Some(queued) => report(receiver.receive(queued).await),
                        None => incoming = false,
                    },
                    notice = receipts.recv(), if workers => match notice {
                        Some((digest, worker)) => report(receiver.available(digest, worker).await),
                        None => workers = false,
                    },
                    _ = repair.tick() => receiver.retry(),
                }
            }
        });
    }

    fn oldest(&self) -> Round {
        self.round
            .load(Ordering::Relaxed)
            .saturating_sub(self.gc_depth)
    }

    async fn receive(&mut self, queued: QueuedProposal) -> DagResult<()> {
        let header = &queued.0.header;
        if header.round < self.oldest()
            || self.delivered.contains(&header.id)
            || self.pending.contains_key(&header.id)
        {
            return Ok(());
        }
        let missing = verify_worker_payload(&self.committee, &mut self.store, &queued.0).await?;
        if missing.is_empty() {
            return self.deliver(queued).await;
        }
        if missing
            .values()
            .any(|worker| self.committee.worker(&self.name, worker).is_err())
        {
            return Err(crate::error::DagError::MalformedHeader(header.id.clone()));
        }
        // An uncooperative author can only occupy its own repair slots.
        let limit = self.gc_depth.saturating_add(1).saturating_mul(2) as usize;
        if self
            .pending
            .values()
            .filter(|p| p.queued.0.header.author == header.author)
            .count()
            >= limit
        {
            return Ok(());
        }
        let requests = Self::by_worker(header.author, &missing);
        self.pending
            .insert(header.id.clone(), Pending { queued, missing });
        self.request(requests);
        Ok(())
    }

    async fn available(&mut self, digest: Digest, worker: WorkerId) -> DagResult<()> {
        let mut ready = Vec::new();
        for (id, p) in &mut self.pending {
            if p.missing.get(&digest) == Some(&worker) {
                p.missing.remove(&digest);
                if p.missing.is_empty() {
                    ready.push(id.clone());
                }
            }
        }
        for id in ready {
            let p = self.pending.remove(&id).unwrap();
            if p.queued.0.header.round >= self.oldest() {
                self.deliver(p.queued).await?;
            }
        }
        Ok(())
    }

    async fn deliver(&mut self, (proposal, guard): QueuedProposal) -> DagResult<()> {
        let id = proposal.header.id.clone();
        let stored = StoredProposalHeader::store(&mut self.store, proposal, Some(guard)).await?;
        self.tx
            .send(stored)
            .await
            .expect("Failed to deliver available proposal");
        self.delivered.insert(id.clone());
        self.order.push_back(id);
        if self.order.len()
            > self
                .committee
                .size()
                .saturating_mul(self.gc_depth as usize)
                .max(1)
        {
            self.delivered.remove(&self.order.pop_front().unwrap());
        }
        Ok(())
    }

    fn by_worker(
        author: PublicKey,
        missing: &BTreeMap<Digest, WorkerId>,
    ) -> BTreeMap<(PublicKey, WorkerId), Vec<Digest>> {
        let mut requests = BTreeMap::new();
        for (digest, &worker) in missing {
            requests
                .entry((author, worker))
                .or_insert_with(Vec::new)
                .push(digest.clone());
        }
        requests
    }

    fn request(&mut self, requests: BTreeMap<(PublicKey, WorkerId), Vec<Digest>>) {
        for ((author, worker), digests) in requests {
            if let Ok(address) = self.committee.worker(&self.name, &worker) {
                // Each request must fit the same transport frame as the proposal.
                for group in digests.chunks(4096) {
                    let message = PrimaryWorkerMessage::Synchronize(group.to_vec(), author);
                    let bytes =
                        bincode::serialize(&message).expect("Failed to encode batch request");
                    // A full worker link is retried on the timer; it must not block available headers.
                    let enqueue = self
                        .network
                        .send(address.primary_to_worker, Bytes::from(bytes));
                    let _ = tokio::task::unconstrained(enqueue).now_or_never();
                }
            }
        }
    }

    fn retry(&mut self) {
        let oldest = self.oldest();
        self.pending
            .retain(|_, p| p.queued.0.header.round >= oldest);
        let mut requests = BTreeMap::new();
        for pending in self.pending.values() {
            for (key, mut digests) in
                Self::by_worker(pending.queued.0.header.author, &pending.missing)
            {
                requests
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .append(&mut digests);
            }
        }
        for digests in requests.values_mut() {
            digests.sort_unstable();
            digests.dedup();
        }
        self.request(requests);
    }
}
