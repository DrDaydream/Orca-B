// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, ConsensusCommand, ConsensusMessage, Round};
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

pub mod aba;
use aba::{Aba, AbaAction, AbaMessage, BinaryValue, DeterministicCoin};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// The validated DAG contains blocks delivered by GRBC at grade 1.
///
/// A block is inserted exactly once when the primary first sends its certificate
/// to consensus. A later grade-2 upgrade does not create another VDag node.
type VDag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

#[derive(Default)]
struct AbaSupport {
    processed_grade_two: HashSet<Digest>,
    strong: HashSet<PublicKey>,
    strong_or_virtual: HashSet<PublicKey>,
}

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
    dag_by_digest: HashMap<Digest, Certificate>,
    /// Blocks locally delivered by GRBC with grade 1.
    vdag: VDag,
    /// Blocks for which a valid grade-2 proof has been delivered.
    grade_two: HashSet<Digest>,
    /// Digest index of blocks already present in the formal Dag.
    dag_digests: HashSet<Digest>,
    /// Direct digest lookup over Dag union VDag. This avoids scanning every
    /// round for each step of a reachability query.
    observed: HashMap<Digest, Certificate>,
    /// Locates an observed leader even before it reaches grade 1.
    observed_by_round: HashMap<Round, HashMap<PublicKey, Digest>>,
    strong_ancestors: HashMap<Digest, HashSet<Digest>>,
    strong_children: HashMap<Digest, HashSet<Digest>>,
    observed_strong_support: HashMap<(Round, Digest), HashSet<PublicKey>>,
    dag_strong_support: HashMap<(Round, Digest), HashSet<PublicKey>>,
    observed_direct_support: HashMap<(Round, Digest), HashSet<PublicKey>>,
    dag_direct_support: HashMap<(Round, Digest), HashSet<PublicKey>>,
    /// Number of strong/weak dependencies not yet known to have entered Dag.
    missing_dependencies: HashMap<Digest, usize>,
    /// Reverse dependency index used to wake only blocks affected by a newly
    /// promoted Dag certificate.
    dependency_waiters: HashMap<Digest, HashSet<Digest>>,
    /// Missing causal history authorized for direct Dag admission by a
    /// commit-ready leader. Arrival at any verified GRBC stage wakes it.
    forced_history_waiters: HashMap<Digest, HashSet<Round>>,
    promotion_queue: VecDeque<Digest>,
    aba_support: HashMap<(Round, Digest), AbaSupport>,
    /// The authority designated as leader for every round.
    leaders: HashMap<Round, PublicKey>,
    /// Leader rounds already committed, preventing duplicate commits.
    committed_leaders: HashSet<Round>,
    /// Leader rounds explicitly skipped by commit rule 3.
    skipped_leaders: HashSet<Round>,
    /// Commit-ready leaders waiting for the previous round's leader.
    pending_leaders: BTreeMap<Round, Certificate>,
    /// Time when the leader first satisfied a commit rule. Benchmark latency
    /// ends here rather than after predecessor/output waiting.
    rule_ready_at_ms: HashMap<Round, u128>,
    /// First commit rule that made each leader ready (1, 2, or 3/ABA fallback).
    leader_commit_rules: HashMap<Round, u8>,
    pending_order: HashMap<Round, Vec<Certificate>>,
    ready_pending: BTreeSet<Round>,
    commit_tx: Option<mpsc::UnboundedSender<Vec<Certificate>>>,
    aba_instances: HashMap<Round, Aba<DeterministicCoin>>,
    aba_inputs: HashSet<Round>,
    aba_decisions: HashMap<Round, BinaryValue>,
    buffered_aba: HashMap<Round, Vec<(PublicKey, AbaMessage)>>,
    /// ABA broadcasts accumulated during one consensus ingress batch.
    aba_outbox: Vec<Vec<u8>>,
    missing_leader_requests: HashSet<Round>,
    /// Leaders made commit-ready directly by rules 1 or 2. ABA can help
    /// propagate input 1 but can never override this local fast-path result.
    direct_commit_ready: HashSet<Round>,
    /// Highest leader round whose r+3 deadline has been processed. This keeps
    /// zero-input handling incremental instead of rescanning round 1 onward.
    zero_input_checked_through: Round,
    highest_entered_round: Round,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        let genesis_dag: Dag = [(0, genesis)].iter().cloned().collect();

        let dag_by_digest: HashMap<_, _> = genesis_dag
            .values()
            .flat_map(|authorities| authorities.values())
            .map(|(digest, certificate)| (digest.clone(), certificate.clone()))
            .collect();
        let dag_digests = dag_by_digest.keys().cloned().collect();
        let observed = genesis_dag
            .values()
            .flat_map(|authorities| authorities.values())
            .map(|(digest, certificate)| (digest.clone(), certificate.clone()))
            .collect();
        let observed_by_round = genesis_dag
            .iter()
            .map(|(round, authorities)| {
                (
                    *round,
                    authorities
                        .iter()
                        .map(|(authority, (digest, _))| (*authority, digest.clone()))
                        .collect(),
                )
            })
            .collect();

        Self {
            last_committed_round: 0,
            last_committed: genesis_dag
                .get(&0)
                .unwrap()
                .iter()
                .map(|(x, (_, y))| (*x, y.round()))
                .collect(),
            dag: genesis_dag,
            dag_by_digest,
            // Genesis blocks already belong to the ordering DAG, so they must
            // not also appear in VDag.
            vdag: HashMap::new(),
            grade_two: HashSet::new(),
            dag_digests,
            observed,
            observed_by_round,
            strong_ancestors: HashMap::new(),
            strong_children: HashMap::new(),
            observed_strong_support: HashMap::new(),
            dag_strong_support: HashMap::new(),
            observed_direct_support: HashMap::new(),
            dag_direct_support: HashMap::new(),
            missing_dependencies: HashMap::new(),
            dependency_waiters: HashMap::new(),
            forced_history_waiters: HashMap::new(),
            promotion_queue: VecDeque::new(),
            aba_support: HashMap::new(),
            leaders: HashMap::new(),
            committed_leaders: [0].iter().cloned().collect(),
            skipped_leaders: HashSet::new(),
            pending_leaders: BTreeMap::new(),
            rule_ready_at_ms: HashMap::new(),
            leader_commit_rules: HashMap::new(),
            pending_order: HashMap::new(),
            ready_pending: BTreeSet::new(),
            commit_tx: None,
            aba_instances: HashMap::new(),
            aba_inputs: HashSet::new(),
            aba_decisions: HashMap::new(),
            buffered_aba: HashMap::new(),
            aba_outbox: Vec::new(),
            missing_leader_requests: HashSet::new(),
            direct_commit_ready: HashSet::new(),
            zero_input_checked_through: 0,
            highest_entered_round: 1,
        }
    }

    fn record_rule_ready(&mut self, round: Round) {
        self.rule_ready_at_ms.entry(round).or_insert_with(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System clock is before Unix epoch")
                .as_millis()
        });
    }

    /// Record valid data seen at any GRBC stage. If it belongs to the causal
    /// history of a commit-ready leader, admit it immediately without waiting
    /// for grade 1 or grade 2.
    fn observe(&mut self, certificate: Certificate) -> HashSet<Round> {
        let digest = certificate.digest();
        self.observed_by_round
            .entry(certificate.round())
            .or_default()
            .insert(certificate.origin(), digest.clone());
        match self.observed.entry(digest.clone()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(certificate);
                return HashSet::new();
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(certificate.clone());
            }
        }
        self.index_strong_paths(&certificate);

        let owners = self
            .forced_history_waiters
            .remove(&digest)
            .unwrap_or_default();
        for owner_round in &owners {
            self.force_observed_history_to_dag(certificate.clone(), *owner_round);
        }
        owners
    }

    fn index_strong_paths(&mut self, certificate: &Certificate) {
        let digest = certificate.digest();
        let mut additions = HashSet::new();
        for parent in &certificate.header.parents {
            self.observed_direct_support
                .entry((certificate.round(), parent.clone()))
                .or_default()
                .insert(certificate.origin());
            self.strong_children
                .entry(parent.clone())
                .or_default()
                .insert(digest.clone());
            additions.insert(parent.clone());
            if let Some(ancestors) = self.strong_ancestors.get(parent) {
                additions.extend(ancestors.iter().cloned());
            }
        }
        self.propagate_strong_ancestors(digest, additions);
    }

    fn propagate_strong_ancestors(&mut self, source: Digest, additions: HashSet<Digest>) {
        let mut pending = vec![(source, additions)];
        while let Some((digest, candidates)) = pending.pop() {
            let ancestors = self.strong_ancestors.entry(digest.clone()).or_default();
            let fresh: HashSet<_> = candidates
                .into_iter()
                .filter(|ancestor| ancestors.insert(ancestor.clone()))
                .collect();
            if fresh.is_empty() {
                continue;
            }
            if let Some(block) = self.observed.get(&digest) {
                for ancestor in &fresh {
                    self.observed_strong_support
                        .entry((block.round(), ancestor.clone()))
                        .or_default()
                        .insert(block.origin());
                    if self.dag_digests.contains(&digest) {
                        self.dag_strong_support
                            .entry((block.round(), ancestor.clone()))
                            .or_default()
                            .insert(block.origin());
                    }
                }
            }
            if let Some(children) = self.strong_children.get(&digest).cloned() {
                for child in children {
                    pending.push((child, fresh.clone()));
                }
            }
        }
    }

    /// Orca-A forced admission: once a leader is commit-ready, insert every
    /// currently observed strong/weak/virtual ancestor directly into Dag and
    /// remember missing digests so their later observation completes history.
    fn force_observed_history_to_dag(&mut self, root: Certificate, owner_round: Round) {
        let mut pending = vec![root];
        let mut visited = HashSet::new();
        while let Some(certificate) = pending.pop() {
            let digest = certificate.digest();
            if !visited.insert(digest.clone()) {
                continue;
            }
            for dependency in certificate
                .header
                .parents
                .iter()
                .chain(&certificate.header.weak_edges)
                .chain(&certificate.header.virtual_edges)
            {
                if self.dag_digests.contains(dependency) {
                    continue;
                }
                if let Some(ancestor) = self.observed.get(dependency).cloned() {
                    pending.push(ancestor);
                } else {
                    self.forced_history_waiters
                        .entry(dependency.clone())
                        .or_default()
                        .insert(owner_round);
                }
            }
            if !self.dag_digests.contains(&digest) {
                self.promote_to_dag(certificate);
            }
        }
    }

    /// Insert a block delivered by GRBC at grade 1 into the validated DAG.
    fn insert_grade_one(&mut self, certificate: Certificate) -> HashSet<Round> {
        let round = certificate.round();
        let origin = certificate.origin();
        let digest = certificate.digest();
        if self.dag_digests.contains(&digest) {
            return self.observe(certificate);
        }
        let missing: HashSet<_> = certificate
            .header
            .parents
            .iter()
            .chain(&certificate.header.weak_edges)
            .filter(|dependency| !self.dag_digests.contains(*dependency))
            .cloned()
            .collect();
        for dependency in &missing {
            self.dependency_waiters
                .entry(dependency.clone())
                .or_default()
                .insert(digest.clone());
        }
        self.missing_dependencies
            .insert(digest.clone(), missing.len());
        if missing.is_empty() && self.grade_two.contains(&digest) {
            self.promotion_queue.push_back(digest.clone());
        }
        self.vdag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(origin, (digest, certificate.clone()));
        self.observe(certificate)
    }

    /// Promote a grade-1 block into Tusk's ordering DAG. A block contained in
    /// Dag must never remain in VDag.
    fn promote_to_dag(&mut self, certificate: Certificate) {
        let round = certificate.round();
        let origin = certificate.origin();
        let digest = certificate.digest();

        self.observed_by_round
            .entry(round)
            .or_default()
            .insert(origin, digest.clone());
        self.observed
            .entry(digest.clone())
            .or_insert_with(|| certificate.clone());

        if let Some(authorities) = self.vdag.get_mut(&round) {
            let same_block = authorities
                .get(&origin)
                .map_or(false, |(vdag_digest, _)| vdag_digest == &digest);
            if same_block {
                authorities.remove(&origin);
            }
            if authorities.is_empty() {
                self.vdag.remove(&round);
            }
        }

        self.dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(origin, (digest.clone(), certificate.clone()));
        self.dag_by_digest.insert(digest.clone(), certificate);
        if !self.dag_digests.insert(digest.clone()) {
            return;
        }
        if let Some(ancestors) = self.strong_ancestors.get(&digest) {
            for ancestor in ancestors {
                self.dag_strong_support
                    .entry((round, ancestor.clone()))
                    .or_default()
                    .insert(origin);
            }
        }
        for parent in &self
            .dag_by_digest
            .get(&digest)
            .expect("new Dag block missing from digest index")
            .header
            .parents
        {
            self.dag_direct_support
                .entry((round, parent.clone()))
                .or_default()
                .insert(origin);
        }
        if let Some(waiters) = self.dependency_waiters.remove(&digest) {
            for waiter in waiters {
                if let Some(missing) = self.missing_dependencies.get_mut(&waiter) {
                    *missing = missing.saturating_sub(1);
                    if *missing == 0 && self.grade_two.contains(&waiter) {
                        self.promotion_queue.push_back(waiter);
                    }
                }
            }
        }
        self.wake_pending(round);
    }

    fn predecessor_resolved(&self, round: Round) -> bool {
        round > 0
            && (self.committed_leaders.contains(&(round - 1))
                || self.skipped_leaders.contains(&(round - 1)))
    }

    fn wake_pending(&mut self, round: Round) {
        if self.pending_leaders.contains_key(&round) && self.predecessor_resolved(round) {
            self.ready_pending.insert(round);
        }
    }

    fn mark_skipped(&mut self, round: Round) -> bool {
        let inserted = self.skipped_leaders.insert(round);
        if inserted {
            self.wake_pending(round + 1);
        }
        inserted
    }

    fn mark_grade_two(&mut self, digest: Digest) -> bool {
        let inserted = self.grade_two.insert(digest.clone());
        if inserted
            && self.missing_dependencies.get(&digest) == Some(&0)
            && self.observed.contains_key(&digest)
        {
            self.promotion_queue.push_back(digest);
        }
        inserted
    }

    /// Promote every grade-2 VDag block whose strong and weak dependencies are
    /// already present in Dag. Repeat because one promotion may unblock another.
    fn promote_ready(&mut self) -> Vec<Certificate> {
        let mut promoted = Vec::new();
        while let Some(digest) = self.promotion_queue.pop_front() {
            if self.dag_digests.contains(&digest)
                || !self.grade_two.contains(&digest)
                || self.missing_dependencies.get(&digest) != Some(&0)
            {
                continue;
            }
            let certificate = match self.observed.get(&digest).cloned() {
                Some(certificate) => certificate,
                None => continue,
            };
            self.promote_to_dag(certificate.clone());
            self.missing_dependencies.remove(&digest);
            promoted.push(certificate);
        }
        promoted
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificates: &[Certificate], gc_depth: Round) {
        for certificate in certificates {
            self.last_committed
                .entry(certificate.origin())
                .and_modify(|r| *r = max(*r, certificate.round()))
                .or_insert_with(|| certificate.round());
        }

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        let last_committed = &self.last_committed;
        self.dag.retain(|r, authorities| {
            authorities.retain(|name, _| last_committed.get(name).map_or(true, |round| r >= round));
            !authorities.is_empty() && *r + gc_depth >= last_committed_round
        });
        self.vdag.retain(|r, authorities| {
            authorities.retain(|name, _| last_committed.get(name).map_or(true, |round| r >= round));
            !authorities.is_empty() && *r + gc_depth >= last_committed_round
        });
    }

    fn gc_protocol_state(&mut self, gc_depth: Round) {
        if !self.pending_leaders.is_empty() {
            return;
        }
        let gc_round = self.last_committed_round.saturating_sub(gc_depth);
        let retained: HashSet<_> = self
            .dag
            .values()
            .chain(self.vdag.values())
            .flat_map(|authorities| authorities.values())
            .map(|(digest, _)| digest.clone())
            .collect();
        self.dag_by_digest
            .retain(|digest, _| retained.contains(digest));
        self.observed.retain(|digest, certificate| {
            retained.contains(digest) || certificate.round() >= gc_round
        });
        let observed = &self.observed;
        self.observed_by_round.retain(|round, blocks| {
            blocks.retain(|_, digest| observed.contains_key(digest));
            *round >= gc_round && !blocks.is_empty()
        });
        self.grade_two.retain(|digest| retained.contains(digest));
        let observed = &self.observed;
        self.strong_ancestors
            .retain(|digest, _| observed.contains_key(digest));
        self.strong_children.retain(|digest, children| {
            children.retain(|child| observed.contains_key(child));
            observed.contains_key(digest) || !children.is_empty()
        });
        self.missing_dependencies
            .retain(|digest, _| retained.contains(digest));
        self.dependency_waiters.retain(|_, waiters| {
            waiters.retain(|digest| retained.contains(digest));
            !waiters.is_empty()
        });
        let dag_digests = &self.dag_digests;
        self.forced_history_waiters.retain(|digest, owners| {
            owners.retain(|round| *round >= gc_round);
            !owners.is_empty() && !dag_digests.contains(digest)
        });
        self.promotion_queue
            .retain(|digest| retained.contains(digest));
        self.observed_strong_support
            .retain(|(round, _), _| *round >= gc_round);
        self.dag_strong_support
            .retain(|(round, _), _| *round >= gc_round);
        self.observed_direct_support
            .retain(|(round, _), _| *round >= gc_round);
        self.dag_direct_support
            .retain(|(round, _), _| *round >= gc_round);
        let active_aba: HashSet<_> = self.aba_instances.keys().cloned().collect();
        self.aba_support
            .retain(|(round, _), _| *round >= gc_round || active_aba.contains(round));
        // `dag_digests` is intentionally monotonic: membership proves that a
        // dependency entered Dag before its full certificate was garbage
        // collected, and later promotions still rely on that proof.
        // Unfinished ABA instances are independent of DAG GC and retain all
        // BVAL/AUX/DECIDE state until they produce a certified output.
        self.aba_inputs
            .retain(|round| *round >= gc_round || active_aba.contains(round));
        self.aba_decisions.retain(|round, _| *round >= gc_round);
        let aba_decisions = &self.aba_decisions;
        self.buffered_aba
            .retain(|round, _| *round >= gc_round || !aba_decisions.contains_key(round));
        self.missing_leader_requests
            .retain(|round| *round >= gc_round);
        self.direct_commit_ready.retain(|round| *round >= gc_round);
        self.leaders.retain(|round, _| *round >= gc_round);
        self.committed_leaders.retain(|round| *round >= gc_round);
        self.skipped_leaders.retain(|round| *round >= gc_round);
    }
}

pub struct Consensus {
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<ConsensusMessage>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<ConsensusCommand>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: OutputSender,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

#[derive(Clone)]
enum OutputSender {
    Individual(Sender<Certificate>),
    Batch(Sender<Vec<Certificate>>),
}

impl Consensus {
    fn adversarial_schedule_enabled() -> bool {
        std::env::var("ORCA_FAULTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map_or(false, |faults| faults > 0)
    }

    fn scheduled_rule(&self, round: Round) -> u8 {
        if !Self::adversarial_schedule_enabled() {
            return 0;
        }
        let index = round.saturating_sub(1);
        ((index + index / 3) % 3 + 1) as u8
    }

    fn mark_rule_skipped(&self, round: Round, rule: u8, state: &mut State) {
        if state.mark_skipped(round) {
            #[cfg(feature = "benchmark")]
            info!(
                "Commit rule stats leader round-{} rule {} outcome skip blocks 0",
                round, rule
            );
        }
    }

    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<ConsensusMessage>,
        tx_primary: Sender<ConsensusCommand>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output: OutputSender::Individual(tx_output),
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    /// Production entry point with one channel operation per ordered commit
    /// batch. The legacy `spawn` API remains for unit tests and embedders that
    /// consume certificates individually.
    pub fn spawn_batch(
        name: PublicKey,
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<ConsensusMessage>,
        tx_primary: Sender<ConsensusCommand>,
        tx_output: Sender<Vec<Certificate>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output: OutputSender::Batch(tx_output),
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        let (commit_tx, mut commit_rx) = mpsc::unbounded_channel::<Vec<Certificate>>();
        state.commit_tx = Some(commit_tx);
        let tx_cleanup = self.tx_primary.clone();
        let tx_output = self.tx_output.clone();
        tokio::spawn(async move {
            while let Some(sequence) = commit_rx.recv().await {
                if tx_cleanup
                    .send(ConsensusCommand::CleanupBatch(sequence.clone()))
                    .await
                    .is_err()
                {
                    return;
                }
                let output_failed = match &tx_output {
                    OutputSender::Batch(sender) => sender.send(sequence).await.is_err(),
                    OutputSender::Individual(sender) => {
                        let mut failed = false;
                        for certificate in sequence {
                            if sender.send(certificate).await.is_err() {
                                failed = true;
                                break;
                            }
                        }
                        failed
                    }
                };
                if output_failed {
                    return;
                }
            }
        });

        // Listen to incoming certificates.
        while let Some(message) = self.rx_primary.recv().await {
            if let ConsensusMessage::RoundAdvanced(round) = message {
                if round > state.highest_entered_round {
                    state.highest_entered_round = round;
                    self.input_zero_at_round_end(round, &mut state).await;
                    self.flush_aba_outbox(&mut state).await;
                }
                continue;
            }
            let (
                observed_round,
                observed_origin,
                first_observation,
                grade_two_changed,
                promoted,
                history_owners,
            ) = match message {
                ConsensusMessage::RoundAdvanced(_) => unreachable!(),
                ConsensusMessage::Observed(header) => {
                    let round = header.round;
                    let certificate = Certificate {
                        header,
                        votes: Vec::new(),
                    };
                    let origin = certificate.origin();
                    let first = !state.observed.contains_key(&certificate.digest());
                    let owners = state.observe(certificate);
                    (round, origin, first, false, Vec::new(), owners)
                }
                ConsensusMessage::GradeOne(certificate) => {
                    debug!("Grade 1 delivered {:?}", certificate);
                    let round = certificate.round();
                    let origin = certificate.origin();
                    let first = !state.observed.contains_key(&certificate.digest());
                    let owners = state.insert_grade_one(certificate);
                    (round, origin, first, false, state.promote_ready(), owners)
                }
                ConsensusMessage::GradeTwo(certificate) => {
                    debug!("Grade 2 delivered {:?}", certificate);
                    let round = certificate.round();
                    let first = !state.observed.contains_key(&certificate.digest());
                    let owners = state.observe(certificate.clone());
                    let changed = state.mark_grade_two(certificate.digest());
                    (
                        round,
                        certificate.origin(),
                        first,
                        changed,
                        state.promote_ready(),
                        owners,
                    )
                }
                ConsensusMessage::Aba(sender, bytes) => {
                    match bincode::deserialize::<AbaMessage>(&bytes) {
                        Ok(message) => self.process_aba_message(sender, message, &mut state).await,
                        Err(error) => warn!("Ignoring malformed ABA message: {}", error),
                    }
                    self.flush_aba_outbox(&mut state).await;
                    continue;
                }
                ConsensusMessage::AbaBatch(sender, batch) => {
                    for bytes in batch {
                        match bincode::deserialize::<AbaMessage>(&bytes) {
                            Ok(message) => {
                                self.process_aba_message(sender, message, &mut state).await
                            }
                            Err(error) => warn!("Ignoring malformed ABA message: {}", error),
                        }
                    }
                    self.flush_aba_outbox(&mut state).await;
                    continue;
                }
            };

            // Refresh only cached orders affected by newly arrived forced
            // causal history; this avoids rescanning historical leaders.
            for owner_round in history_owners {
                if let Some(leader) = state.pending_leaders.get(&owner_round).cloned() {
                    let ordered = self.order_dag(&leader, &state);
                    state.pending_order.insert(owner_round, ordered);
                    state.wake_pending(owner_round);
                }
            }

            // Designation happens as soon as the round is observed, even if
            // none of its grade-1 blocks is ready to enter Dag yet.
            let designated = self.leader_authority(observed_round);
            if state.leaders.insert(observed_round, designated).is_none() {
                debug!("Round {} designated leader {}", observed_round, designated);
            }
            if first_observation {
                self.ensure_aba_instance(observed_round, &mut state).await;
                // The designated leader may arrive after its r+2 Grade-2
                // supporters. Reconcile only that leader's GRBC-derived ABA
                // support instead of rescanning historical leaders.
                if observed_origin == self.ordering_leader_authority(observed_round) {
                    self.evaluate_aba_r_plus_two_input(observed_round, &mut state)
                        .await;
                }
            }

            // Incremental hot path: a block in support round r can affect only
            // the leaders at r-1 (rule 1) and r-2 (rule 2 / ABA input). Also
            // inspect rounds of blocks newly promoted from VDag, since a late
            // promotion can cross a Dag-only threshold.
            let promoted_rounds: HashSet<_> = promoted.iter().map(Certificate::round).collect();
            let mut affected_rounds = promoted_rounds.clone();
            if first_observation {
                affected_rounds.insert(observed_round);
            }
            for support_round in affected_rounds {
                self.evaluate_commit_rule_one(support_round, &mut state, true)
                    .await;
                self.evaluate_commit_rule_two(support_round, &mut state, true)
                    .await;
            }
            let mut aba_rounds = promoted_rounds;
            if grade_two_changed {
                aba_rounds.insert(observed_round);
            }
            for support_round in aba_rounds {
                if support_round >= 3 {
                    self.evaluate_aba_r_plus_two_input(support_round - 2, &mut state)
                        .await;
                }
            }

            self.resolve_requested_leaders(&mut state).await;
            self.flush_aba_outbox(&mut state).await;
        }
    }

    async fn flush_aba_outbox(&mut self, state: &mut State) {
        if state.aba_outbox.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut state.aba_outbox);
        self.tx_primary
            .send(ConsensusCommand::AbaBroadcast(batch))
            .await
            .expect("Failed to send ABA batch to primary");
    }

    async fn ensure_aba_instance(&mut self, leader_round: Round, state: &mut State) {
        if state.aba_decisions.contains_key(&leader_round) {
            return;
        }
        if state.aba_instances.contains_key(&leader_round) {
            return;
        }
        state.aba_instances.insert(
            leader_round,
            Aba::new(
                self.committee.clone(),
                self.name,
                leader_round,
                DeterministicCoin::new(0x4f52_4341),
            ),
        );
        if let Some(buffered) = state.buffered_aba.remove(&leader_round) {
            for (sender, message) in buffered {
                self.process_aba_message(sender, message, state).await;
            }
        }
    }

    async fn process_aba_message(
        &mut self,
        sender: PublicKey,
        message: AbaMessage,
        state: &mut State,
    ) {
        let instance = message.instance;
        if !state.aba_instances.contains_key(&instance) {
            if state.aba_decisions.contains_key(&instance) {
                return;
            }
            state
                .buffered_aba
                .entry(instance)
                .or_default()
                .push((sender, message));
            return;
        }
        let actions = state
            .aba_instances
            .get_mut(&instance)
            .unwrap()
            .handle_message(sender, message);
        self.handle_aba_actions(instance, actions, state).await;
    }

    async fn offer_aba_input(
        &mut self,
        leader_round: Round,
        value: BinaryValue,
        state: &mut State,
    ) {
        if state.aba_decisions.contains_key(&leader_round) {
            return;
        }
        self.ensure_aba_instance(leader_round, state).await;
        if !state.aba_inputs.insert(leader_round) {
            return;
        }
        debug!("ABA instance {} receives input {:?}", leader_round, value);
        let actions = state
            .aba_instances
            .get_mut(&leader_round)
            .unwrap()
            .propose(value);
        self.handle_aba_actions(leader_round, actions, state).await;
    }

    async fn handle_aba_actions(
        &mut self,
        leader_round: Round,
        actions: Vec<AbaAction>,
        state: &mut State,
    ) {
        for action in actions {
            match action {
                AbaAction::Broadcast(message) => {
                    let bytes =
                        bincode::serialize(&message).expect("Failed to serialize ABA message");
                    state.aba_outbox.push(bytes);
                }
                AbaAction::Decide(value) => {
                    // ABA is the cold-path trigger for an older leader. Before
                    // applying its output, perform one final targeted scan of
                    // that leader's rule-1 and rule-2 support rounds. A direct
                    // rule result retains precedence over ABA 0.
                    Box::pin(self.evaluate_commit_rule_one(leader_round + 1, state, false)).await;
                    Box::pin(self.evaluate_commit_rule_two(leader_round + 2, state, false)).await;
                    if state.aba_decisions.insert(leader_round, value).is_none() {
                        self.apply_aba_decision(leader_round, value, state).await;
                    }
                }
            }
        }
        // A certified DECIDE contains f+1 authenticated votes, so reliable
        // dissemination no longer depends on this instance retaining all of
        // its BVAL/AUX round state.
        let certified = state
            .aba_instances
            .get(&leader_round)
            .map_or(false, Aba::decision_certified);
        if certified {
            state.aba_instances.remove(&leader_round);
            state.buffered_aba.remove(&leader_round);
        }
    }

    async fn apply_aba_decision(
        &mut self,
        leader_round: Round,
        value: BinaryValue,
        state: &mut State,
    ) {
        // An ABA result produced before entering r+3 belongs to rule 2. Once
        // r+3 has been entered, the same fallback is accounted to rule 3.
        let aba_rule = if state.highest_entered_round <= leader_round + 2 {
            2
        } else {
            3
        };
        match value {
            BinaryValue::Zero => {
                if state.direct_commit_ready.contains(&leader_round) {
                    debug!(
                        "Ignoring ABA 0 for directly commit-ready leader round {}",
                        leader_round
                    );
                    return;
                }
                debug!("ABA skips leader round {}", leader_round);
                self.mark_rule_skipped(leader_round, aba_rule, state);
                self.drain_ready_leaders(state).await;
            }
            BinaryValue::One => {
                if state.direct_commit_ready.contains(&leader_round) {
                    return;
                }
                if let Some(leader) = self.observed_leader(leader_round, state) {
                    let digest = leader.digest();
                    state.mark_grade_two(digest);
                    state.promote_ready();
                    state.force_observed_history_to_dag(leader.clone(), leader_round);
                    self.queue_leader_commit(leader, aba_rule, state).await;
                } else if state.missing_leader_requests.insert(leader_round) {
                    state
                        .leader_commit_rules
                        .entry(leader_round)
                        .or_insert(aba_rule);
                    let authority = self.ordering_leader_authority(leader_round);
                    self.tx_primary
                        .send(ConsensusCommand::LeaderRequest(leader_round, authority))
                        .await
                        .expect("Failed to request decided leader");
                }
            }
        }
    }

    async fn resolve_requested_leaders(&mut self, state: &mut State) {
        let rounds: Vec<_> = state.missing_leader_requests.iter().cloned().collect();
        for round in rounds {
            if let Some(leader) = self.observed_leader(round, state) {
                state.missing_leader_requests.remove(&round);
                let digest = leader.digest();
                state.mark_grade_two(digest);
                state.promote_ready();
                state.force_observed_history_to_dag(leader.clone(), round);
                let rule = state.leader_commit_rules.get(&round).copied().unwrap_or(3);
                self.queue_leader_commit(leader, rule, state).await;
            }
        }
    }

    async fn evaluate_aba_r_plus_two_input(&mut self, leader_round: Round, state: &mut State) {
        if state.aba_inputs.contains(&leader_round) {
            return;
        }
        let leader = match self.observed_leader(leader_round, state) {
            Some(leader) => leader,
            None => return,
        };
        let digest = leader.digest();
        let support_round = leader_round + 2;
        let key = (support_round, digest.clone());
        let mut support = state.aba_support.remove(&key).unwrap_or_default();
        let blocks: Vec<_> = state
            .dag
            .get(&support_round)
            .into_iter()
            .flat_map(|x| x.values())
            .chain(
                state
                    .vdag
                    .get(&support_round)
                    .into_iter()
                    .flat_map(|x| x.values()),
            )
            .filter(|(block_digest, _)| state.grade_two.contains(block_digest))
            .map(|(block_digest, block)| (block_digest.clone(), block.clone()))
            .collect();
        let fresh: Vec<_> = blocks
            .into_iter()
            .filter(|(block_digest, _)| support.processed_grade_two.insert(block_digest.clone()))
            .map(|(_, block)| block)
            .collect();
        let classified = self.classify_paths_parallel(&fresh, &digest, state);
        // Worker threads only read immutable indexes. All protocol state is
        // merged here, in the single ordered consensus task.
        for (origin, strong, strong_or_virtual) in classified {
            if strong {
                support.strong.insert(origin);
            }
            if strong_or_virtual {
                support.strong_or_virtual.insert(origin);
            }
        }
        let any_strong = !support.strong.is_empty();
        let stake: Stake = support
            .strong_or_virtual
            .iter()
            .map(|name| self.committee.stake(name))
            .sum();
        state.aba_support.insert(key, support);
        if any_strong || stake >= self.committee.validity_threshold() {
            self.offer_aba_input(leader_round, BinaryValue::One, state)
                .await;
        }
    }

    async fn input_zero_at_round_end(&mut self, current_round: Round, state: &mut State) {
        let deadline = current_round.saturating_sub(3);
        for leader_round in state.zero_input_checked_through.saturating_add(1)..=deadline {
            if !state.aba_inputs.contains(&leader_round) {
                self.offer_aba_input(leader_round, BinaryValue::Zero, state)
                    .await;
            }
        }
        state.zero_input_checked_through = state.zero_input_checked_through.max(deadline);
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        let leader = self.ordering_leader_authority(round);

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Deterministically designates one authority as leader for every round.
    /// Keeping this separate from `leader` means a round has a designated
    /// leader even when that authority's certificate has not arrived yet.
    fn leader_authority(&self, round: Round) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();

        let coin = round;

        keys[coin as usize % self.committee.size()]
    }

    /// Returns `(observed_stake, dag_stake)` for round-`round` blocks that
    /// strongly reference `leader_digest`. Observed support is the union of
    /// Dag and VDag and counts each authority at most once.
    fn strong_support_stake(
        &self,
        round: Round,
        leader_digest: &Digest,
        state: &State,
    ) -> (Stake, Stake) {
        let empty = HashSet::new();
        let observed = state
            .observed_direct_support
            .get(&(round, leader_digest.clone()))
            .unwrap_or(&empty);
        let dag = state
            .dag_direct_support
            .get(&(round, leader_digest.clone()))
            .unwrap_or(&empty);
        let observed_stake = observed
            .iter()
            .map(|authority| self.committee.stake(authority))
            .sum();
        let dag_stake = dag
            .iter()
            .map(|authority| self.committee.stake(authority))
            .sum();
        (observed_stake, dag_stake)
    }

    /// Evaluate commit rule 1 using round `r` as support for the leader of
    /// round `r-1`. A leader already marked commit-ready is never rechecked.
    async fn evaluate_commit_rule_one(
        &mut self,
        r: Round,
        state: &mut State,
        propagate_aba_input: bool,
    ) {
        if r < 2 {
            return;
        }
        let leader_round = r - 1;
        if Self::adversarial_schedule_enabled() && self.scheduled_rule(leader_round) != 1 {
            return;
        }
        if state.committed_leaders.contains(&leader_round)
            || state.pending_leaders.contains_key(&leader_round)
        {
            return;
        }
        let leader = match self.observed_leader(leader_round, state) {
            Some(leader) => leader,
            None => return,
        };
        let leader_digest = leader.digest();

        let (observed_stake, dag_stake) = self.strong_support_stake(r, &leader_digest, state);
        if observed_stake < self.committee.quorum_threshold()
            && dag_stake < self.committee.validity_threshold()
        {
            return;
        }

        debug!(
            "Leader {:?} satisfies commit rule 1: observed {}, Dag {}",
            leader, observed_stake, dag_stake
        );
        state.direct_commit_ready.insert(leader.round());
        state.skipped_leaders.remove(&leader.round());
        state.force_observed_history_to_dag(leader.clone(), leader_round);
        if propagate_aba_input {
            self.offer_aba_input(leader.round(), BinaryValue::One, state)
                .await;
        }
        self.queue_leader_commit(leader, 1, state).await;
    }

    /// Evaluate commit rule 2 for observed round `q` and leader round `q-2`.
    async fn evaluate_commit_rule_two(
        &mut self,
        q: Round,
        state: &mut State,
        propagate_aba_input: bool,
    ) {
        if q < 3 {
            return;
        }
        let leader_round = q - 2;
        if Self::adversarial_schedule_enabled() && self.scheduled_rule(leader_round) != 2 {
            return;
        }
        if state.committed_leaders.contains(&leader_round)
            || state.pending_leaders.contains_key(&leader_round)
        {
            return;
        }

        let leader = match self.observed_leader(leader_round, state) {
            Some(leader) => leader,
            None => return,
        };
        let leader_digest = leader.digest();
        let (observed_strong, dag_strong, dag_strong_or_virtual) =
            self.rule_two_support_stake(q, &leader_digest, state);

        let condition_one = observed_strong >= self.committee.quorum_threshold();
        let condition_two = dag_strong >= self.committee.validity_threshold();
        let condition_three = dag_strong_or_virtual >= self.committee.quorum_threshold();
        if !condition_one && !condition_two && !condition_three {
            return;
        }

        if condition_three && !state.grade_two.contains(&leader_digest) {
            debug!("Commit rule 2 forces grade 2 for {:?}", leader);
            state.mark_grade_two(leader_digest.clone());
            state.promote_ready();
        }

        // Conditions 1 and 2 normally operate on a grade-2 Leader already in
        // Dag. Condition 3 may promote it immediately above. If dependencies
        // still prevent promotion, the ordered pending queue retains it.
        debug!(
            "Leader {:?} satisfies commit rule 2: observed-strong {}, Dag-strong {}, Dag-strong-or-virtual {}",
            leader, observed_strong, dag_strong, dag_strong_or_virtual
        );
        state.direct_commit_ready.insert(leader.round());
        state.skipped_leaders.remove(&leader.round());
        state.force_observed_history_to_dag(leader.clone(), leader_round);
        if propagate_aba_input {
            self.offer_aba_input(leader.round(), BinaryValue::One, state)
                .await;
        }
        self.queue_leader_commit(leader, 2, state).await;
    }

    /// Counts rule-2 support in round `q`, with each authority counted once.
    fn rule_two_support_stake(
        &self,
        q: Round,
        leader_digest: &Digest,
        state: &State,
    ) -> (Stake, Stake, Stake) {
        let empty = HashSet::new();
        let dag_strong = state
            .dag_strong_support
            .get(&(q, leader_digest.clone()))
            .unwrap_or(&empty);
        let mut dag_strong_or_virtual = HashSet::new();
        let dag_blocks: Vec<_> = state
            .dag
            .get(&q)
            .into_iter()
            .flat_map(|round| round.values())
            .map(|(_, block)| block.clone())
            .collect();
        for (origin, _, strong_or_virtual) in
            self.classify_paths_parallel(&dag_blocks, leader_digest, state)
        {
            if strong_or_virtual {
                dag_strong_or_virtual.insert(origin);
            }
        }
        let observed_strong = state
            .observed_strong_support
            .get(&(q, leader_digest.clone()))
            .unwrap_or(&empty);

        let stake = |authorities: &HashSet<PublicKey>| {
            authorities
                .iter()
                .map(|authority| self.committee.stake(authority))
                .sum()
        };
        let result = (
            stake(observed_strong),
            stake(dag_strong),
            stake(&dag_strong_or_virtual),
        );
        result
    }

    /// Evaluate independent reachability queries in parallel and return only
    /// immutable results. The caller performs the deterministic state update.
    fn classify_paths_parallel(
        &self,
        blocks: &[Certificate],
        target: &Digest,
        state: &State,
    ) -> Vec<(PublicKey, bool, bool)> {
        if blocks.len() < 8 {
            return blocks
                .iter()
                .map(|block| {
                    let strong = self.has_strong_path(block, target, state);
                    let virtual_path =
                        strong || self.has_two_hop_virtual_path(block, target, state);
                    (block.origin(), strong, virtual_path)
                })
                .collect();
        }
        let workers = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1)
            .min(4)
            .min(blocks.len());
        let chunk_size = (blocks.len() + workers - 1) / workers;
        std::thread::scope(|scope| {
            let handles: Vec<_> = blocks
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || {
                        chunk
                            .iter()
                            .map(|block| {
                                let strong = self.has_strong_path(block, target, state);
                                let virtual_path =
                                    strong || self.has_two_hop_virtual_path(block, target, state);
                                (block.origin(), strong, virtual_path)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("path worker panicked"))
                .collect()
        })
    }

    /// Strong-path reachability over blocks observed in Dag union VDag.
    fn has_strong_path(&self, block: &Certificate, target: &Digest, state: &State) -> bool {
        if let Some(ancestors) = state.strong_ancestors.get(&block.digest()) {
            return ancestors.contains(target);
        }
        let mut pending: Vec<_> = block.header.parents.iter().cloned().collect();
        let mut visited = HashSet::new();
        while let Some(digest) = pending.pop() {
            if &digest == target {
                return true;
            }
            if !visited.insert(digest.clone()) {
                continue;
            }
            if let Some(parent) = Self::observed_certificate(&digest, state) {
                pending.extend(parent.header.parents.iter().cloned());
            }
        }
        false
    }

    /// Exactly two hops: one strong edge followed by one virtual edge to the
    /// target leader.
    fn has_two_hop_virtual_path(
        &self,
        block: &Certificate,
        target: &Digest,
        state: &State,
    ) -> bool {
        block.header.parents.iter().any(|parent_digest| {
            Self::observed_certificate(parent_digest, state)
                .map_or(false, |parent| parent.header.virtual_edges.contains(target))
        })
    }

    /// Counts exact three-edge virtual paths from `higher` to `lower`:
    /// higher --parent--> block --parent--> block --virtual--> lower.
    /// Paths are distinct when either intermediate block differs, so the
    /// identity of a path is `(first_digest, second_digest)`.
    #[allow(dead_code)]
    fn three_edge_virtual_path_stake(
        &self,
        higher: &Certificate,
        lower: &Digest,
        state: &State,
    ) -> Stake {
        let mut paths = HashSet::new();
        for first_digest in &higher.header.parents {
            if let Some(first) = Self::observed_certificate(first_digest, state) {
                for second_digest in &first.header.parents {
                    if Self::observed_certificate(second_digest, state)
                        .map_or(false, |second| second.header.virtual_edges.contains(lower))
                    {
                        paths.insert((first_digest.clone(), second_digest.clone()));
                    }
                }
            }
        }
        paths.len() as Stake
    }

    /// Commit rule 3 resolves leaders that did not satisfy rules 1 or 2.
    /// A commit-ready leader at round h observes leaders h-3, h-6, ... .
    /// Every adjacent pair in that chain must have either a strong path or
    /// f+1 distinct three-edge virtual paths. The target is
    /// marked commit-ready when the whole chain succeeds, otherwise skipped.
    #[allow(dead_code)]
    async fn evaluate_commit_rule_three(&mut self, state: &mut State) {
        let observers: Vec<_> = state.pending_leaders.keys().cloned().collect();
        for observer_round in observers {
            if observer_round < 4 {
                continue;
            }

            let mut target_round = observer_round - 3;
            loop {
                if !state.committed_leaders.contains(&target_round)
                    && !state.skipped_leaders.contains(&target_round)
                    && !state.pending_leaders.contains_key(&target_round)
                {
                    let target = self.observed_leader(target_round, state);
                    let mut chain_round = observer_round;
                    let mut chain_valid = target.is_some();

                    while chain_valid && chain_round > target_round {
                        let lower_round = chain_round - 3;
                        let higher = self.observed_leader(chain_round, state);
                        let lower = self.observed_leader(lower_round, state);
                        chain_valid = match (higher, lower) {
                            (Some(higher), Some(lower)) => {
                                let lower_digest = lower.digest();
                                self.has_strong_path(&higher, &lower_digest, state)
                                    || self.three_edge_virtual_path_stake(
                                        &higher,
                                        &lower_digest,
                                        state,
                                    ) >= self.committee.validity_threshold()
                            }
                            _ => false,
                        };
                        chain_round = lower_round;
                    }

                    if chain_valid {
                        let target = target.unwrap();
                        debug!(
                            "Leader {:?} marked commit-ready by commit rule 3 through round {}",
                            target, observer_round
                        );
                        self.queue_leader_commit(target, 3, state).await;
                    } else {
                        debug!(
                            "Skipping leader round {} by commit rule 3 observed from round {}",
                            target_round, observer_round
                        );
                        self.mark_rule_skipped(target_round, 3, state);
                    }
                    self.drain_ready_leaders(state).await;
                }

                if target_round < 4 {
                    break;
                }
                target_round -= 3;
            }
        }
    }

    fn observed_leader(&self, round: Round, state: &State) -> Option<Certificate> {
        let authority = self.ordering_leader_authority(round);
        state
            .dag
            .get(&round)
            .and_then(|blocks| blocks.get(&authority))
            .or_else(|| {
                state
                    .vdag
                    .get(&round)
                    .and_then(|blocks| blocks.get(&authority))
            })
            .map(|(_, certificate)| certificate.clone())
            .or_else(|| {
                state
                    .observed_by_round
                    .get(&round)
                    .and_then(|blocks| blocks.get(&authority))
                    .and_then(|digest| state.observed.get(digest))
                    .cloned()
            })
    }

    fn observed_certificate<'a>(digest: &Digest, state: &'a State) -> Option<&'a Certificate> {
        state.observed.get(digest).or_else(|| {
            // Compatibility fallback for restored/test state assembled
            // without going through `insert_grade_one`.
            state
                .dag
                .values()
                .chain(state.vdag.values())
                .flat_map(|round| round.values())
                .find(|(candidate, _)| candidate == digest)
                .map(|(_, certificate)| certificate)
        })
    }

    /// Queue a leader once and commit ready leaders in consecutive round order.
    async fn queue_leader_commit(&mut self, leader: Certificate, rule: u8, state: &mut State) {
        let round = leader.round();
        if state.committed_leaders.contains(&round) {
            return;
        }
        state.force_observed_history_to_dag(leader.clone(), round);
        state.record_rule_ready(round);
        state.leader_commit_rules.entry(round).or_insert(rule);
        let ordered = self.order_dag(&leader, state);
        #[cfg(feature = "benchmark")]
        for certificate in &ordered {
            if certificate.origin() != self.ordering_leader_authority(certificate.round()) {
                info!(
                    "Header rule-ordered round {} digest {:?}",
                    certificate.round(),
                    certificate.header.digest()
                );
            }
        }
        state.pending_order.insert(round, ordered);
        state.pending_leaders.entry(round).or_insert(leader);
        state.wake_pending(round);

        self.drain_ready_leaders(state).await;
    }

    async fn drain_ready_leaders(&mut self, state: &mut State) {
        loop {
            let ready_round = match state.ready_pending.iter().next().cloned() {
                Some(round) => {
                    state.ready_pending.remove(&round);
                    round
                }
                None => break,
            };
            let leader_ready = state
                .pending_leaders
                .get(&ready_round)
                .map_or(false, |leader| {
                    state.predecessor_resolved(ready_round)
                        && state.dag_digests.contains(&leader.digest())
                });
            if !leader_ready {
                continue;
            }
            let leader = state.pending_leaders.remove(&ready_round).unwrap();
            if !state.committed_leaders.insert(ready_round) {
                continue;
            }
            state.wake_pending(ready_round + 1);

            let mut sequence = state
                .pending_order
                .remove(&ready_round)
                .unwrap_or_else(|| self.order_dag(&leader, state));
            sequence.retain(|certificate| {
                state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or(true, |round| certificate.round() > *round)
            });
            let commit_rule = state.leader_commit_rules.remove(&ready_round).unwrap_or(3);
            #[cfg(feature = "benchmark")]
            info!(
                "Commit rule stats leader {:?} rule {} outcome commit blocks {}",
                leader.header.digest(),
                commit_rule,
                sequence.len()
            );
            let _rule_ready_at_ms =
                state
                    .rule_ready_at_ms
                    .remove(&ready_round)
                    .unwrap_or_else(|| {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("System clock is before Unix epoch")
                            .as_millis()
                    });
            let _committed_at_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System clock is before Unix epoch")
                .as_millis();
            state.update(&sequence, self.gc_depth);
            for certificate in &sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);
                #[cfg(feature = "benchmark")]
                info!(
                    "Header committed round {} digest {:?} leader {}",
                    certificate.round(),
                    certificate.header.digest(),
                    certificate.origin() == self.ordering_leader_authority(certificate.round())
                );
                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    info!(
                        "Committed {} -> {:?} @ {} commit {}",
                        certificate.header, digest, _rule_ready_at_ms, _committed_at_ms
                    );
                }
            }
            if let Some(commit_tx) = &state.commit_tx {
                commit_tx.send(sequence).expect("Commit writer stopped");
            } else {
                for certificate in sequence {
                    self.tx_primary
                        .send(ConsensusCommand::Cleanup(certificate.clone()))
                        .await
                        .expect("Failed to send certificate to primary");
                    match &self.tx_output {
                        OutputSender::Individual(sender) => {
                            if let Err(error) = sender.send(certificate).await {
                                warn!("Failed to output certificate: {}", error);
                            }
                        }
                        OutputSender::Batch(sender) => {
                            if let Err(error) = sender.send(vec![certificate]).await {
                                warn!("Failed to output certificate batch: {}", error);
                            }
                        }
                    }
                }
            }
        }

        if log_enabled!(log::Level::Debug) {
            for (name, round) in &state.last_committed {
                debug!("Latest commit of {}: Round {}", name, round);
            }
        }
        // Protocol metadata is collected only after the ordered waiting queue
        // has drained, so no pending leader loses the state it depends on.
        state.gc_protocol_state(self.gc_depth);
    }

    fn ordering_leader_authority(&self, _round: Round) -> PublicKey {
        #[cfg(test)]
        {
            let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
            keys.sort();
            keys[0]
        }
        #[cfg(not(test))]
        {
            self.leader_authority(_round)
        }
    }

    /// Order the past leaders that we didn't already commit.
    #[allow(dead_code)]
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for r in (state.last_committed_round + 2..leader.round())
            .rev()
            .step_by(2)
        {
            // Get the certificate proposed by the previous leader.
            let (_, prev_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if self.linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        to_commit
    }

    /// Checks if there is a path between two leaders.
    #[allow(dead_code)]
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    /// Checks whether `leader` reaches `prev_leader` through any combination
    /// of strong (`parents`) and weak (`weak_edges`) edges.
    ///
    /// Weak edges may skip rounds, so unlike `linked` this method performs a
    /// digest-based depth-first search rather than walking one round at a time.
    #[allow(dead_code)] // Available for the VDag-aware commit rule added next.
    fn linked_by_strong_or_weak(
        &self,
        leader: &Certificate,
        prev_leader: &Certificate,
        dag: &Dag,
    ) -> bool {
        let target = prev_leader.digest();
        if leader.digest() == target {
            return true;
        }

        let mut visited = HashSet::new();
        let mut pending: Vec<Digest> = leader
            .header
            .parents
            .iter()
            .chain(&leader.header.weak_edges)
            .cloned()
            .collect();

        while let Some(digest) = pending.pop() {
            if digest == target {
                return true;
            }
            if !visited.insert(digest.clone()) {
                continue;
            }

            let certificate = dag
                .values()
                .flat_map(|authorities| authorities.values())
                .find(|(candidate, _)| candidate == &digest)
                .map(|(_, certificate)| certificate);

            if let Some(certificate) = certificate {
                pending.extend(
                    certificate
                        .header
                        .parents
                        .iter()
                        .chain(&certificate.header.weak_edges)
                        .cloned(),
                );
            }
        }
        false
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in x.header.parents.iter().chain(&x.header.weak_edges) {
                let certificate = match state.dag_by_digest.get(parent) {
                    Some(certificate) => certificate,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(parent);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or(false, |r| certificate.round() <= *r);
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(parent.clone());
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}
