// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, ConsensusCommand, ConsensusMessage, Round};
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
struct RuleOneSupport {
    processed_observed: HashSet<Digest>,
    processed_dag: HashSet<Digest>,
    observed: HashSet<PublicKey>,
    dag: HashSet<PublicKey>,
}

#[derive(Default)]
struct RuleTwoSupport {
    processed_observed: HashSet<Digest>,
    processed_dag: HashSet<Digest>,
    observed_strong: HashSet<PublicKey>,
    dag_strong: HashSet<PublicKey>,
    dag_strong_or_virtual: HashSet<PublicKey>,
}

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
    /// Blocks locally delivered by GRBC with grade 1.
    vdag: VDag,
    /// Blocks for which a valid grade-2 proof has been delivered.
    grade_two: HashSet<Digest>,
    /// Digest index of blocks already present in the formal Dag.
    dag_digests: HashSet<Digest>,
    /// Direct digest lookup over Dag union VDag. This avoids scanning every
    /// round for each step of a reachability query.
    observed: HashMap<Digest, Certificate>,
    /// Memoized strong-path answers keyed by (source, target). Positive
    /// answers remain valid because certificate edges are immutable. Negative
    /// answers are discarded whenever a newly observed block may fill a gap.
    strong_path_cache: HashMap<(Digest, Digest), bool>,
    /// Number of strong/weak dependencies not yet known to have entered Dag.
    missing_dependencies: HashMap<Digest, usize>,
    /// Reverse dependency index used to wake only blocks affected by a newly
    /// promoted Dag certificate.
    dependency_waiters: HashMap<Digest, HashSet<Digest>>,
    promotion_queue: VecDeque<Digest>,
    rule_one_support: HashMap<(Round, Digest), RuleOneSupport>,
    rule_two_support: HashMap<(Round, Digest), RuleTwoSupport>,
    aba_support: HashMap<(Round, Digest), AbaSupport>,
    /// The authority designated as leader for every round.
    leaders: HashMap<Round, PublicKey>,
    /// Leader rounds already committed, preventing duplicate commits.
    committed_leaders: HashSet<Round>,
    /// Leader rounds explicitly skipped by commit rule 3.
    skipped_leaders: HashSet<Round>,
    /// Commit-ready leaders waiting for the previous round's leader.
    pending_leaders: BTreeMap<Round, Certificate>,
    aba_instances: HashMap<Round, Aba<DeterministicCoin>>,
    aba_inputs: HashSet<Round>,
    aba_decisions: HashMap<Round, BinaryValue>,
    buffered_aba: HashMap<Round, Vec<(PublicKey, AbaMessage)>>,
    missing_leader_requests: HashSet<Round>,
    /// Leaders made commit-ready directly by rules 1 or 2. ABA can help
    /// propagate input 1 but can never override this local fast-path result.
    direct_commit_ready: HashSet<Round>,
    /// Highest leader round whose r+3 deadline has been processed. This keeps
    /// zero-input handling incremental instead of rescanning round 1 onward.
    zero_input_checked_through: Round,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        let genesis_dag: Dag = [(0, genesis)].iter().cloned().collect();

        let dag_digests = genesis_dag
            .values()
            .flat_map(|authorities| authorities.values())
            .map(|(digest, _)| digest.clone())
            .collect();
        let observed = genesis_dag
            .values()
            .flat_map(|authorities| authorities.values())
            .map(|(digest, certificate)| (digest.clone(), certificate.clone()))
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
            // Genesis blocks already belong to the ordering DAG, so they must
            // not also appear in VDag.
            vdag: HashMap::new(),
            grade_two: HashSet::new(),
            dag_digests,
            observed,
            strong_path_cache: HashMap::new(),
            missing_dependencies: HashMap::new(),
            dependency_waiters: HashMap::new(),
            promotion_queue: VecDeque::new(),
            rule_one_support: HashMap::new(),
            rule_two_support: HashMap::new(),
            aba_support: HashMap::new(),
            leaders: HashMap::new(),
            committed_leaders: [0].iter().cloned().collect(),
            skipped_leaders: HashSet::new(),
            pending_leaders: BTreeMap::new(),
            aba_instances: HashMap::new(),
            aba_inputs: HashSet::new(),
            aba_decisions: HashMap::new(),
            buffered_aba: HashMap::new(),
            missing_leader_requests: HashSet::new(),
            direct_commit_ready: HashSet::new(),
            zero_input_checked_through: 0,
        }
    }

    /// Insert a block delivered by GRBC at grade 1 into the validated DAG.
    fn insert_grade_one(&mut self, certificate: Certificate) {
        let round = certificate.round();
        let origin = certificate.origin();
        let digest = certificate.digest();
        self.observed.insert(digest.clone(), certificate.clone());
        self.strong_path_cache.retain(|_, reachable| *reachable);
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
        self.missing_dependencies.insert(digest.clone(), missing.len());
        if missing.is_empty() && self.grade_two.contains(&digest) {
            self.promotion_queue.push_back(digest.clone());
        }
        self.vdag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(origin, (digest, certificate));
    }

    /// Promote a grade-1 block into Tusk's ordering DAG. A block contained in
    /// Dag must never remain in VDag.
    fn promote_to_dag(&mut self, certificate: Certificate) {
        let round = certificate.round();
        let origin = certificate.origin();
        let digest = certificate.digest();

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
            .insert(origin, (digest.clone(), certificate));
        self.dag_digests.insert(digest);
    }

    fn mark_grade_two(&mut self, digest: Digest) {
        if self.grade_two.insert(digest.clone())
            && self.missing_dependencies.get(&digest) == Some(&0)
            && self.observed.contains_key(&digest)
        {
            self.promotion_queue.push_back(digest);
        }
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
        }
        promoted
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
            self.vdag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }

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
        self.observed.retain(|digest, _| retained.contains(digest));
        self.grade_two.retain(|digest| retained.contains(digest));
        self.strong_path_cache.retain(|(source, target), reachable| {
            *reachable && retained.contains(source) && retained.contains(target)
        });
        self.missing_dependencies
            .retain(|digest, _| retained.contains(digest));
        self.dependency_waiters.retain(|_, waiters| {
            waiters.retain(|digest| retained.contains(digest));
            !waiters.is_empty()
        });
        self.promotion_queue
            .retain(|digest| retained.contains(digest));
        self.rule_one_support
            .retain(|(round, _), _| *round >= gc_round);
        self.rule_two_support
            .retain(|(round, _), _| *round >= gc_round);
        self.aba_support
            .retain(|(round, _), _| *round >= gc_round);
        // `dag_digests` is intentionally monotonic: membership proves that a
        // dependency entered Dag before its full certificate was garbage
        // collected, and later promotions still rely on that proof.
        self.aba_instances.retain(|round, _| *round >= gc_round);
        self.aba_inputs.retain(|round| *round >= gc_round);
        self.aba_decisions.retain(|round, _| *round >= gc_round);
        self.buffered_aba.retain(|round, _| *round >= gc_round);
        self.missing_leader_requests.retain(|round| *round >= gc_round);
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
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
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
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        // Listen to incoming certificates.
        while let Some(message) = self.rx_primary.recv().await {
            let (observed_round, promoted) = match message {
                ConsensusMessage::GradeOne(certificate) => {
                    debug!("Grade 1 delivered {:?}", certificate);
                    let round = certificate.round();
                    state.insert_grade_one(certificate);
                    (round, state.promote_ready())
                }
                ConsensusMessage::GradeTwo(certificate) => {
                    debug!("Grade 2 delivered {:?}", certificate);
                    let round = certificate.round();
                    state.mark_grade_two(certificate.digest());
                    (round, state.promote_ready())
                }
                ConsensusMessage::Aba(sender, bytes) => {
                    match bincode::deserialize::<AbaMessage>(&bytes) {
                        Ok(message) => self.process_aba_message(sender, message, &mut state).await,
                        Err(error) => warn!("Ignoring malformed ABA message: {}", error),
                    }
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
                    continue;
                }
            };

            // Designation happens as soon as the round is observed, even if
            // none of its grade-1 blocks is ready to enter Dag yet.
            let designated = self.leader_authority(observed_round);
            if state.leaders.insert(observed_round, designated).is_none() {
                debug!("Round {} designated leader {}", observed_round, designated);
            }
            self.ensure_aba_instance(observed_round, &mut state).await;

            // Incremental hot path: a block in support round r can affect only
            // the leaders at r-1 (rule 1) and r-2 (rule 2 / ABA input). Also
            // inspect rounds of blocks newly promoted from VDag, since a late
            // promotion can cross a Dag-only threshold.
            let mut affected_rounds: HashSet<_> = promoted
                .iter()
                .map(Certificate::round)
                .collect();
            affected_rounds.insert(observed_round);
            for support_round in affected_rounds {
                self.evaluate_commit_rule_one(support_round, &mut state, true)
                    .await;
                self.evaluate_commit_rule_two(support_round, &mut state, true)
                    .await;
                if support_round >= 3 {
                    self.evaluate_aba_r_plus_two_input(support_round - 2, &mut state)
                        .await;
                }
            }

            let entered_round = state
                .dag
                .iter()
                .filter(|(_, blocks)| {
                    blocks
                        .keys()
                        .map(|name| self.committee.stake(name))
                        .sum::<Stake>()
                        >= self.committee.quorum_threshold()
                })
                .map(|(round, _)| round + 1)
                .max()
                .unwrap_or(1);
            self.input_zero_at_round_end(entered_round, &mut state)
                .await;
            self.resolve_requested_leaders(&mut state).await;
        }
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
        if instance + self.gc_depth < state.last_committed_round {
            return;
        }
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
        let mut broadcasts = Vec::new();
        for action in actions {
            match action {
                AbaAction::Broadcast(message) => {
                    let bytes =
                        bincode::serialize(&message).expect("Failed to serialize ABA message");
                    broadcasts.push(bytes);
                }
                AbaAction::Decide(value) => {
                    // ABA is the cold-path trigger for an older leader. Before
                    // applying its output, perform one final targeted scan of
                    // that leader's rule-1 and rule-2 support rounds. A direct
                    // rule result retains precedence over ABA 0.
                    Box::pin(self.evaluate_commit_rule_one(
                        leader_round + 1,
                        state,
                        false,
                    ))
                    .await;
                    Box::pin(self.evaluate_commit_rule_two(
                        leader_round + 2,
                        state,
                        false,
                    ))
                    .await;
                    if state.aba_decisions.insert(leader_round, value).is_none() {
                        self.apply_aba_decision(leader_round, value, state).await;
                    }
                }
            }
        }
        if !broadcasts.is_empty() {
            self.tx_primary
                .send(ConsensusCommand::AbaBroadcast(broadcasts))
                .await
                .expect("Failed to send ABA batch to primary");
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
                state.skipped_leaders.insert(leader_round);
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
                    self.queue_leader_commit(leader, state).await;
                } else if state.missing_leader_requests.insert(leader_round) {
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
                self.queue_leader_commit(leader, state).await;
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
        for (block_digest, block) in blocks {
            if !support.processed_grade_two.insert(block_digest) {
                continue;
            }
            let strong = self.has_strong_path(&block, &digest, state);
            if strong {
                support.strong.insert(block.origin());
            }
            if strong || self.has_two_hop_virtual_path(&block, &digest, state) {
                support.strong_or_virtual.insert(block.origin());
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
        state: &mut State,
    ) -> (Stake, Stake) {
        let key = (round, leader_digest.clone());
        let mut support = state.rule_one_support.remove(&key).unwrap_or_default();
        let dag_blocks: Vec<_> = state
            .dag
            .get(&round)
            .into_iter()
            .flat_map(|authorities| authorities.values())
            .map(|(digest, certificate)| (digest.clone(), certificate.clone()))
            .collect();
        for (digest, certificate) in dag_blocks {
            if support.processed_dag.insert(digest.clone())
                && certificate.header.parents.contains(leader_digest)
            {
                support.dag.insert(certificate.origin());
            }
            support.processed_observed.insert(digest);
        }
        let vdag_blocks: Vec<_> = state
            .vdag
            .get(&round)
            .into_iter()
            .flat_map(|authorities| authorities.values())
            .map(|(digest, certificate)| (digest.clone(), certificate.clone()))
            .collect();
        for (digest, certificate) in vdag_blocks {
            if support.processed_observed.insert(digest)
                && certificate.header.parents.contains(leader_digest)
            {
                support.observed.insert(certificate.origin());
            }
        }
        support.observed.extend(support.dag.iter().cloned());
        let observed_stake = support
            .observed
            .iter()
            .map(|authority| self.committee.stake(authority))
            .sum();
        let dag_stake = support
            .dag
            .iter()
            .map(|authority| self.committee.stake(authority))
            .sum();
        state.rule_one_support.insert(key, support);
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
        if state.committed_leaders.contains(&leader_round)
            || state.pending_leaders.contains_key(&leader_round)
        {
            return;
        }
        let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
            Some((digest, leader)) => (digest.clone(), leader.clone()),
            None => return,
        };

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
        if propagate_aba_input {
            self.offer_aba_input(leader.round(), BinaryValue::One, state)
                .await;
        }
        self.queue_leader_commit(leader, state).await;
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
        if state.committed_leaders.contains(&leader_round)
            || state.pending_leaders.contains_key(&leader_round)
        {
            return;
        }

        let leader_authority = self.ordering_leader_authority(leader_round);
        let leader = state
            .dag
            .get(&leader_round)
            .and_then(|round| round.get(&leader_authority))
            .or_else(|| {
                state
                    .vdag
                    .get(&leader_round)
                    .and_then(|round| round.get(&leader_authority))
            })
            .map(|(_, certificate)| certificate.clone());
        let leader = match leader {
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
        if propagate_aba_input {
            self.offer_aba_input(leader.round(), BinaryValue::One, state)
                .await;
        }
        self.queue_leader_commit(leader, state).await;
    }

    /// Counts rule-2 support in round `q`, with each authority counted once.
    fn rule_two_support_stake(
        &self,
        q: Round,
        leader_digest: &Digest,
        state: &mut State,
    ) -> (Stake, Stake, Stake) {
        let key = (q, leader_digest.clone());
        let mut support = state.rule_two_support.remove(&key).unwrap_or_default();
        let dag_blocks: Vec<_> = state
            .dag
            .get(&q)
            .into_iter()
            .flat_map(|round| round.values())
            .map(|(digest, block)| (digest.clone(), block.clone()))
            .collect();
        for (digest, block) in &dag_blocks {
            if !support.processed_dag.insert(digest.clone()) {
                continue;
            }
            let strong = self.has_strong_path(block, leader_digest, state);
            if strong {
                support.dag_strong.insert(block.origin());
            }
            if strong || self.has_two_hop_virtual_path(block, leader_digest, state) {
                support.dag_strong_or_virtual.insert(block.origin());
            }
            support.processed_observed.insert(digest.clone());
        }

        let vdag_blocks: Vec<_> = state
            .vdag
            .get(&q)
            .into_iter()
            .flat_map(|round| round.values())
            .map(|(digest, block)| (digest.clone(), block.clone()))
            .collect();
        for (digest, block) in &vdag_blocks {
            if !support.processed_observed.insert(digest.clone()) {
                continue;
            }
            if self.has_strong_path(block, leader_digest, state) {
                support.observed_strong.insert(block.origin());
            }
        }
        support
            .observed_strong
            .extend(support.dag_strong.iter().cloned());

        let stake = |authorities: &HashSet<PublicKey>| {
            authorities
                .iter()
                .map(|authority| self.committee.stake(authority))
                .sum()
        };
        let result = (
            stake(&support.observed_strong),
            stake(&support.dag_strong),
            stake(&support.dag_strong_or_virtual),
        );
        state.rule_two_support.insert(key, support);
        result
    }

    /// Strong-path reachability over blocks observed in Dag union VDag.
    fn has_strong_path(&self, block: &Certificate, target: &Digest, state: &mut State) -> bool {
        let source = block.digest();
        let key = (source, target.clone());
        if let Some(reachable) = state.strong_path_cache.get(&key) {
            return *reachable;
        }
        let mut pending: Vec<_> = block.header.parents.iter().cloned().collect();
        let mut visited = HashSet::new();
        while let Some(digest) = pending.pop() {
            if &digest == target {
                state.strong_path_cache.insert(key, true);
                return true;
            }
            if !visited.insert(digest.clone()) {
                continue;
            }
            if let Some(parent) = Self::observed_certificate(&digest, state) {
                pending.extend(parent.header.parents.iter().cloned());
            }
        }
        state.strong_path_cache.insert(key, false);
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
                        state.pending_leaders.insert(target_round, target);
                    } else {
                        debug!(
                            "Skipping leader round {} by commit rule 3 observed from round {}",
                            target_round, observer_round
                        );
                        state.skipped_leaders.insert(target_round);
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
    async fn queue_leader_commit(&mut self, leader: Certificate, state: &mut State) {
        let round = leader.round();
        if state.committed_leaders.contains(&round) {
            return;
        }
        state.pending_leaders.entry(round).or_insert(leader);

        self.drain_ready_leaders(state).await;
    }

    async fn drain_ready_leaders(&mut self, state: &mut State) {
        loop {
            let ready_round = state.pending_leaders.keys().cloned().find(|round| {
                (state.committed_leaders.contains(&(round - 1))
                    || state.skipped_leaders.contains(&(round - 1)))
                    && state
                        .pending_leaders
                        .get(round)
                        .map_or(false, |leader| state.dag_digests.contains(&leader.digest()))
            });
            let ready_round = match ready_round {
                Some(round) => round,
                None => break,
            };
            let leader = state.pending_leaders.remove(&ready_round).unwrap();
            if !state.committed_leaders.insert(ready_round) {
                continue;
            }

            let sequence = self.order_dag(&leader, state);
            for certificate in sequence {
                state.update(&certificate, self.gc_depth);

                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);
                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_primary
                    .send(ConsensusCommand::Cleanup(certificate.clone()))
                    .await
                    .expect("Failed to send certificate to primary");
                if let Err(error) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", error);
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
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
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
