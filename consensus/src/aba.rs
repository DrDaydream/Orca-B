//! An asynchronous binary agreement (ABA) state machine.
//!
//! Networking and authentication stay outside this module: the caller
//! broadcasts every [`AbaAction::Broadcast`] and supplies the authenticated
//! sender to [`Aba::handle_message`]. The local broadcast is already counted
//! by the state machine and must not be looped back into it.

use config::{Committee, Stake};
use crypto::PublicKey;
use log::trace;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub type AbaInstance = u64;
pub type AbaRound = u64;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum BinaryValue {
    Zero,
    One,
}

impl BinaryValue {
    fn index(self) -> usize {
        match self {
            Self::Zero => 0,
            Self::One => 1,
        }
    }
}

impl From<bool> for BinaryValue {
    fn from(value: bool) -> Self {
        if value {
            Self::One
        } else {
            Self::Zero
        }
    }
}

impl From<BinaryValue> for bool {
    fn from(value: BinaryValue) -> Self {
        value == BinaryValue::One
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AbaMessageKind {
    BVal(BinaryValue),
    Aux(BinaryValue),
    /// A vote emitted only after the sender has locally decided. A receiver
    /// terminates after collecting f+1 authenticated votes for one value.
    Decide(BinaryValue),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AbaMessage {
    pub instance: AbaInstance,
    pub round: AbaRound,
    pub kind: AbaMessageKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AbaAction {
    Broadcast(AbaMessage),
    Decide(BinaryValue),
}

/// A common coin must return the same bit to every honest node for an
/// `(instance, round)` pair. Production deployments should implement this
/// trait with a threshold-signature coin.
pub trait CommonCoin: Clone + Send + Sync + 'static {
    fn coin(&self, instance: AbaInstance, round: AbaRound) -> BinaryValue;
}

/// Predictable shared coin intended for tests and integration plumbing only.
/// It preserves agreement but does not provide adversarial asynchronous
/// termination; replace it with a threshold coin in production.
#[derive(Clone, Debug)]
pub struct DeterministicCoin {
    seed: u64,
}

impl DeterministicCoin {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }
}

impl CommonCoin for DeterministicCoin {
    fn coin(&self, instance: AbaInstance, round: AbaRound) -> BinaryValue {
        let mut x = self.seed ^ instance.rotate_left(17) ^ round.rotate_left(41);
        x ^= x >> 30;
        x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x ^= x >> 27;
        BinaryValue::from((x & 1) == 1)
    }
}

#[derive(Default)]
struct RoundState {
    bval_senders: [HashSet<PublicKey>; 2],
    bval_sent: [bool; 2],
    bin_values: [bool; 2],
    aux_sent: bool,
    aux: HashMap<PublicKey, BinaryValue>,
    advanced: bool,
}

/// Bracha BV-broadcast plus AUX and a pluggable common coin.
pub struct Aba<C: CommonCoin> {
    committee: Committee,
    name: PublicKey,
    instance: AbaInstance,
    coin: C,
    round: AbaRound,
    estimate: Option<BinaryValue>,
    rounds: HashMap<AbaRound, RoundState>,
    decision: Option<BinaryValue>,
    decide_senders: [HashSet<PublicKey>; 2],
    decide_sent: bool,
    decision_certified: bool,
}

impl<C: CommonCoin> Aba<C> {
    pub fn new(committee: Committee, name: PublicKey, instance: AbaInstance, coin: C) -> Self {
        Self {
            committee,
            name,
            instance,
            coin,
            round: 0,
            estimate: None,
            rounds: HashMap::new(),
            decision: None,
            decide_senders: Default::default(),
            decide_sent: false,
            decision_certified: false,
        }
    }

    pub fn instance(&self) -> AbaInstance {
        self.instance
    }

    pub fn round(&self) -> AbaRound {
        self.round
    }

    pub fn decision(&self) -> Option<BinaryValue> {
        self.decision
    }

    pub fn decision_certified(&self) -> bool {
        self.decision_certified
    }

    /// Starts this ABA instance. Repeated calls are idempotent.
    pub fn propose(&mut self, value: BinaryValue) -> Vec<AbaAction> {
        if self.estimate.is_some() {
            return Vec::new();
        }
        self.estimate = Some(value);
        let mut actions = Vec::new();
        self.send_bval(0, value, &mut actions);
        actions
    }

    /// Handles a message whose sender has already been authenticated.
    pub fn handle_message(&mut self, sender: PublicKey, message: AbaMessage) -> Vec<AbaAction> {
        if message.instance != self.instance || self.committee.stake(&sender) == 0 {
            return Vec::new();
        }

        let mut actions = Vec::new();
        match message.kind {
            AbaMessageKind::BVal(value) => {
                self.accept_bval(sender, message.round, value, &mut actions)
            }
            AbaMessageKind::Aux(value) => {
                self.accept_aux(sender, message.round, value, &mut actions)
            }
            AbaMessageKind::Decide(value) => self.accept_decide(sender, value, &mut actions),
        }
        actions
    }

    fn send_decide(&mut self, value: BinaryValue, actions: &mut Vec<AbaAction>) {
        if self.decide_sent {
            return;
        }
        self.decide_sent = true;
        actions.push(AbaAction::Broadcast(AbaMessage {
            instance: self.instance,
            round: self.round,
            kind: AbaMessageKind::Decide(value),
        }));
        self.decide_senders[value.index()].insert(self.name);
    }

    fn accept_decide(
        &mut self,
        sender: PublicKey,
        value: BinaryValue,
        actions: &mut Vec<AbaAction>,
    ) {
        self.decide_senders[value.index()].insert(sender);
        let stake: Stake = self.decide_senders[value.index()]
            .iter()
            .map(|name| self.committee.stake(name))
            .sum();
        if stake < self.committee.validity_threshold() {
            return;
        }
        if self.decision.map_or(false, |decision| decision != value) {
            return;
        }

        // At least one of f+1 authenticated senders is honest, and honest
        // nodes vote DECIDE only after the ABA decision rule fires.
        self.send_decide(value, actions);
        if self.decision.is_none() {
            self.decision = Some(value);
            actions.push(AbaAction::Decide(value));
        }
        self.decision_certified = true;
    }

    fn send_bval(&mut self, round: AbaRound, value: BinaryValue, actions: &mut Vec<AbaAction>) {
        let state = self.rounds.entry(round).or_default();
        if state.bval_sent[value.index()] {
            return;
        }
        state.bval_sent[value.index()] = true;
        actions.push(AbaAction::Broadcast(AbaMessage {
            instance: self.instance,
            round,
            kind: AbaMessageKind::BVal(value),
        }));
        self.accept_bval(self.name, round, value, actions);
    }

    fn accept_bval(
        &mut self,
        sender: PublicKey,
        round: AbaRound,
        value: BinaryValue,
        actions: &mut Vec<AbaAction>,
    ) {
        let index = value.index();
        self.rounds.entry(round).or_default().bval_senders[index].insert(sender);

        let stake = self.bval_stake(round, value);
        if stake >= self.committee.validity_threshold() && !self.rounds[&round].bval_sent[index] {
            self.send_bval(round, value, actions);
        }

        if stake >= self.committee.quorum_threshold() && !self.rounds[&round].bin_values[index] {
            self.rounds.entry(round).or_default().bin_values[index] = true;
            self.send_aux(round, value, actions);
        }
        self.try_advance(round, actions);
    }

    fn send_aux(&mut self, round: AbaRound, value: BinaryValue, actions: &mut Vec<AbaAction>) {
        let state = self.rounds.entry(round).or_default();
        if state.aux_sent {
            return;
        }
        state.aux_sent = true;
        actions.push(AbaAction::Broadcast(AbaMessage {
            instance: self.instance,
            round,
            kind: AbaMessageKind::Aux(value),
        }));
        self.accept_aux(self.name, round, value, actions);
    }

    fn accept_aux(
        &mut self,
        sender: PublicKey,
        round: AbaRound,
        value: BinaryValue,
        actions: &mut Vec<AbaAction>,
    ) {
        // A Byzantine authority cannot gain weight by sending both values.
        self.rounds
            .entry(round)
            .or_default()
            .aux
            .entry(sender)
            .or_insert(value);
        self.try_advance(round, actions);
    }

    fn bval_stake(&self, round: AbaRound, value: BinaryValue) -> Stake {
        self.rounds
            .get(&round)
            .map(|state| {
                state.bval_senders[value.index()]
                    .iter()
                    .map(|name| self.committee.stake(name))
                    .sum()
            })
            .unwrap_or(0)
    }

    fn try_advance(&mut self, round: AbaRound, actions: &mut Vec<AbaAction>) {
        if round != self.round || self.rounds[&round].advanced {
            return;
        }

        let state = &self.rounds[&round];
        let accepted: Vec<_> = state
            .aux
            .iter()
            .filter(|(_, value)| state.bin_values[value.index()])
            .collect();
        let accepted_stake: Stake = accepted
            .iter()
            .map(|(name, _)| self.committee.stake(name))
            .sum();
        if accepted_stake < self.committee.quorum_threshold() {
            return;
        }

        let values: HashSet<_> = accepted.into_iter().map(|(_, value)| *value).collect();
        let coin = self.coin.coin(self.instance, round);
        let next_estimate = if values.len() == 1 {
            let value = *values.iter().next().unwrap();
            if value == coin && self.decision.is_none() {
                self.decision = Some(value);
                actions.push(AbaAction::Decide(value));
                self.send_decide(value, actions);
            }
            value
        } else {
            coin
        };

        trace!(
            "ABA {} advances from internal round {} with values {:?}, coin {:?}, decision {:?}",
            self.instance,
            round,
            values,
            coin,
            self.decision
        );

        self.rounds.entry(round).or_default().advanced = true;
        if self.decision_certified {
            return;
        }
        self.round += 1;
        self.estimate = Some(next_estimate);
        self.send_bval(self.round, next_estimate, actions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{Authority, PrimaryAddresses};
    use crypto::generate_keypair;
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::{BTreeMap, VecDeque};

    fn fixture() -> (Committee, Vec<PublicKey>) {
        let mut rng = StdRng::from_seed([7; 32]);
        let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng).0).collect();
        let authorities = keys
            .iter()
            .map(|key| {
                (
                    *key,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "127.0.0.1:0".parse().unwrap(),
                            aba_to_aba: "127.0.0.1:0".parse().unwrap(),
                            worker_to_primary: "127.0.0.1:0".parse().unwrap(),
                        },
                        workers: HashMap::new(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        (Committee { authorities }, keys)
    }

    fn run(inputs: &[BinaryValue]) -> Vec<BinaryValue> {
        let (committee, keys) = fixture();
        let mut nodes: Vec<_> = keys
            .iter()
            .map(|key| Aba::new(committee.clone(), *key, 11, DeterministicCoin::new(9)))
            .collect();
        let mut messages = VecDeque::new();
        for (index, input) in inputs.iter().enumerate() {
            for action in nodes[index].propose(*input) {
                if let AbaAction::Broadcast(message) = action {
                    messages.push_back((index, message));
                }
            }
        }

        for _ in 0..50_000 {
            if nodes.iter().all(|node| node.decision().is_some()) {
                return nodes.iter().map(|node| node.decision().unwrap()).collect();
            }
            let (sender, message) = messages.pop_front().expect("ABA stopped making progress");
            for receiver in 0..nodes.len() {
                if receiver == sender {
                    continue;
                }
                for action in nodes[receiver].handle_message(keys[sender], message.clone()) {
                    if let AbaAction::Broadcast(message) = action {
                        messages.push_back((receiver, message));
                    }
                }
            }
        }
        panic!("ABA did not terminate within the test bound")
    }

    #[test]
    fn unanimous_input_preserves_validity() {
        let decisions = run(&[BinaryValue::One; 4]);
        assert_eq!(decisions, vec![BinaryValue::One; 4]);
    }

    #[test]
    fn split_inputs_reach_agreement() {
        let decisions = run(&[
            BinaryValue::Zero,
            BinaryValue::Zero,
            BinaryValue::One,
            BinaryValue::One,
        ]);
        assert!(decisions.iter().all(|decision| *decision == decisions[0]));
    }

    #[test]
    fn ignores_unknown_sender_and_wrong_instance() {
        let (committee, keys) = fixture();
        let mut node = Aba::new(committee, keys[0], 3, DeterministicCoin::new(0));
        let message = AbaMessage {
            instance: 4,
            round: 0,
            kind: AbaMessageKind::BVal(BinaryValue::One),
        };
        assert!(node.handle_message(keys[1], message).is_empty());
        let outsider = PublicKey([99; 32]);
        let message = AbaMessage {
            instance: 3,
            round: 0,
            kind: AbaMessageKind::BVal(BinaryValue::One),
        };
        assert!(node.handle_message(outsider, message).is_empty());
    }

    #[test]
    fn decide_certificate_requires_f_plus_one_authenticated_votes() {
        let (committee, keys) = fixture();
        let mut node = Aba::new(committee, keys[0], 3, DeterministicCoin::new(0));
        let decide = AbaMessage {
            instance: 3,
            round: 0,
            kind: AbaMessageKind::Decide(BinaryValue::One),
        };

        assert!(node.handle_message(keys[1], decide.clone()).is_empty());
        assert_eq!(node.decision(), None);
        assert!(!node.decision_certified());

        let actions = node.handle_message(keys[2], decide);
        assert!(actions.contains(&AbaAction::Decide(BinaryValue::One)));
        assert_eq!(node.decision(), Some(BinaryValue::One));
        assert!(node.decision_certified());
    }
}
