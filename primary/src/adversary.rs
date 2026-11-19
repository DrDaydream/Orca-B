use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use ed25519_dalek::{Digest as _, Sha512};
use network::{DelayBudget, Delivery, DeliveryPolicy, ReliableSender, SimpleSender};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(test)]
#[path = "tests/adversary_tests.rs"]
mod scheduling_tests;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Mode {
    None,
    Silent,
    Targeted,
    Network,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Configuration {
    mode: Mode,
    byzantine_authorities: Vec<String>,
    seed: u64,
    start_round: u64,
    rounds: u64,
    delay_ms: u64,
    max_pending_messages: usize,
    max_pending_bytes: usize,
    // Shared launcher field. Orca designates a leader in every round.
    bullshark_alternating: bool,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            mode: Mode::None,
            byzantine_authorities: Vec::new(),
            seed: 1,
            start_round: 1,
            rounds: 0,
            delay_ms: 200,
            max_pending_messages: 4096,
            max_pending_bytes: 67_108_864,
            bullshark_alternating: true,
        }
    }
}

#[derive(Clone)]
struct Policy {
    config: Configuration,
    name: PublicKey,
    leaders: Vec<PublicKey>,
    byzantine: BTreeSet<PublicKey>,
    peers: BTreeMap<SocketAddr, PublicKey>,
    aba_peers: BTreeMap<SocketAddr, PublicKey>,
}

impl Policy {
    fn parse(json: &str, name: PublicKey, committee: &Committee) -> Result<Self, String> {
        let config: Configuration = serde_json::from_str(json).map_err(|e| e.to_string())?;
        if config.start_round == 0 || config.start_round.checked_add(config.rounds).is_none() {
            return Err(
                "start_round must be positive and the round window must not overflow".into(),
            );
        }
        if config.delay_ms == 0 || config.delay_ms > 86_400_000 {
            return Err("delay_ms must be in 1..=86400000".into());
        }
        if config.max_pending_messages == 0 || config.max_pending_bytes == 0 {
            return Err("pending message and byte limits must be positive".into());
        }
        if !committee.authorities.contains_key(&name) {
            return Err("local authority is not in committee".into());
        }
        // Match canonical public encodings instead of the legacy decoder,
        // which assumes 32 decoded bytes and can panic on a short key.
        let keys: BTreeMap<_, _> = committee
            .authorities
            .keys()
            .map(|key| (key.encode_base64(), *key))
            .collect();
        let mut byzantine = BTreeSet::new();
        for encoded in &config.byzantine_authorities {
            let key = keys
                .get(encoded)
                .ok_or("Byzantine identity must be a full base64 committee public key")?;
            if !byzantine.insert(*key) {
                return Err("duplicate Byzantine identity".into());
            }
        }
        if byzantine.len() > committee.size().saturating_sub(1) / 3 {
            return Err("fixed Byzantine set exceeds floor((n-1)/3)".into());
        }
        if config.mode == Mode::None && !byzantine.is_empty() {
            return Err("none mode requires an empty Byzantine set".into());
        }
        if matches!(config.mode, Mode::Silent | Mode::Targeted) && byzantine.is_empty() {
            return Err("silent and targeted modes require a fixed Byzantine set".into());
        }
        Ok(Self {
            config,
            name,
            leaders: committee.authorities.keys().copied().collect(),
            byzantine,
            peers: committee
                .authorities
                .iter()
                .map(|(key, authority)| (authority.primary.primary_to_primary, *key))
                .collect(),
            aba_peers: committee
                .authorities
                .iter()
                .map(|(key, authority)| (authority.primary.aba_to_aba, *key))
                .collect(),
        })
    }

    fn active(&self, round: u64) -> bool {
        round >= self.config.start_round
            && (self.config.rounds == 0 || round - self.config.start_round < self.config.rounds)
    }

    fn leader(&self, round: u64) -> PublicKey {
        self.leaders[(round % self.leaders.len() as u64) as usize]
    }

    fn silent(&self, name: PublicKey, round: u64) -> bool {
        self.config.mode == Mode::Silent && self.byzantine.contains(&name) && self.active(round)
    }

    fn schedules_local_sender(&self) -> bool {
        self.config.mode == Mode::Network
            || (matches!(self.config.mode, Mode::Silent | Mode::Targeted)
                && self.byzantine.contains(&self.name))
    }

    fn should_delay(&self, round: u64, origins: &[PublicKey], recipient: PublicKey) -> bool {
        if !self.active(round) || !origins.contains(&self.leader(round)) {
            return false;
        }
        match self.config.mode {
            Mode::Targeted if !self.byzantine.contains(&self.name) => return false,
            Mode::Targeted | Mode::Network => {}
            _ => return false,
        }
        // Rank a fixed set of honest recipients for this known leader round.
        // Selecting half by rank guarantees a nonempty, reproducible group.
        let mut recipients: Vec<_> = self
            .leaders
            .iter()
            .filter(|peer| !self.byzantine.contains(peer))
            .copied()
            .collect();
        recipients.sort_by_key(|peer| (score(peer, round, self.config.seed, 3), *peer));
        recipients
            .iter()
            .take((recipients.len() + 1) / 2)
            .any(|peer| *peer == recipient)
    }

    fn classify(
        &self,
        round: u64,
        origins: &[PublicKey],
        recipient: PublicKey,
        kind: &str,
    ) -> Delivery {
        let trace = || {
            format!(
                "mode={:?} round={} target={} sender={} recipient={} kind={}",
                self.config.mode,
                round,
                self.leader(round).encode_base64(),
                self.name.encode_base64(),
                recipient.encode_base64(),
                kind
            )
        };
        if self.silent(self.name, round) {
            return Delivery::Drop(trace());
        }
        if self.should_delay(round, origins, recipient) {
            Delivery::Delay(Duration::from_millis(self.config.delay_ms), trace())
        } else {
            Delivery::Send
        }
    }
}

impl DeliveryPolicy for Policy {
    fn delivery(&self, address: SocketAddr, data: &Bytes) -> Delivery {
        if !self.schedules_local_sender() {
            return Delivery::Send;
        }
        if let Some(recipient) = self.aba_peers.get(&address) {
            // ABA scheduling is reserved for A5. A3 only controls GRBC traffic;
            // A1 process failures are applied by the benchmark launcher.
            if self.config.mode != Mode::Network {
                return Delivery::Send;
            }
            let envelope: crate::messages::ConsensusNetworkMessage =
                bincode::deserialize(data).expect("Locally serialized ABA envelope");
            let batches: Vec<Vec<u8>> =
                bincode::deserialize(&envelope.payload).expect("Locally serialized ABA batches");
            let messages: Vec<crate::messages::AbaMessage> =
                bincode::deserialize(&batches[0]).expect("Locally serialized ABA messages");
            let round = messages[0].instance;
            return self.classify(round, &[self.leader(round)], *recipient, "aba");
        }
        let recipient = match self.peers.get(&address) {
            Some(peer) => *peer,
            None => return Delivery::Send,
        };
        // All bytes are locally serialized. Read only a proposal's Header;
        // never allocate/copy its large payload just to schedule a packet.
        if data.get(..4) == Some(&[0, 0, 0, 0]) {
            let header: crate::messages::Header = match bincode::deserialize_from(&data[4..]) {
                Ok(header) => header,
                Err(_) => return Delivery::Send,
            };
            return self.classify(
                header.round,
                &[header.author],
                recipient,
                "proposal-or-repair",
            );
        }
        match bincode::deserialize::<crate::primary::PrimaryMessage>(data) {
            Ok(crate::primary::PrimaryMessage::GrbcVote(vote)) => {
                self.classify(vote.round, &[vote.origin], recipient, "vote")
            }
            Ok(crate::primary::PrimaryMessage::GrbcVoteBatch(batch)) => self.classify(
                batch.round,
                &batch
                    .votes
                    .iter()
                    .map(|(_, origin, _)| *origin)
                    .collect::<Vec<_>>(),
                recipient,
                "votes-or-repair",
            ),
            _ => Delivery::Send,
        }
    }
}

#[derive(Clone)]
struct Runtime {
    policy: Arc<Policy>,
    budget: DelayBudget,
}
static RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);

/// Validate before starting a primary's network listeners or consensus tasks.
pub fn initialize(name: PublicKey, committee: &Committee) -> Result<(), String> {
    let json = match std::env::var("BENCH_ADVERSARY_CONFIG") {
        Ok(json) => json,
        Err(std::env::VarError::NotPresent) => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    let policy = Policy::parse(&json, name, committee)?;
    let budget = DelayBudget::new(
        policy.config.max_pending_messages,
        policy.config.max_pending_bytes,
    );
    log::info!("BENCH_ADVERSARY mode={:?} action=configure sender={} fixed_byzantine={} seed={} start_round={} rounds={} delay_ms={} max_pending_messages={} max_pending_bytes={} leader_schedule=every-round bullshark_alternating={}", policy.config.mode, name.encode_base64(), policy.byzantine.len(), policy.config.seed, policy.config.start_round, policy.config.rounds, policy.config.delay_ms, policy.config.max_pending_messages, policy.config.max_pending_bytes, policy.config.bullshark_alternating);
    *RUNTIME.lock().unwrap() = Some(Runtime {
        policy: Arc::new(policy),
        budget,
    });
    Ok(())
}

pub(crate) fn schedule_aba() -> bool {
    RUNTIME
        .lock()
        .unwrap()
        .as_ref()
        .map_or(false, |runtime| runtime.policy.config.mode == Mode::Network)
}

pub(crate) fn fixed_silence(name: PublicKey, round: u64) -> Option<bool> {
    RUNTIME
        .lock()
        .unwrap()
        .as_ref()
        .map(|runtime| runtime.policy.silent(name, round))
}

pub(crate) fn reliable_sender() -> ReliableSender {
    match RUNTIME.lock().unwrap().clone() {
        Some(runtime) if runtime.policy.schedules_local_sender() => {
            ReliableSender::with_schedule(runtime.policy, runtime.budget)
        }
        _ => ReliableSender::new(),
    }
}

#[cfg(feature = "benchmark")]
pub(crate) fn reliable_sender_with_counter(
    sent_bytes: Arc<std::sync::atomic::AtomicU64>,
) -> ReliableSender {
    match RUNTIME.lock().unwrap().clone() {
        Some(runtime) if runtime.policy.schedules_local_sender() => {
            ReliableSender::with_schedule_and_counter(runtime.policy, runtime.budget, sent_bytes)
        }
        _ => ReliableSender::with_sent_byte_counter(sent_bytes),
    }
}

pub(crate) fn simple_sender() -> SimpleSender {
    match RUNTIME.lock().unwrap().clone() {
        Some(runtime) if runtime.policy.schedules_local_sender() => {
            SimpleSender::with_schedule(runtime.policy, runtime.budget)
        }
        _ => SimpleSender::new(),
    }
}

pub(crate) fn group_aba_messages(batches: Vec<Vec<u8>>) -> Vec<Vec<Vec<u8>>> {
    let mut instances = BTreeMap::<u64, Vec<crate::messages::AbaMessage>>::new();
    for batch in batches {
        let messages: Vec<crate::messages::AbaMessage> =
            bincode::deserialize(&batch).expect("Locally serialized ABA messages");
        for message in messages {
            instances.entry(message.instance).or_default().push(message);
        }
    }
    instances
        .into_values()
        .map(|messages| vec![bincode::serialize(&messages).expect("Serialize ABA instance batch")])
        .collect()
}

fn score(authority: &PublicKey, round: u64, seed: u64, domain: u8) -> [u8; 32] {
    let mut hasher = Sha512::new();
    hasher.update(b"narwhal-dynamic-adversary-v1");
    hasher.update(seed.to_le_bytes());
    hasher.update(round.to_le_bytes());
    hasher.update([domain]);
    hasher.update(authority.as_ref());
    let output = hasher.finalize();
    output[..32].try_into().unwrap()
}

/// Test whether an authority belongs to this round's deterministic random
/// adversary set. A forced authority occupies one of the configured slots.
pub fn selected(
    authority: &PublicKey,
    authorities: &[PublicKey],
    round: u64,
    faults: usize,
    seed: u64,
    forced: Option<PublicKey>,
) -> bool {
    if std::env::var_os("BENCH_ADVERSARY_CONFIG").is_some() {
        return false;
    }
    let faults = faults.min(authorities.len());
    if faults == 0 {
        return false;
    }
    if forced == Some(*authority) {
        return true;
    }

    let forced_is_member = forced.map_or(false, |value| authorities.contains(&value));
    let random_slots = faults.saturating_sub(usize::from(forced_is_member));
    if random_slots == 0 {
        return false;
    }

    let own_key = (score(authority, round, seed, 0), *authority);
    let rank = authorities
        .iter()
        .filter(|candidate| forced != Some(**candidate))
        .filter(|candidate| (score(candidate, round, seed, 0), **candidate) < own_key)
        .count();
    rank < random_slots
}

/// A reproducible per-authority coin used by Orca's Rule-3 mixed mode.
pub(crate) fn mixed_silence(authority: &PublicKey, round: u64, seed: u64) -> bool {
    score(authority, round, seed, 1)[0] & 1 == 0
}

/// Deterministically defer about half of the non-adversarial leaders from the
/// Rule-1 fast path to Rule 2 during adversarial benchmarks.
pub fn defer_to_rule_two(authority: &PublicKey, round: u64, seed: u64) -> bool {
    if std::env::var_os("BENCH_ADVERSARY_CONFIG").is_some() {
        return false;
    }
    score(authority, round, seed, 2)[0] & 1 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authorities(count: u8) -> Vec<PublicKey> {
        (0..count).map(|value| PublicKey([value; 32])).collect()
    }

    #[test]
    fn selects_exactly_the_configured_number() {
        let authorities = authorities(10);
        for round in 1..20 {
            let count = authorities
                .iter()
                .filter(|authority| selected(authority, &authorities, round, 3, 7, None))
                .count();
            assert_eq!(count, 3);
        }
    }

    #[test]
    fn forced_authority_is_always_selected() {
        let authorities = authorities(10);
        let forced = authorities[7];
        for round in 1..20 {
            assert!(selected(&forced, &authorities, round, 1, 11, Some(forced)));
        }
    }

    #[test]
    fn schedule_is_deterministic_and_changes_across_rounds() {
        let authorities = authorities(10);
        let schedule = |round| {
            authorities
                .iter()
                .filter(|authority| selected(authority, &authorities, round, 3, 19, None))
                .copied()
                .collect::<Vec<_>>()
        };
        assert_eq!(schedule(4), schedule(4));
        assert_ne!(schedule(4), schedule(5));
    }

    #[test]
    fn rule_one_and_rule_two_slots_are_approximately_balanced() {
        let authority = authorities(1)[0];
        let rule_two = (1..=1_000)
            .filter(|round| defer_to_rule_two(&authority, *round, 23))
            .count();
        assert!((450..=550).contains(&rule_two));
        assert_eq!(
            defer_to_rule_two(&authority, 7, 23),
            defer_to_rule_two(&authority, 7, 23)
        );
    }
}
