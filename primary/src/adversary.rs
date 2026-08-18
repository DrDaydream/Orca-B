use crypto::PublicKey;
use ed25519_dalek::{Digest as _, Sha512};
use std::convert::TryInto as _;

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
