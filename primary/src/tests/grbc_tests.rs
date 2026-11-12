use super::GrbcVotesAggregator;
use crate::common::{committee, header, keys, proposal, votes};
use crate::messages::{batch_digest, GrbcPhase, GrbcVoteBatch};
use crypto::{Hash as _, Signature};

#[test]
fn echo_and_ready_are_independent_and_duplicates_do_not_add_weight() {
    let h = header();
    let c = committee();
    let mut state = GrbcVotesAggregator::new();
    for v in votes(&h, GrbcPhase::Echo).into_iter().take(3) {
        assert!(state.append(&v, &c, &h).unwrap());
        assert!(!state.append(&v, &c, &h).unwrap());
    }
    assert_eq!(state.echo_weight, 3);
    assert_eq!(state.ready_weight, 0);
    for v in votes(&h, GrbcPhase::Ready).into_iter().take(3) {
        state.append(&v, &c, &h).unwrap();
    }
    assert_eq!(state.ready_weight, 3);
}

#[test]
fn signatures_bind_phase_sender_instance_and_header() {
    let h = header();
    let vote = votes(&h, GrbcPhase::Echo).remove(0);
    for field in 0..5 {
        let mut forged = vote.clone();
        match field {
            0 => forged.phase = GrbcPhase::Ready,
            1 => forged.round += 1,
            2 => forged.id = Default::default(),
            3 => forged.author = keys()[1].0,
            _ => forged.origin = keys()[1].0,
        }
        assert!(forged.verify(&committee()).is_err());
    }
    let mut other_header = h.clone();
    other_header.round += 1;
    assert!(GrbcVotesAggregator::new()
        .append(&vote, &committee(), &other_header)
        .is_err());
}

#[test]
fn proposal_requires_exact_payload_and_fixed_dag_metadata() {
    let mut p = proposal();
    let bytes = bincode::serialize(&(0u32, vec![vec![42u8; 32]])).unwrap();
    let digest = batch_digest(&bytes);
    p.header.payload.insert(digest.clone(), 0);
    p.batches.insert(digest.clone(), bytes);
    p.header.id = p.header.digest();
    p.header.signature = Signature::new(&p.header.id, &keys().pop().unwrap().1);
    p.verify(&committee()).unwrap();
    let mut missing = p.clone();
    missing.batches.clear();
    assert!(missing.verify(&committee()).is_err());
    let mut changed = p.clone();
    changed.batches.get_mut(&digest).unwrap()[0] ^= 1;
    assert!(changed.verify(&committee()).is_err());
    p.header.parents.clear();
    assert!(p.verify(&committee()).is_err());
}

#[test]
fn wire_vote_batch_cannot_mix_senders_or_phases() {
    let h = header();
    let echoes = votes(&h, GrbcPhase::Echo);
    assert!(GrbcVoteBatch::from_votes(echoes.clone()).is_none());
    assert!(GrbcVoteBatch::from_votes(vec![
        echoes[0].clone(),
        votes(&h, GrbcPhase::Ready)[0].clone()
    ])
    .is_none());
    let batch = GrbcVoteBatch::from_votes(vec![echoes[0].clone()]).unwrap();
    let restored = batch.into_votes();
    restored[0].verify(&committee()).unwrap();
}
