// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SecretKey};
use primary::Header;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::{BTreeSet, HashSet, VecDeque};
use tokio::sync::mpsc::{channel, Sender};

async fn deliver(tx: &Sender<ConsensusMessage>, certificate: Certificate) {
    let entered_round = certificate.round() + 1;
    tx.send(ConsensusMessage::GradeOne(certificate.clone()))
        .await
        .unwrap();
    tx.send(ConsensusMessage::GradeTwo(certificate))
        .await
        .unwrap();
    tx.send(ConsensusMessage::RoundAdvanced(entered_round))
        .await
        .unwrap();
}

fn spawn_aba_network(
    mut rx: tokio::sync::mpsc::Receiver<ConsensusCommand>,
    tx: Sender<ConsensusMessage>,
    authorities: Vec<PublicKey>,
) {
    tokio::spawn(async move {
        while let Some(command) = rx.recv().await {
            if let ConsensusCommand::AbaBroadcast(batch) = command {
                for bytes in batch {
                    for authority in &authorities {
                        tx.send(ConsensusMessage::Aba(*authority, bytes.clone()))
                            .await
                            .unwrap();
                    }
                }
            }
        }
    });
}

// Fixture
fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn mock_committee() -> Committee {
    Committee {
        authorities: keys()
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "0.0.0.0:0".parse().unwrap(),
                            aba_to_aba: "0.0.0.0:0".parse().unwrap(),
                            worker_to_primary: "0.0.0.0:0".parse().unwrap(),
                        },
                        workers: HashMap::default(),
                    },
                )
            })
            .collect(),
    }
}

// Fixture
fn mock_certificate(
    origin: PublicKey,
    round: Round,
    parents: BTreeSet<Digest>,
) -> (Digest, Certificate) {
    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

// Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
// of digests to be used as parents for the certificates of the next round.
fn make_certificates(
    start: Round,
    stop: Round,
    initial_parents: &BTreeSet<Digest>,
    keys: &[PublicKey],
) -> (VecDeque<Certificate>, BTreeSet<Digest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in start..=stop {
        next_parents.clear();
        for name in keys {
            let (digest, certificate) = mock_certificate(*name, round, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

#[test]
fn vdag_stores_grade_one_deliveries() {
    let committee = mock_committee();
    let mut state = State::new(Certificate::genesis(&committee));
    let origin = keys()[0].0;
    let (digest, certificate) = mock_certificate(origin, 1, BTreeSet::new());

    state.insert_grade_one(certificate.clone());

    let (stored_digest, stored) = state.vdag.get(&1).unwrap().get(&origin).unwrap();
    assert_eq!(stored_digest, &digest);
    assert_eq!(stored, &certificate);
    assert!(state.dag.get(&1).is_none());

    state.promote_to_dag(certificate.clone());

    assert!(state.vdag.get(&1).is_none());
    let (stored_digest, stored) = state.dag.get(&1).unwrap().get(&origin).unwrap();
    assert_eq!(stored_digest, &digest);
    assert_eq!(stored, &certificate);
}

#[test]
fn grade_two_waits_for_strong_and_weak_edges() {
    let committee = mock_committee();
    let genesis = Certificate::genesis(&committee);
    let genesis_parents = genesis.iter().map(|x| x.digest()).collect();
    let mut state = State::new(genesis);
    let authority = keys()[0].0;

    let (dependency_digest, dependency) = mock_certificate(authority, 1, genesis_parents);
    let (_, mut block) = mock_certificate(authority, 3, BTreeSet::new());
    block.header.weak_edges.insert(dependency_digest.clone());
    let block_digest = block.digest();

    state.insert_grade_one(block);
    state.mark_grade_two(block_digest.clone());
    assert!(state.promote_ready().is_empty());
    assert!(state.vdag.get(&3).unwrap().contains_key(&authority));

    state.insert_grade_one(dependency);
    state.mark_grade_two(dependency_digest);
    let promoted = state.promote_ready();
    assert_eq!(promoted.len(), 2);
    assert!(state.dag_digests.contains(&block_digest));
    assert!(state.vdag.get(&3).is_none());
}

#[test]
fn finds_paths_over_strong_and_weak_edges() {
    let committee = mock_committee();
    let consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(1).0,
        tx_output: OutputSender::Individual(channel(1).0),
        genesis: Certificate::genesis(&committee),
    };
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();

    let (target_digest, target) = mock_certificate(authorities[0], 1, BTreeSet::new());
    let (middle_digest, middle) = mock_certificate(
        authorities[1],
        3,
        [target_digest.clone()].iter().cloned().collect(),
    );
    let (_, mut leader) = mock_certificate(authorities[2], 5, BTreeSet::new());
    leader.header.weak_edges.insert(middle_digest.clone());
    let leader_digest = leader.digest();

    let dag: Dag = [
        (
            1,
            [(target.origin(), (target_digest, target.clone()))]
                .iter()
                .cloned()
                .collect(),
        ),
        (
            3,
            [(middle.origin(), (middle_digest, middle))]
                .iter()
                .cloned()
                .collect(),
        ),
        (
            5,
            [(leader.origin(), (leader_digest, leader.clone()))]
                .iter()
                .cloned()
                .collect(),
        ),
    ]
    .iter()
    .cloned()
    .collect();

    let mut state = State::new(Certificate::genesis(&consensus.committee));
    for (_, block) in dag.values().flat_map(|round| round.values()) {
        state.promote_to_dag(block.clone());
    }
    let ordered = consensus.order_dag(&leader, &state);
    assert!(ordered
        .iter()
        .any(|block| block.digest() == target.digest()));

    let (_, unreachable) = mock_certificate(authorities[3], 2, BTreeSet::new());
    assert!(!ordered
        .iter()
        .any(|block| block.digest() == unreachable.digest()));
}

#[test]
fn order_dag_includes_parents_and_weak_but_excludes_virtual_history() {
    let committee = mock_committee();
    let consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(1).0,
        tx_output: OutputSender::Individual(channel(1).0),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();

    let (weak_digest, weak_parent) = mock_certificate(authorities[1], 1, BTreeSet::new());
    let (virtual_digest, virtual_parent) = mock_certificate(authorities[2], 2, BTreeSet::new());
    let (parent_digest, parent) = mock_certificate(authorities[3], 2, BTreeSet::new());
    let (_, mut leader) = mock_certificate(authorities[0], 3, BTreeSet::new());
    leader.header.parents.insert(parent_digest.clone());
    leader.header.weak_edges.insert(weak_digest.clone());
    leader.header.virtual_edges.insert(virtual_digest.clone());
    leader.header.id = leader.header.digest();

    state.promote_to_dag(weak_parent);
    state.promote_to_dag(virtual_parent);
    state.promote_to_dag(parent);
    state.promote_to_dag(leader.clone());
    let ordered = consensus.order_dag(&leader, &state);

    assert!(ordered.iter().any(|block| block.digest() == parent_digest));
    assert!(ordered.iter().any(|block| block.digest() == weak_digest));
    assert!(!ordered.iter().any(|block| block.digest() == virtual_digest));
    assert_eq!(ordered.last().unwrap().digest(), leader.digest());
}

#[test]
fn designates_one_leader_every_round() {
    let committee = mock_committee();
    let consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(1).0,
        tx_output: OutputSender::Individual(channel(1).0),
        genesis: Certificate::genesis(&committee),
    };
    let mut expected: Vec<_> = committee.authorities.keys().cloned().collect();
    expected.sort();

    for round in 0..8 {
        assert_eq!(
            consensus.leader_authority(round),
            expected[round as usize % expected.len()]
        );
    }
}

#[test]
fn commit_rule_counts_observed_and_dag_strong_support_separately() {
    let committee = mock_committee();
    let consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(1).0,
        tx_output: OutputSender::Individual(channel(1).0),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let (leader_digest, leader) = mock_certificate(authorities[0], 1, BTreeSet::new());
    state.promote_to_dag(leader);

    let parents: BTreeSet<_> = [leader_digest.clone()].iter().cloned().collect();
    let support: Vec<_> = authorities
        .iter()
        .take(3)
        .map(|authority| mock_certificate(*authority, 2, parents.clone()).1)
        .collect();
    for certificate in &support {
        state.insert_grade_one(certificate.clone());
    }

    // Three out of four authorities are observed in Dag union VDag, while no
    // supporter has entered the formal Dag yet.
    assert_eq!(
        consensus.strong_support_stake(2, &leader_digest, &mut state),
        (3, 0)
    );

    state.promote_to_dag(support[0].clone());
    state.promote_to_dag(support[1].clone());
    assert_eq!(
        consensus.strong_support_stake(2, &leader_digest, &mut state),
        (3, 2)
    );
}

#[test]
fn commit_rule_two_counts_strong_and_two_hop_virtual_paths() {
    let committee = mock_committee();
    let consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(10).0,
        tx_output: OutputSender::Individual(channel(10).0),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let (leader_digest, leader) = mock_certificate(authorities[0], 1, BTreeSet::new());
    state.promote_to_dag(leader);

    // The middle block virtually references the leader.
    let (_, mut middle) = mock_certificate(authorities[1], 2, BTreeSet::new());
    middle.header.virtual_edges.insert(leader_digest.clone());
    let middle_digest = middle.digest();
    state.promote_to_dag(middle);

    // Three formal-Dag blocks use one strong edge to the middle block, making
    // an exact strong+virtual two-hop path to the leader.
    let parents: BTreeSet<_> = [middle_digest].iter().cloned().collect();
    for authority in authorities.iter().take(3) {
        let block = mock_certificate(*authority, 3, parents.clone()).1;
        state.promote_to_dag(block);
    }

    assert_eq!(
        consensus.rule_two_support_stake(3, &leader_digest, &mut state),
        (0, 0, 3)
    );
    // A fourth authority supports the same leader through both kinds of path.
    // Its weight must contribute only once to the combined condition.
    let (strong_digest, strong) = mock_certificate(
        authorities[2],
        2,
        [leader_digest.clone()].iter().cloned().collect(),
    );
    state.observe(strong.clone());
    state.promote_to_dag(strong);
    let mut both_parents = parents;
    both_parents.insert(strong_digest);
    let both = mock_certificate(authorities[3], 3, both_parents).1;
    state.observe(both.clone());
    state.promote_to_dag(both);
    assert_eq!(
        consensus.rule_two_support_stake(3, &leader_digest, &state),
        (1, 1, 4)
    );
}

#[test]
fn virtual_path_checks_the_reference_without_loading_its_target() {
    let committee = mock_committee();
    let consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(10).0,
        tx_output: OutputSender::Individual(channel(10).0),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let missing_target = Digest::default();

    let (_, mut middle) = mock_certificate(authorities[1], 2, BTreeSet::new());
    middle.header.virtual_edges.insert(missing_target.clone());
    let middle_digest = middle.digest();
    state.observe(middle);

    let block = mock_certificate(authorities[2], 3, [middle_digest].iter().cloned().collect()).1;
    assert!(consensus.has_two_hop_virtual_path(&block, &missing_target, &state));
    assert!(!state.observed.contains_key(&missing_target));
}

#[test]
fn strong_support_index_is_bounded_by_two_rounds() {
    let committee = mock_committee();
    let genesis = Certificate::genesis(&committee);
    let parents = genesis.iter().map(|block| block.digest()).collect();
    let authorities: Vec<_> = committee.authorities.keys().copied().collect();
    let (blocks, _) = make_certificates(1, 20, &parents, &authorities);
    let mut state = State::new(genesis);
    for block in blocks {
        state.observe(block);
    }
    // A rule-2 or r+2 ABA query never needs support for more distant rounds.
    // Retained support entries must grow linearly with the observed window.
    assert!(state.observed_strong_support.len() <= 20 * authorities.len());
}

#[test]
fn two_hop_support_matches_reference_with_late_parents() {
    let committee = mock_committee();
    let genesis = Certificate::genesis(&committee);
    let parents = genesis.iter().map(|block| block.digest()).collect();
    let authorities: Vec<_> = committee.authorities.keys().copied().collect();
    let (blocks, _) = make_certificates(1, 6, &parents, &authorities);
    // Reverse delivery makes intermediates arrive after their children have
    // already entered Dag, exercising both observed and Dag index repair.
    let mut state = State::new(genesis);
    for block in blocks.into_iter().rev() {
        state.observe(block.clone());
        state.promote_to_dag(block);
        for source in state.observed.values().filter(|block| block.round() >= 2) {
            for target in state
                .observed
                .values()
                .filter(|target| target.round() + 2 == source.round())
            {
                let target_digest = target.digest();
                let expected = source.header.parents.iter().any(|parent| {
                    state.observed.get(parent).map_or(false, |middle| {
                        middle.header.parents.contains(&target_digest)
                    })
                });
                let key = (source.round(), target_digest);
                for index in [&state.observed_strong_support, &state.dag_strong_support] {
                    assert_eq!(
                        index
                            .get(&key)
                            .map_or(false, |support| support.contains(&source.origin())),
                        expected
                    );
                }
            }
        }
    }
}

#[test]
#[ignore = "manual CPU and index-size measurement"]
fn measure_strong_support_index() {
    let mut rng = StdRng::from_seed([19; 32]);
    let authorities: Vec<_> = (0..10).map(|_| generate_keypair(&mut rng).0).collect();
    let (blocks, _) = make_certificates(1, 80, &BTreeSet::new(), &authorities);
    let mut state = State::new(Vec::new());
    let started = Instant::now();
    for block in blocks {
        state.observe(block.clone());
        state.promote_to_dag(block);
    }
    eprintln!(
        "10 nodes x 80 rounds: {:?}, observed support keys {}, Dag support keys {}",
        started.elapsed(),
        state.observed_strong_support.len(),
        state.dag_strong_support.len()
    );
}

#[test]
fn unfinished_aba_instance_survives_dag_gc() {
    let committee = mock_committee();
    let name = keys()[0].0;
    let mut state = State::new(Certificate::genesis(&committee));
    state.last_committed_round = 100;
    state.aba_instances.insert(
        1,
        Aba::new(committee, name, 1, DeterministicCoin::new(0x4f52_4341)),
    );
    state.aba_inputs.insert(1);

    state.gc_protocol_state(0);

    assert!(state.aba_instances.contains_key(&1));
    assert!(state.aba_inputs.contains(&1));
}

#[test]
fn forced_admission_accepts_observed_leader_and_late_history() {
    let committee = mock_committee();
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let (late_digest, late_parent) = mock_certificate(authorities[1], 1, BTreeSet::new());
    let (_, leader) = mock_certificate(
        authorities[0],
        2,
        [late_digest.clone()].iter().cloned().collect(),
    );
    let leader_digest = leader.digest();

    state.observe(leader.clone());
    state.force_observed_history_to_dag(leader, 2);
    assert!(state.dag_digests.contains(&leader_digest));
    assert!(!state.dag_digests.contains(&late_digest));

    state.observe(late_parent);
    assert!(state.dag_digests.contains(&late_digest));
}

#[test]
fn forced_admission_does_not_follow_virtual_edges() {
    let committee = mock_committee();
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let (virtual_digest, virtual_block) = mock_certificate(authorities[1], 1, BTreeSet::new());
    state.observe(virtual_block);
    let (_, mut leader) = mock_certificate(authorities[0], 2, BTreeSet::new());
    leader.header.virtual_edges.insert(virtual_digest.clone());
    leader.header.id = leader.header.digest();

    state.force_observed_history_to_dag(leader, 2);

    assert!(!state.dag_digests.contains(&virtual_digest));
}

#[tokio::test]
async fn commit_waits_for_missing_history_even_through_an_already_forced_parent() {
    let committee = mock_committee();
    let (tx_primary, _rx_primary) = channel(100);
    let (tx_output, mut rx_output) = channel(100);
    let mut consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary,
        tx_output: OutputSender::Individual(tx_output),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let (late_digest, late) = mock_certificate(keys()[1].0, 1, BTreeSet::new());
    let (middle_digest, middle) = mock_certificate(
        keys()[2].0,
        2,
        [late_digest.clone()].iter().cloned().collect(),
    );
    state.observe(middle.clone());
    state.force_observed_history_to_dag(middle, 2);
    assert!(state.dag_digests.contains(&middle_digest));
    let (_, leader) = mock_certificate(keys()[0].0, 3, [middle_digest].iter().cloned().collect());
    state.mark_skipped(1);
    state.mark_skipped(2);
    consensus.queue_leader_commit(leader, 1, &mut state).await;
    assert!(rx_output.try_recv().is_err());
    assert!(!state.committed_leaders.contains(&3));
    assert!(state.forced_history_waiters[&late_digest].contains(&3));

    let owners = state.observe(late);
    for owner in owners {
        if let Some(leader) = state.pending_leaders.get(&owner) {
            let ordered = consensus.order_dag(leader, &state);
            state.pending_order.insert(owner, ordered);
            state.wake_pending(owner);
        }
    }
    consensus.drain_ready_leaders(&mut state).await;
    assert!(state.committed_leaders.contains(&3));
    let mut rounds = Vec::new();
    while let Ok(block) = rx_output.try_recv() {
        rounds.push(block.round());
    }
    assert_eq!(rounds, vec![1, 2, 3]);
}

#[test]
fn grade_two_dirties_only_its_r_plus_two_aba_instance() {
    let committee = mock_committee();
    let mut state = State::new(Certificate::genesis(&committee));
    let (digest, block) = mock_certificate(keys()[0].0, 7, BTreeSet::new());
    state.observe(block);

    state.mark_grade_two(digest.clone());

    let dirty: HashSet<_> = [5].iter().cloned().collect();
    let evidence: HashSet<_> = [digest].iter().cloned().collect();
    assert_eq!(state.dirty_aba_instances, dirty);
    assert_eq!(state.pending_aba_evidence.get(&5), Some(&evidence));
}

#[tokio::test]
async fn missing_aba_leader_request_is_retried() {
    let committee = mock_committee();
    let (tx_primary, mut rx_primary) = channel(10);
    let mut consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary,
        tx_output: OutputSender::Individual(channel(1).0),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    state.aba_decisions.insert(1, BinaryValue::One);
    state.missing_leader_requests.insert(1, Instant::now());

    consensus.retry_missing_leaders(&mut state).await;

    assert!(matches!(
        rx_primary.try_recv(),
        Ok(ConsensusCommand::LeaderRequest(1, _))
    ));
    assert!(state.missing_leader_requests[&1] > Instant::now());
}

#[tokio::test]
async fn aba_one_accepts_complete_observed_leader_before_local_ready() {
    let committee = mock_committee();
    let (tx_primary, mut rx_primary) = channel(100);
    let (tx_output, mut rx_output) = channel(100);
    let mut consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary,
        tx_output: OutputSender::Individual(tx_output),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let (_, leader) = mock_certificate(consensus.ordering_leader_authority(1), 1, BTreeSet::new());
    state.observe(leader.clone());
    assert!(!state.vdag.contains_key(&1));
    assert!(!state.grade_two.contains(&leader.digest()));
    state.aba_decisions.insert(1, BinaryValue::One);
    consensus
        .apply_aba_decision(1, BinaryValue::One, &mut state)
        .await;
    assert_eq!(rx_output.try_recv().unwrap().digest(), leader.digest());
    assert!(rx_output.try_recv().is_err());
    while let Ok(command) = rx_primary.try_recv() {
        assert!(!matches!(command, ConsensusCommand::LeaderRequest(_, _)));
    }
    assert!(state.committed_leaders.contains(&1));
}

#[tokio::test]
async fn aba_one_recovers_ungraded_leader_and_waits_only_for_missing_history() {
    let committee = mock_committee();
    let (tx_primary, mut rx_primary) = channel(100);
    let (tx_output, mut rx_output) = channel(100);
    let mut consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary,
        tx_output: OutputSender::Individual(tx_output),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    state.mark_skipped(1);
    state.aba_decisions.insert(2, BinaryValue::One);
    consensus
        .apply_aba_decision(2, BinaryValue::One, &mut state)
        .await;
    assert!(matches!(
        rx_primary.try_recv(),
        Ok(ConsensusCommand::LeaderRequest(2, _))
    ));
    let (parent_digest, parent) = mock_certificate(keys()[1].0, 1, BTreeSet::new());
    let (_, leader) = mock_certificate(
        consensus.ordering_leader_authority(2),
        2,
        [parent_digest.clone()].iter().cloned().collect(),
    );
    state.observe(leader.clone());
    assert!(!state.vdag.contains_key(&2));
    consensus.resolve_requested_leaders(&mut state).await;
    assert!(state.pending_leaders.contains_key(&2));
    assert!(!state.missing_leader_requests.contains_key(&2));
    assert!(rx_output.try_recv().is_err());
    let owners = state.observe(parent);
    for owner in owners {
        let ordered = consensus.order_dag(&state.pending_leaders[&owner], &state);
        state.pending_order.insert(owner, ordered);
        state.wake_pending(owner);
    }
    consensus.drain_ready_leaders(&mut state).await;
    assert_eq!(rx_output.try_recv().unwrap().digest(), parent_digest);
    assert_eq!(rx_output.try_recv().unwrap().digest(), leader.digest());
    assert!(rx_output.try_recv().is_err());
}

#[tokio::test]
async fn commit_rule_two_condition_three_forces_leader_grade_two() {
    let committee = mock_committee();
    let (tx_primary, _rx_primary) = channel(100);
    let (tx_output, _rx_output) = channel(100);
    let mut consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary,
        tx_output: OutputSender::Individual(tx_output),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let mut authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    authorities.sort();

    let (leader_digest, leader) = mock_certificate(authorities[0], 1, BTreeSet::new());
    state.insert_grade_one(leader);
    let (_, mut middle) = mock_certificate(authorities[1], 2, BTreeSet::new());
    middle.header.virtual_edges.insert(leader_digest.clone());
    let middle_digest = middle.digest();
    state.promote_to_dag(middle);
    let parents: BTreeSet<_> = [middle_digest].iter().cloned().collect();
    for authority in authorities.iter().take(3) {
        state.promote_to_dag(mock_certificate(*authority, 3, parents.clone()).1);
    }

    assert!(!state.grade_two.contains(&leader_digest));
    consensus
        .evaluate_commit_rule_two(3, &mut state, true)
        .await;
    assert!(state.grade_two.contains(&leader_digest));
    assert!(state.dag_digests.contains(&leader_digest));
    assert!(state.aba_inputs.contains(&1));
    assert!(state.direct_commit_ready.contains(&1));
    assert!(state.committed_leaders.contains(&1));
    consensus
        .apply_aba_decision(1, BinaryValue::Zero, &mut state)
        .await;
    assert!(!state.skipped_leaders.contains(&1));
}

#[tokio::test]
async fn leader_commits_wait_for_the_previous_leader() {
    let committee = mock_committee();
    let (tx_primary, _rx_primary) = channel(100);
    let (tx_output, _rx_output) = channel(100);
    let mut consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary,
        tx_output: OutputSender::Individual(tx_output),
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let (_, leader_one) = mock_certificate(authorities[0], 1, BTreeSet::new());
    let (_, leader_two) = mock_certificate(authorities[0], 2, BTreeSet::new());
    state.promote_to_dag(leader_one.clone());
    state.promote_to_dag(leader_two.clone());

    consensus
        .queue_leader_commit(leader_two, 1, &mut state)
        .await;
    assert!(!state.committed_leaders.contains(&2));
    assert!(state.pending_leaders.contains_key(&2));

    consensus
        .queue_leader_commit(leader_one, 1, &mut state)
        .await;
    assert!(state.committed_leaders.contains(&1));
    assert!(state.committed_leaders.contains(&2));
    assert!(state.pending_leaders.is_empty());
}

// Run for 4 dag rounds in ideal conditions (all nodes reference all other nodes). We should commit
// the leader of round 2.
#[tokio::test]
async fn commit_one() {
    // Make certificates for rounds 1 to 4.
    let keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, next_parents) = make_certificates(1, 4, &genesis, &keys);

    // Make one certificate with round 5 to trigger the commits.
    let (_, certificate) = mock_certificate(keys[0], 5, next_parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(100);
    let (tx_primary, rx_primary) = channel(100);
    let (tx_output, mut rx_output) = channel(100);
    Consensus::spawn(
        keys[0],
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    spawn_aba_network(rx_primary, tx_waiter.clone(), keys.clone());

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            deliver(&tx_waiter, certificate).await;
        }
    });

    // Ensure the first 4 ordered certificates are from round 1 (they are the parents of the committed
    // leader); then the leader's certificate should be committed.
    for _ in 1..=4 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 1);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 2);
}

// Run for 8 dag rounds with one dead node node (that is not a leader). We should commit the leaders of
// rounds 2, 4, and 6.
#[tokio::test]
async fn dead_node() {
    // Make the certificates.
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort(); // Ensure we don't remove one of the leaders.
    let _ = keys.pop().unwrap();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let (mut certificates, _) = make_certificates(1, 9, &genesis, &keys);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(100);
    let (tx_primary, rx_primary) = channel(100);
    let (tx_output, mut rx_output) = channel(100);
    Consensus::spawn(
        keys[0],
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    spawn_aba_network(rx_primary, tx_waiter.clone(), keys.clone());

    // Feed all certificates to the consensus.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            deliver(&tx_waiter, certificate).await;
        }
    });

    // We should commit 3 leaders (rounds 2, 4, and 6).
    for i in 1..=15 {
        let certificate = rx_output.recv().await.unwrap();
        let expected = ((i - 1) / keys.len() as u64) + 1;
        assert_eq!(certificate.round(), expected);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 6);
}

// Run for 6 dag rounds. The leaders of round 2 does not have enough support, but the leader of
// round 4 does. The leader of rounds 2 and 4 should thus be committed upon entering round 6.
#[tokio::test]
async fn not_enough_support() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Round 1: Fully connected graph.
    let nodes: Vec<_> = keys.iter().cloned().take(3).collect();
    let (out, parents) = make_certificates(1, 1, &genesis, &nodes);
    certificates.extend(out);

    // Round 2: Fully connect graph. But remember the digest of the leader. Note that this
    // round is the only one with 4 certificates.
    let (leader_2_digest, certificate) = mock_certificate(keys[0], 2, parents.clone());
    certificates.push_back(certificate);

    let nodes: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, mut parents) = make_certificates(2, 2, &parents, &nodes);
    certificates.extend(out);

    // Round 3: Only node 0 links to the leader of round 2.
    let mut next_parents = BTreeSet::new();

    let name = &keys[1];
    let (digest, certificate) = mock_certificate(*name, 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[2];
    let (digest, certificate) = mock_certificate(*name, 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[0];
    parents.insert(leader_2_digest);
    let (digest, certificate) = mock_certificate(*name, 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    parents = next_parents.clone();

    // Rounds 4, 5, and 6: Fully connected graph.
    let nodes: Vec<_> = keys.iter().cloned().take(3).collect();
    let (out, parents) = make_certificates(4, 6, &parents, &nodes);
    certificates.extend(out);

    // Round 7: Send a single certificate to trigger the commits.
    let (_, certificate) = mock_certificate(keys[0], 7, parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(100);
    let (tx_primary, rx_primary) = channel(100);
    let (tx_output, mut rx_output) = channel(100);
    Consensus::spawn(
        keys[0],
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    spawn_aba_network(rx_primary, tx_waiter.clone(), keys.clone());

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            deliver(&tx_waiter, certificate).await;
        }
    });

    // We should commit 2 leaders (rounds 2 and 4).
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 1);
    }
    for _ in 1..=4 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 2);
    }
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 3);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 4);
}

// Rule 3 skips missing early leaders and releases later commit-ready leaders.
#[tokio::test]
async fn missing_leader() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Remove the leader for rounds 1 and 2.
    let nodes: Vec<_> = keys.iter().cloned().skip(1).collect();
    let (out, parents) = make_certificates(1, 2, &genesis, &nodes);
    certificates.extend(out);

    // Add back the leader for rounds 3, 4, 5 and 6.
    let (out, parents) = make_certificates(3, 6, &parents, &keys);
    certificates.extend(out);

    // Add a certificate of round 7 to commit the leader of round 4.
    let (_, certificate) = mock_certificate(keys[0], 7, parents.clone());
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(100);
    let (tx_primary, rx_primary) = channel(100);
    let (tx_output, mut rx_output) = channel(100);
    Consensus::spawn(
        keys[0],
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    spawn_aba_network(rx_primary, tx_waiter.clone(), keys.clone());

    // Feed all certificates to the consensus. We should only commit upon receiving the last
    // certificate, so calls below should not block the task.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            deliver(&tx_waiter, certificate).await;
        }
    });

    let certificate = tokio::time::timeout(std::time::Duration::from_secs(1), rx_output.recv())
        .await
        .expect("rule 3 did not release a later leader")
        .expect("consensus output closed");
    assert!(certificate.round() > 0);
}
