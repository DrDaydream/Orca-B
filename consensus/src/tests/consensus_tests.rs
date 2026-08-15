// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SecretKey};
use primary::Header;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::{BTreeSet, VecDeque};
use tokio::sync::mpsc::{channel, Sender};

async fn deliver(tx: &Sender<ConsensusMessage>, certificate: Certificate) {
    tx.send(ConsensusMessage::GradeOne(certificate.clone()))
        .await
        .unwrap();
    tx.send(ConsensusMessage::GradeTwo(certificate))
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
        tx_output: channel(1).0,
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

    assert!(consensus.linked_by_strong_or_weak(&leader, &target, &dag));

    let (_, unreachable) = mock_certificate(authorities[3], 2, BTreeSet::new());
    assert!(!consensus.linked_by_strong_or_weak(&leader, &unreachable, &dag));
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
        tx_output: channel(1).0,
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
        tx_output: channel(1).0,
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
        tx_output: channel(10).0,
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
}

#[test]
fn commit_rule_three_counts_exact_three_edge_virtual_paths() {
    let committee = mock_committee();
    let consensus = Consensus {
        name: keys()[0].0,
        committee: committee.clone(),
        gc_depth: 50,
        rx_primary: channel(1).1,
        tx_primary: channel(10).0,
        tx_output: channel(10).0,
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let mut authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    authorities.sort();

    let (lower_digest, lower) = mock_certificate(authorities[0], 1, BTreeSet::new());
    state.promote_to_dag(lower);

    // Two different round-2 blocks virtually reference the lower leader.
    let mut second_digests = Vec::new();
    for authority in authorities.iter().take(2) {
        let (_, mut second) = mock_certificate(*authority, 2, BTreeSet::new());
        second.header.virtual_edges.insert(lower_digest.clone());
        second_digests.push(second.digest());
        state.promote_to_dag(second);
    }

    // The same round-3 block points to both round-2 blocks. These are two
    // distinct paths because their second intermediate blocks differ.
    let second_parents = second_digests.into_iter().collect();
    let (first_digest, first) = mock_certificate(authorities[0], 3, second_parents);
    state.promote_to_dag(first);
    let first_digests = [first_digest].iter().cloned().collect();
    let (_, higher) = mock_certificate(authorities[3], 4, first_digests);

    assert_eq!(
        consensus.three_edge_virtual_path_stake(&higher, &lower_digest, &state),
        committee.validity_threshold()
    );
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
        tx_output,
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
    consensus.evaluate_commit_rule_two(3, &mut state, true).await;
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
        tx_output,
        genesis: Certificate::genesis(&committee),
    };
    let mut state = State::new(Certificate::genesis(&committee));
    let authorities: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let (_, leader_one) = mock_certificate(authorities[0], 1, BTreeSet::new());
    let (_, leader_two) = mock_certificate(authorities[0], 2, BTreeSet::new());
    state.promote_to_dag(leader_one.clone());
    state.promote_to_dag(leader_two.clone());

    consensus.queue_leader_commit(leader_two, &mut state).await;
    assert!(!state.committed_leaders.contains(&2));
    assert!(state.pending_leaders.contains_key(&2));

    consensus.queue_leader_commit(leader_one, &mut state).await;
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
