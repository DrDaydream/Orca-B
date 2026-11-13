use super::*;
use crate::aba::AbaMessageKind;

fn fixture() -> (
    Consensus,
    State,
    Sender<ConsensusMessage>,
    Receiver<ConsensusCommand>,
    Receiver<Certificate>,
) {
    let committee = mock_committee();
    let (input, rx_primary) = channel(1024);
    let (tx_primary, feedback) = channel(1024);
    let (tx_output, output) = channel(1024);
    let genesis = Certificate::genesis(&committee);
    let consensus = Consensus {
        name: *committee.authorities.keys().min().unwrap(),
        committee,
        gc_depth: 50,
        rx_primary,
        tx_primary,
        tx_output: OutputSender::Individual(tx_output),
        genesis: genesis.clone(),
    };
    (consensus, State::new(genesis), input, feedback, output)
}

fn authorities(consensus: &Consensus) -> Vec<PublicKey> {
    let mut names: Vec<_> = consensus.committee.authorities.keys().copied().collect();
    names.sort();
    names
}

fn take_output(output: &mut Receiver<Certificate>) -> Vec<Digest> {
    let mut digests = Vec::new();
    while let Ok(block) = output.try_recv() {
        digests.push(block.digest());
    }
    digests
}

#[tokio::test]
async fn out_of_order_skip_waits_for_the_whole_prefix() {
    let (mut consensus, mut state, _input, _feedback, mut output) = fixture();
    let names = authorities(&consensus);
    let genesis: BTreeSet<_> = consensus.genesis.iter().map(Certificate::digest).collect();
    let (_, leader_one) = mock_certificate(names[0], 1, genesis.clone());
    let (history, parents) = make_certificates(1, 2, &genesis, &names[1..]);
    for block in &history {
        state.observe(block.clone());
    }
    let (_, leader_three) = mock_certificate(names[0], 3, parents);
    state.observe(leader_three.clone());

    // ABA(2) can finish before ABA(1). Skipping 2 must not release leader 3.
    consensus
        .apply_aba_decision(2, BinaryValue::Zero, &mut state)
        .await;
    consensus
        .queue_leader_commit(leader_three.clone(), 1, &mut state)
        .await;
    assert!(!state.committed_leaders.contains(&3));
    assert!(take_output(&mut output).is_empty());

    // Resolving 1 must consume the saved skip and release 3 without a new event.
    consensus
        .queue_leader_commit(leader_one.clone(), 1, &mut state)
        .await;
    let actual = take_output(&mut output);
    assert_eq!(actual.len(), 8);
    assert_eq!(actual.first(), Some(&leader_one.digest()));
    assert_eq!(actual.last(), Some(&leader_three.digest()));
    assert_eq!(actual.iter().collect::<HashSet<_>>().len(), actual.len());
    assert!(state.pending_leaders.is_empty());

    // Another replica resolving leaders in order must produce the same output.
    let (mut other, mut other_state, _input, _feedback, mut other_output) = fixture();
    for block in history {
        other_state.observe(block);
    }
    other
        .queue_leader_commit(leader_one, 1, &mut other_state)
        .await;
    other
        .apply_aba_decision(2, BinaryValue::Zero, &mut other_state)
        .await;
    other
        .queue_leader_commit(leader_three, 1, &mut other_state)
        .await;
    assert_eq!(actual, take_output(&mut other_output));
}

#[tokio::test]
async fn committed_prefix_survives_gc_and_replayed_decisions() {
    let (mut consensus, mut state, _input, mut feedback, mut output) = fixture();
    consensus.gc_depth = 0;
    let name = consensus.name;
    let (_, first) = mock_certificate(name, 1, BTreeSet::new());
    let (_, second) = mock_certificate(name, 2, BTreeSet::new());
    consensus
        .queue_leader_commit(first.clone(), 1, &mut state)
        .await;
    consensus
        .queue_leader_commit(second.clone(), 1, &mut state)
        .await;
    assert_eq!(
        take_output(&mut output),
        vec![first.digest(), second.digest()]
    );
    assert!(!state.committed_leaders.contains(&1));
    while feedback.try_recv().is_ok() {}

    // Old terminal sets have been collected, but replay must not reopen round 1.
    consensus.queue_leader_commit(first, 1, &mut state).await;
    for value in [BinaryValue::One, BinaryValue::Zero] {
        consensus.apply_aba_decision(1, value, &mut state).await;
    }
    assert!(state.pending_leaders.is_empty());
    assert!(state.missing_leader_requests.is_empty());
    assert!(feedback.try_recv().is_err());
    assert!(take_output(&mut output).is_empty());

    let (_, third) = mock_certificate(name, 3, BTreeSet::new());
    consensus
        .queue_leader_commit(third.clone(), 1, &mut state)
        .await;
    assert_eq!(take_output(&mut output), vec![third.digest()]);
}

#[tokio::test]
async fn skipped_prefix_releases_only_the_next_pending_leader() {
    let (mut consensus, mut state, _input, _feedback, mut output) = fixture();
    let (_, leader) = mock_certificate(consensus.name, 4, BTreeSet::new());
    consensus
        .queue_leader_commit(leader.clone(), 1, &mut state)
        .await;
    for round in [3, 2, 3] {
        consensus
            .apply_aba_decision(round, BinaryValue::Zero, &mut state)
            .await;
        assert!(take_output(&mut output).is_empty());
    }
    consensus
        .apply_aba_decision(1, BinaryValue::Zero, &mut state)
        .await;
    assert_eq!(take_output(&mut output), vec![leader.digest()]);
    assert!(state.pending_leaders.is_empty());
    assert!(state.ready_pending.is_empty());
    consensus
        .apply_aba_decision(3, BinaryValue::Zero, &mut state)
        .await;
    assert!(take_output(&mut output).is_empty());
}

#[tokio::test]
async fn history_arrival_drains_the_production_commit_queue() {
    let (mut consensus, _state, input, _feedback, mut output) = fixture();
    let names = authorities(&consensus);
    let genesis: BTreeSet<_> = consensus.genesis.iter().map(Certificate::digest).collect();
    let (_, known_one) = mock_certificate(names[1], 1, genesis.clone());
    let (_, known_two) = mock_certificate(names[2], 1, genesis.clone());
    let (missing_digest, missing) = mock_certificate(names[3], 1, genesis);
    let parents = [known_one.digest(), known_two.digest(), missing_digest]
        .iter()
        .cloned()
        .collect();
    let (leader_digest, leader) = mock_certificate(names[0], 2, parents);
    let task = tokio::spawn(async move { consensus.run().await });
    input
        .send(ConsensusMessage::Observed(known_one.header))
        .await
        .unwrap();
    input
        .send(ConsensusMessage::Observed(known_two.header))
        .await
        .unwrap();
    for name in &names[1..3] {
        let message = AbaMessage {
            instance: 1,
            round: 0,
            kind: AbaMessageKind::Decide(BinaryValue::Zero),
        };
        input
            .send(ConsensusMessage::Aba(
                *name,
                bincode::serialize(&message).unwrap(),
            ))
            .await
            .unwrap();
    }
    input
        .send(ConsensusMessage::Observed(leader.header))
        .await
        .unwrap();
    for name in names.iter().take(3) {
        let (_, support) =
            mock_certificate(*name, 3, [leader_digest.clone()].iter().cloned().collect());
        input
            .send(ConsensusMessage::Observed(support.header))
            .await
            .unwrap();
    }
    assert!(time::timeout(Duration::from_millis(50), output.recv())
        .await
        .is_err());
    input
        .send(ConsensusMessage::Observed(missing.header))
        .await
        .unwrap();
    let result = time::timeout(Duration::from_secs(2), output.recv()).await;
    task.abort();
    assert!(
        matches!(result, Ok(Some(_))),
        "Complete history did not release its pending leader"
    );
}

#[tokio::test]
async fn late_leader_rechecks_both_direct_commit_rules() {
    for distance in [1, 2] {
        let (mut consensus, _state, input, _feedback, mut output) = fixture();
        let names = authorities(&consensus);
        let genesis: BTreeSet<_> = consensus.genesis.iter().map(Certificate::digest).collect();
        let (leader_digest, leader) = mock_certificate(names[0], 1, genesis.clone());
        let task = tokio::spawn(async move { consensus.run().await });
        let mut other_parents = BTreeSet::new();
        for name in &names[1..] {
            let (digest, block) = mock_certificate(*name, 1, genesis.clone());
            other_parents.insert(digest);
            input
                .send(ConsensusMessage::Observed(block.header))
                .await
                .unwrap();
        }
        let leader_parents: BTreeSet<_> = other_parents
            .iter()
            .take(2)
            .cloned()
            .chain(std::iter::once(leader_digest.clone()))
            .collect();
        let mut parents = leader_parents.clone();
        if distance == 2 {
            // Only one round-2 author references the leader: rule 1 cannot fire.
            // Every round-3 supporter reaches it through that author: rule 2 can.
            parents.clear();
            for (index, name) in names[1..].iter().enumerate() {
                let references = if index == 0 {
                    &leader_parents
                } else {
                    &other_parents
                };
                let (digest, middle) = mock_certificate(*name, 2, references.clone());
                parents.insert(digest);
                input
                    .send(ConsensusMessage::Observed(middle.header))
                    .await
                    .unwrap();
            }
        }
        for name in names.iter().take(3) {
            let (_, support) = mock_certificate(*name, 1 + distance, parents.clone());
            input
                .send(ConsensusMessage::Observed(support.header))
                .await
                .unwrap();
        }
        input
            .send(ConsensusMessage::Observed(leader.header))
            .await
            .unwrap();
        let result = time::timeout(Duration::from_secs(2), output.recv()).await;
        task.abort();
        assert!(
            matches!(result, Ok(Some(_))),
            "Rule {} was not reconsidered for its late leader",
            distance
        );
    }
}

#[tokio::test]
async fn buffered_decide_can_finish_before_local_aba_input() {
    for value in [BinaryValue::Zero, BinaryValue::One] {
        let (mut consensus, mut state, _input, _feedback, _output) = fixture();
        for name in authorities(&consensus)
            .into_iter()
            .filter(|name| *name != consensus.name)
            .take(2)
            .collect::<Vec<_>>()
        {
            consensus
                .process_aba_message(
                    name,
                    AbaMessage {
                        instance: 1,
                        round: 0,
                        kind: AbaMessageKind::Decide(value),
                    },
                    &mut state,
                )
                .await;
        }
        consensus.offer_aba_input(1, value, &mut state).await;
        assert_eq!(state.aba_decisions.get(&1), Some(&value));
        assert!(!state.aba_instances.contains_key(&1));
        assert!(!state.aba_inputs.contains(&1));
        let queued = state.aba_outbox.len();
        consensus.offer_aba_input(1, value, &mut state).await;
        assert_eq!(state.aba_outbox.len(), queued);
    }
}
