use super::*;
use crate::common::{committee, keys, proposal, votes};
use crate::messages::{batch_key, proposal_header_key, GrbcVoteBatch};
use crypto::Signature;
use std::collections::BTreeMap;
use tokio::sync::mpsc::channel;

struct Harness {
    core: Core,
    events: Receiver<ConsensusMessage>,
    outgoing: Receiver<VoteDispatch>,
    _repairs: Receiver<crate::helper::RepairRequest>,
    _parents: Receiver<ProposerMessage>,
    _headers: Receiver<crate::header_waiter::WaiterMessage>,
    cleanup: tokio::sync::watch::Receiver<Round>,
}

impl Harness {
    fn new(label: &str, node: usize) -> Self {
        Self::with_committee(label, node, committee())
    }

    fn with_committee(label: &str, node: usize, c: Committee) -> Self {
        let (name, secret) = keys().remove(node);
        let path = format!(".db_test_grbc_{}_{}_{}", label, node, std::process::id());
        let store = Store::new(&path).unwrap();
        let (tx_headers, headers) = channel(100);
        let sync = Synchronizer::new(tx_headers);
        let (tx_events, events) = channel(100);
        let (tx_parents, parents) = channel(100);
        let (tx_votes, outgoing) = channel(100);
        let (tx_repairs, repairs) = channel(100);
        let (tx_cleanup, cleanup) = tokio::sync::watch::channel(0);
        Self {
            core: Core {
                name,
                quorum_threshold: c.quorum_threshold(),
                validity_threshold: c.validity_threshold(),
                vote_verifier: SignatureVerifier::new(c.authorities.keys().copied()),
                committee: c,
                store,
                synchronizer: sync,
                signature_service: SignatureService::new(secret),
                consensus_round: Arc::new(AtomicU64::new(0)),
                gc_depth: 50,
                rx_primaries: channel(1).1,
                rx_header_waiter: channel(1).1,
                rx_proposals: channel(1).1,
                in_flight: ProposalTracker::default(),
                vote_batch: Vec::new().into_iter(),
                tx_votes,
                tx_cert_requests: tx_repairs,
                tx_consensus: tx_events,
                tx_proposer: tx_parents,
                rx_consensus: channel(1).1,
                tx_cleanup,
                gc_round: 0,
                last_voted: HashMap::new(),
                votes_aggregators: HashMap::new(),
                proposal_headers: HashMap::new(),
                validated: HashSet::new(),
                last_ready: HashMap::new(),
                local_votes: HashMap::new(),
                deferred_vote_blocks: 0,
                missing_proposals: HashMap::new(),
                pending_votes: HashMap::new(),
                certificates_aggregators: HashMap::new(),
                grade_one_certificates: HashMap::new(),
                completed: HashSet::new(),
                own_pending_grbc: HashSet::new(),
                own_local_deliveries: HashMap::new(),
                weak_edge_candidates: HashMap::new(),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::new(),
            },
            events,
            outgoing,
            _repairs: repairs,
            _parents: parents,
            _headers: headers,
            cleanup,
        }
    }

    fn outgoing(&mut self) -> Vec<GrbcVote> {
        let mut votes = Vec::new();
        while let Ok(dispatch) = self.outgoing.try_recv() {
            votes.extend(dispatch.votes);
        }
        votes
    }

    fn grades(&mut self) -> Vec<u8> {
        let mut grades = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            match event {
                ConsensusMessage::GradeOne(c) => {
                    assert!(c.votes.is_empty());
                    grades.push(1);
                }
                ConsensusMessage::GradeTwo(c) => {
                    assert!(c.votes.is_empty());
                    grades.push(2);
                }
                _ => (),
            }
        }
        grades
    }
}

#[tokio::test]
async fn echo_quorum_delivers_ready_but_never_finish() {
    let mut h = Harness::new("thresholds", 0);
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(h.grades().is_empty()); // A local ECHO is not grade 1.
    let echoes = votes(&p.header, GrbcPhase::Echo);
    h.core.process_grbc_vote(echoes[1].clone()).await.unwrap();
    h.core.process_grbc_vote(echoes[1].clone()).await.unwrap();
    assert!(h.grades().is_empty());
    h.core.process_grbc_vote(echoes[2].clone()).await.unwrap();
    assert_eq!(h.grades(), vec![1]);
    assert!(h.core.completed.is_empty());
    assert!(h
        .core
        .store
        .read(p.header.id.to_vec())
        .await
        .unwrap()
        .is_none());
    assert!(h
        .core
        .store
        .read(crate::messages::grade_one_key(&p.block().digest()))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        h.outgoing()
            .iter()
            .filter(|v| v.phase == GrbcPhase::Ready)
            .count(),
        1
    );
    let ready = votes(&p.header, GrbcPhase::Ready);
    h.core.process_grbc_vote(ready[1].clone()).await.unwrap();
    assert!(h.grades().is_empty());
    h.core.process_grbc_vote(ready[2].clone()).await.unwrap();
    assert_eq!(h.grades(), vec![2]);
    h.core.process_grbc_vote(ready[2].clone()).await.unwrap();
    assert!(h.grades().is_empty());
    // FINISH is terminal: late votes and dependency loopbacks do no more work.
    h.core.process_grbc_vote(echoes[3].clone()).await.unwrap();
    h.core.process_grbc_vote(ready[3].clone()).await.unwrap();
    h.core.process_header(&p.header).await.unwrap();
    assert_eq!(h.core.votes_aggregators[&p.header.id].echo_weight, 3);
    assert_eq!(h.core.votes_aggregators[&p.header.id].ready_weight, 3);
    assert!(h.grades().is_empty());
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn own_proposal_delivers_immediately_and_still_supplies_peer_ready_votes() {
    let mut h = Harness::new("self_deliver", 3);
    let p = proposal(); // Fixture proposer is node 3.
    h.core.process_proposal(p.clone()).await.unwrap();
    assert_eq!(h.grades(), vec![1, 2]);
    assert!(h.core.completed.contains(&p.header.id));
    assert_eq!(h.core.votes_aggregators[&p.header.id].echo_weight, 1);
    assert_eq!(h.core.votes_aggregators[&p.header.id].ready_weight, 0);
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
    // Local commit feedback must not close the READY slot before peers receive
    // our normal broadcast contribution.
    h.core.process_committed(vec![p.block()]).await.unwrap();
    let echoes = votes(&p.header, GrbcPhase::Echo);
    h.core.process_grbc_vote(echoes[0].clone()).await.unwrap();
    h.core.process_grbc_vote(echoes[0].clone()).await.unwrap();
    assert!(h.outgoing().is_empty());
    h.core.process_grbc_vote(echoes[1].clone()).await.unwrap();
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].phase, GrbcPhase::Ready);
    assert!(h.grades().is_empty()); // No second local delivery.
    let ready = votes(&p.header, GrbcPhase::Ready);
    h.core.process_grbc_vote(ready[0].clone()).await.unwrap();
    h.core.process_grbc_vote(ready[1].clone()).await.unwrap();
    h.core.process_grbc_vote(ready[2].clone()).await.unwrap();
    assert_eq!(h.core.votes_aggregators[&p.header.id].ready_weight, 3);
    assert!(h.grades().is_empty());
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn runtime_review_stored_self_progresses_while_a_remote_vote_is_waiting() {
    let h = Harness::new("self_behind_remote", 3);
    let mut core = h.core;
    let mut events = h.events;
    let mut storage = core.store.clone();
    let remote = Proposal {
        header: crate::common::headers().remove(0),
        batches: BTreeMap::new(),
    };
    let own = proposal();
    let (tx, mut outgoing) = channel(1);
    tx.send(VoteDispatch {
        requestor: None,
        votes: Vec::new(),
    })
    .await
    .unwrap();
    core.tx_votes = tx;
    let (tx_stored, rx_stored) = channel(2);
    core.rx_proposals = rx_stored;
    let run = tokio::spawn(async move { core.run().await });
    let check = async {
        for p in [&remote, &own] {
            let (stored, _) = crate::proposal_processor::prepare_proposal(
                p.clone(),
                committee(),
                &mut storage,
                None,
                None,
            )
            .await
            .unwrap();
            tx_stored.send(stored).await.ok().unwrap();
            if p.header.id == remote.header.id {
                storage
                    .notify_read(local_votes_key(&remote.block().digest()))
                    .await
                    .unwrap();
            }
        }
        for expected in [1, 2] {
            let (grade, id) = loop {
                match events.recv().await.unwrap() {
                    ConsensusMessage::GradeOne(c) => break (1, c.header.id),
                    ConsensusMessage::GradeTwo(c) => break (2, c.header.id),
                    _ => {}
                }
            };
            assert_eq!((grade, id), (expected, own.header.id.clone()));
        }
        assert!(outgoing.recv().await.unwrap().votes.is_empty());
        let mut seen = HashSet::new();
        while seen.len() < 2 {
            for vote in outgoing.recv().await.unwrap().votes {
                assert_eq!(vote.phase, GrbcPhase::Echo);
                seen.insert(vote.id);
            }
        }
        assert!(seen.contains(&remote.header.id) && seen.contains(&own.header.id));
    };
    let result = tokio::time::timeout(Duration::from_secs(3), check).await;
    run.abort();
    let _ = run.await;
    result.expect("Core must deliver own grades before the earlier vote queue drains");
}

#[tokio::test]
async fn queue_invariant_qualified_remote_finishes_behind_an_unrelated_vote() {
    let mut h = Harness::new("remote_behind_remote_vote", 3);
    let (tx_votes, mut outgoing) = channel(1);
    tx_votes
        .try_send(VoteDispatch {
            requestor: None,
            votes: Vec::new(),
        })
        .unwrap();
    h.core.tx_votes = tx_votes;
    let mut remote = crate::common::headers();
    let first = remote.remove(0);
    let next = remote.remove(0);
    for header in [&first, &next] {
        h.core
            .process_proposal(Proposal {
                header: header.clone(),
                batches: BTreeMap::new(),
            })
            .await
            .unwrap();
    }
    for vote in votes(&next, GrbcPhase::Ready).into_iter().take(2) {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    assert_eq!(h.grades(), vec![1, 2]);
    assert!(h.core.completed.contains(&next.id));
    assert!(outgoing.recv().await.unwrap().votes.is_empty());
    let run = tokio::spawn(async move { h.core.run().await });
    let phases = tokio::time::timeout(Duration::from_secs(2), async {
        let mut phases = Vec::new();
        let mut saw_first = false;
        while phases.len() < 2 || !saw_first {
            for vote in outgoing.recv().await.unwrap().votes {
                if vote.id == first.id {
                    assert_eq!(vote.phase, GrbcPhase::Echo);
                    saw_first = true;
                } else {
                    assert_eq!(vote.id, next.id);
                    phases.push(vote.phase);
                }
            }
        }
        phases
    })
    .await
    .unwrap();
    assert_eq!(phases, vec![GrbcPhase::Echo, GrbcPhase::Ready]);
    run.abort();
    let _ = run.await;
}

#[tokio::test]
async fn queue_invariant_paused_worker_cleanup_preserves_latest_round_without_blocking_core() {
    let mut h = Harness::new("paused_worker_cleanup", 0);
    let mut p = proposal();
    let feedback = async {
        for round in 1..=105 {
            p.header.round = round;
            resign(&mut p);
            h.core.process_proposal(p.clone()).await.unwrap();
            h.grades();
            h.outgoing();
            h.core.process_committed(vec![p.block()]).await.unwrap();
            assert!(h._headers.try_recv().is_ok());
        }
    };
    tokio::time::timeout(Duration::from_secs(4), feedback)
        .await
        .expect("committed feedback blocked behind the paused worker cleanup consumer");
    assert_eq!(*h.cleanup.borrow_and_update(), 105);
    assert_eq!(h.core.consensus_round.load(Ordering::Relaxed), 105);
}

#[tokio::test]
async fn own_local_delivery_bypasses_a_full_vote_queue() {
    let mut h = Harness::new("self_delivery_full_votes", 3);
    let (tx_votes, mut rx_votes) = channel(1);
    tx_votes
        .send(VoteDispatch {
            requestor: None,
            votes: vec![],
        })
        .await
        .unwrap();
    h.core.tx_votes = tx_votes;
    let p = proposal();
    tokio::time::timeout(Duration::from_secs(2), h.core.process_proposal(p.clone()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(h.grades(), vec![1, 2]);
    assert_eq!(h.core.deferred_vote_blocks, 1);
    assert!(rx_votes.recv().await.unwrap().votes.is_empty());
    let run = tokio::spawn(async move { h.core.run().await });
    let dispatch = tokio::time::timeout(Duration::from_secs(2), rx_votes.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dispatch.votes[0].id, p.header.id);
    assert_eq!(dispatch.votes[0].phase, GrbcPhase::Echo);
    run.abort();
    let _ = run.await;
}

#[tokio::test]
async fn own_local_delivery_is_idempotent_and_rejects_a_second_round_variant() {
    let mut h = Harness::new("self_delivery_duplicate", 3);
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    assert_eq!(h.grades(), vec![1, 2]);
    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(h.grades().is_empty());
    let mut conflict = p.clone();
    conflict.header.weak_edges.insert(Digest([42; 32]));
    resign(&mut conflict);
    h.core.process_proposal(conflict.clone()).await.unwrap();
    assert!(h.grades().is_empty());
    assert!(!h.core.completed.contains(&conflict.header.id));
}

#[tokio::test]
async fn own_commit_before_core_registration_still_votes_and_does_not_duplicate_delivery() {
    let mut h = Harness::new("self_commit_before_callback", 3);
    let p = proposal();
    let (stored, _) = crate::proposal_processor::prepare_proposal(
        p.clone(),
        h.core.committee.clone(),
        &mut h.core.store,
        None,
        None,
    )
    .await
    .unwrap();
    for vote in votes(&p.header, GrbcPhase::Echo).iter().take(2) {
        h.core.process_grbc_vote(vote.clone()).await.unwrap();
    }
    h.core.process_committed(vec![p.block()]).await.unwrap();
    assert!(h.grades().is_empty());
    h.core.process_stored_proposal(stored).await.unwrap();
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 2);
    assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
    assert_eq!(outgoing[1].phase, GrbcPhase::Ready);
    assert!(h.grades().iter().all(|grade| *grade != 2));
}

#[tokio::test]
async fn runtime_review_early_commit_selects_both_own_voting_slots() {
    let mut h = Harness::new("self_selected_votes", 3);
    let selected = proposal();
    let (stored, _) = crate::proposal_processor::prepare_proposal(
        selected.clone(),
        committee(),
        &mut h.core.store,
        None,
        None,
    )
    .await
    .unwrap();
    h.core
        .process_committed(vec![selected.block()])
        .await
        .unwrap();
    let mut other = selected.clone();
    other.header.weak_edges.insert(Digest([42; 32]));
    resign(&mut other);
    h.core.process_proposal(other.clone()).await.unwrap();
    for vote in votes(&other.header, GrbcPhase::Ready).into_iter().take(2) {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    assert!(
        h.outgoing().is_empty(),
        "Both voting slots belong to the committed header"
    );
    h.core.process_stored_proposal(stored).await.unwrap();
    for vote in votes(&selected.header, GrbcPhase::Echo).into_iter().take(2) {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 2);
    assert!(outgoing.iter().all(|v| v.id == selected.header.id));
    assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
    assert_eq!(outgoing[1].phase, GrbcPhase::Ready);
}

#[tokio::test]
async fn invalid_own_payload_is_not_delivered_locally() {
    let mut h = Harness::new("self_invalid_payload", 3);
    let mut p = proposal();
    p.header.payload.insert(Digest([77; 32]), 0);
    resign(&mut p);
    assert!(h.core.process_proposal(p).await.is_err());
    assert!(h.grades().is_empty());
    assert!(h.core.completed.is_empty());
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn weighted_votes_can_cross_multiple_thresholds_without_another_message() {
    for phase in [GrbcPhase::Echo, GrbcPhase::Ready] {
        let mut c = committee();
        c.authorities.get_mut(&keys()[3].0).unwrap().stake = 7;
        // Total stake 10: quorum 7, relay 4. One remote vote can cross both.
        let mut h = Harness::with_committee(&format!("weighted_{:?}", phase), 0, c);
        let p = proposal();
        h.core.process_proposal(p.clone()).await.unwrap();
        assert!(h.grades().is_empty());
        h.core
            .process_grbc_vote(votes(&p.header, phase).remove(3))
            .await
            .unwrap();
        if phase == GrbcPhase::Echo {
            assert_eq!(h.grades(), vec![1]);
            h.core
                .process_grbc_vote(votes(&p.header, GrbcPhase::Ready).remove(3))
                .await
                .unwrap();
            assert_eq!(h.grades(), vec![2]);
        } else {
            assert_eq!(h.grades(), vec![1, 2]);
        }
        assert_eq!(h.core.votes_aggregators[&p.header.id].ready_weight, 8);
        let outgoing = h.outgoing();
        assert_eq!(outgoing.len(), 2);
        assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
        assert_eq!(outgoing[1].phase, GrbcPhase::Ready);
    }
}

#[tokio::test]
async fn local_votes_finish_a_single_authority_committee() {
    let name = keys()[0].0;
    let mut c = committee();
    c.authorities.retain(|key, _| *key == name);
    let mut p = Proposal {
        header: Header {
            author: name,
            round: 1,
            parents: Certificate::genesis(&c)
                .iter()
                .map(Certificate::digest)
                .collect(),
            ..Header::default()
        },
        batches: BTreeMap::new(),
    };
    resign(&mut p);
    let mut h = Harness::with_committee("single_authority", 0, c);
    h.core.process_proposal(p.clone()).await.unwrap();
    assert_eq!(h.grades(), vec![1, 2]);
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 2);
    assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
    assert_eq!(outgoing[1].phase, GrbcPhase::Ready);
}

#[tokio::test]
async fn ready_relay_finishes_without_an_echo_quorum() {
    let mut h = Harness::new("relay", 0);
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    let ready = votes(&p.header, GrbcPhase::Ready);
    h.core.process_grbc_vote(ready[1].clone()).await.unwrap();
    assert!(h.grades().is_empty());
    h.core.process_grbc_vote(ready[2].clone()).await.unwrap();
    assert_eq!(h.grades(), vec![1, 2]); // f+1 remote READY + our relay reaches 2f+1.
    assert_eq!(h.core.votes_aggregators[&p.header.id].echo_weight, 1);
}

#[tokio::test]
async fn votes_before_proposal_wait_for_data_and_do_not_crash_on_bad_metadata() {
    let mut h = Harness::new("reordering", 0);
    let p = proposal();
    let mut bad = votes(&p.header, GrbcPhase::Echo)[3].clone();
    bad.round += 1;
    bad.signature = Signature::new(&bad.digest(), &keys()[3].1);
    h.core.process_grbc_vote(bad.clone()).await.unwrap();
    for round in 3..8 {
        bad.round = round;
        bad.signature = Signature::new(&bad.digest(), &keys()[3].1);
        h.core.process_grbc_vote(bad.clone()).await.unwrap();
    }
    assert_eq!(h.core.missing_proposals.len(), 1);
    for vote in votes(&p.header, GrbcPhase::Ready).into_iter().take(3) {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    assert!(h.grades().is_empty());
    h.core.process_proposal(p.clone()).await.unwrap();
    assert_eq!(h.grades(), vec![1, 2]);
    assert!(h.core.missing_proposals.is_empty());
}

#[tokio::test]
async fn leader_repair_prefers_the_ready_proposal_over_an_equivocation() {
    let mut h = Harness::new("leader_repair", 0);
    let p = proposal();
    let mut other = p.clone();
    other.header.parents = other.header.parents.iter().take(3).cloned().collect();
    resign(&mut other);
    h.core.process_proposal(other.clone()).await.unwrap();
    h.core.process_proposal(p.clone()).await.unwrap();
    assert_eq!(
        h.core.leader_headers(p.header.round, p.header.author).len(),
        2
    );
    for vote in votes(&p.header, GrbcPhase::Ready)
        .into_iter()
        .skip(1)
        .take(2)
    {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    let replies = h.core.leader_headers(p.header.round, p.header.author);
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].id, p.header.id);
    assert_ne!(replies[0].id, other.header.id);
    assert_eq!(h.grades(), vec![1, 2]);
}

#[tokio::test]
async fn equivocation_does_not_produce_two_honest_echoes() {
    let mut h = Harness::new("equivocation", 0);
    let p = proposal();
    let mut other = p.clone();
    let first = other.header.parents.iter().next().unwrap().clone();
    other.header.parents.remove(&first); // Three distinct genesis parents remain.
    other.header.id = other.header.digest();
    other.header.signature = Signature::new(&other.header.id, &keys().pop().unwrap().1);
    h.core.process_proposal(p.clone()).await.unwrap();
    h.core.process_proposal(other).await.unwrap();
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].id, p.header.id);
    assert!(h.grades().is_empty());
}

#[tokio::test]
async fn malformed_or_missing_data_cannot_trigger_echo() {
    let mut h = Harness::new("invalid", 0);
    let mut p = proposal();
    p.header.payload.insert(Digest([7; 32]), 0);
    p.header.id = p.header.digest();
    p.header.signature = Signature::new(&p.header.id, &keys().pop().unwrap().1);
    assert!(h.core.process_proposal(p).await.is_err());
    assert!(h.outgoing().is_empty());
    assert!(h.grades().is_empty());
}

#[tokio::test]
async fn local_delivery_record_cannot_bypass_ready_quorum() {
    let mut h = Harness::new("no_certificate_shortcut", 0);
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(h.core.process_certificate(p.block()).await.is_err());
    assert!(h.grades().is_empty());
}

#[tokio::test]
async fn honest_nodes_finish_when_proposer_stops_after_propose() {
    let p = proposal(); // Node 3 proposes, then contributes no ECHO or READY.
    let mut nodes: Vec<_> = (0..3).map(|i| Harness::new("silent_proposer", i)).collect();
    for node in &mut nodes {
        node.core.process_proposal(p.clone()).await.unwrap();
    }
    // Move the actual production outboxes with duplicates and reverse order.
    for _ in 0..3 {
        let mut messages = Vec::new();
        for node in &mut nodes {
            messages.extend(node.outgoing());
        }
        for vote in messages.into_iter().rev() {
            for node in &mut nodes {
                node.core.process_grbc_vote(vote.clone()).await.unwrap();
                node.core.process_grbc_vote(vote.clone()).await.unwrap();
            }
        }
    }
    for node in &mut nodes {
        assert_eq!(node.grades(), vec![1, 2]);
    }
}

#[tokio::test]
async fn complete_payloads_progress_across_rounds_using_local_finish_parents() {
    let mut nodes: Vec<_> = (0..4).map(|i| Harness::new("multi_round", i)).collect();
    let mut parents: Vec<_> = Certificate::genesis(&committee())
        .iter()
        .map(|c| c.digest())
        .collect();
    for round in 1..=3 {
        let mut proposals = Vec::new();
        for (author, secret) in keys() {
            let data = bincode::serialize(&(0u32, vec![vec![round as u8; 256]])).unwrap();
            let digest = crate::messages::batch_digest(&data);
            let mut header = Header {
                author,
                round,
                payload: [(digest.clone(), 0)].iter().cloned().collect(),
                parents: parents.iter().cloned().collect(),
                ..Header::default()
            };
            header.id = header.digest();
            header.signature = Signature::new(&header.id, &secret);
            proposals.push(Proposal {
                header,
                batches: [(digest, data)].iter().cloned().collect(),
            });
        }
        for proposal in &proposals {
            for node in &mut nodes {
                // Exercise the exact network enum and serialization, including data.
                let wire = bincode::serialize(&PrimaryMessage::Proposal(proposal.clone())).unwrap();
                node.core
                    .process_primary_burst(Some(bincode::deserialize(&wire).unwrap()))
                    .await
                    .unwrap();
            }
        }
        for _ in 0..3 {
            let mut messages = Vec::new();
            for node in &mut nodes {
                let mut grouped = BTreeMap::<GrbcPhase, Vec<GrbcVote>>::new();
                for vote in node.outgoing() {
                    grouped.entry(vote.phase).or_default().push(vote);
                }
                for votes in grouped.into_values() {
                    messages.push(
                        bincode::serialize(&PrimaryMessage::GrbcVoteBatch(
                            GrbcVoteBatch::from_votes(votes).unwrap(),
                        ))
                        .unwrap(),
                    );
                }
            }
            for wire in messages.into_iter().rev() {
                for node in &mut nodes {
                    node.core
                        .process_primary_burst(Some(bincode::deserialize(&wire).unwrap()))
                        .await
                        .unwrap();
                }
            }
        }
        for node in &mut nodes {
            let grades = node.grades();
            assert_eq!(grades.iter().filter(|&&g| g == 1).count(), 4);
            assert_eq!(grades.iter().filter(|&&g| g == 2).count(), 4);
            let (strong, _, _, finished_round) = node._parents.try_recv().unwrap();
            assert_eq!(finished_round, round);
            assert_eq!(strong.len(), 3);
            for proposal in &proposals {
                assert!(node
                    .core
                    .store
                    .read(proposal.block().digest().to_vec())
                    .await
                    .unwrap()
                    .is_some());
                for (digest, bytes) in &proposal.batches {
                    assert_eq!(
                        node.core
                            .store
                            .read(batch_key(digest))
                            .await
                            .unwrap()
                            .as_ref(),
                        Some(bytes)
                    );
                }
            }
        }
        parents = proposals
            .iter()
            .take(3)
            .map(|p| p.block().digest())
            .collect();
    }
}

fn resign(proposal: &mut Proposal) {
    proposal.header.id = proposal.header.digest();
    let secret = keys()
        .into_iter()
        .find(|(key, _)| *key == proposal.header.author)
        .unwrap()
        .1;
    proposal.header.signature = Signature::new(&proposal.header.id, &secret);
}

#[tokio::test]
async fn deduplicated_proposal_echoes_once_without_waiting_for_parents() {
    use crate::proposal_processor::spawn_proposal_processors;
    use tokio::time::{timeout, Duration};

    let mut h = Harness::new("dedup_waiter", 0);
    let (tx_remote, rx_remote) = channel(2);
    let (tx_local, rx_local) = channel(1);
    let (tx_stored, mut rx_stored) = channel(2);
    spawn_proposal_processors(
        h.core.name,
        committee(),
        h.core.store.clone(),
        h.core.consensus_round.clone(),
        50,
        rx_remote,
        rx_local,
        tx_stored,
        h.core.in_flight.clone(),
    );
    drop(tx_local);
    let parents: Vec<_> = crate::common::headers()
        .into_iter()
        .take(3)
        .map(|header| Proposal {
            header,
            batches: BTreeMap::new(),
        })
        .collect();
    let mut child = proposal();
    child.header.round = 2;
    child.header.parents = parents.iter().map(|p| p.block().digest()).collect();
    resign(&mut child);
    for _ in 0..2 {
        let guard = h.core.in_flight.track(&child.header);
        tx_remote.send((child.clone(), guard)).await.unwrap();
    }
    drop(tx_remote);
    let stored = timeout(Duration::from_secs(5), rx_stored.recv())
        .await
        .unwrap()
        .unwrap();
    h.core.process_stored_proposal(stored).await.unwrap();
    assert!(timeout(Duration::from_secs(5), rx_stored.recv())
        .await
        .unwrap()
        .is_none());
    assert!(h.core.validated.contains(&child.header.id));
    let emitted = h.outgoing();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].id, child.header.id);
    assert_eq!(emitted[0].phase, GrbcPhase::Echo);
}

#[tokio::test]
async fn parent_quorum_metadata_is_not_loaded_before_echo() {
    let mut h = Harness::new("invalid_observation", 0);
    let mut p = proposal();
    p.header.parents = p.header.parents.iter().take(2).cloned().collect();
    resign(&mut p);
    h.core.process_proposal(p).await.unwrap();
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert_eq!(h.outgoing()[0].phase, GrbcPhase::Echo);
}

#[tokio::test]
async fn parent_round_metadata_is_not_loaded_before_echo() {
    let mut h = Harness::new("genesis_weak", 0);
    let mut p = proposal();
    let genesis = p.header.parents.iter().next().unwrap().clone();
    p.header.parents.remove(&genesis);
    p.header.weak_edges.insert(genesis);
    resign(&mut p);
    h.core.process_proposal(p).await.unwrap();
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert_eq!(h.outgoing()[0].phase, GrbcPhase::Echo);
}

#[tokio::test]
async fn delayed_weak_edge_does_not_delay_or_repeat_echo() {
    let mut h = Harness::new("delayed_weak", 0);
    let dependency = proposal();
    let mut p = proposal();
    p.header.weak_edges.insert(dependency.block().digest());
    resign(&mut p);
    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(h.core.validated.contains(&p.header.id));
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert_eq!(h.outgoing()[0].phase, GrbcPhase::Echo);
    h.core
        .store
        .write(
            dependency.block().digest().to_vec(),
            bincode::serialize(&dependency.block()).unwrap(),
        )
        .await;
    h.core.process_header(&p.header).await.unwrap();
    assert!(h.events.try_recv().is_err());
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn delayed_virtual_edge_does_not_delay_or_repeat_echo() {
    let mut h = Harness::new("delayed_virtual", 0);
    let dependency = proposal();
    let mut p = proposal();
    p.header.virtual_edges.insert(dependency.block().digest());
    resign(&mut p);
    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(h.core.validated.contains(&p.header.id));
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert_eq!(h.outgoing()[0].phase, GrbcPhase::Echo);
    h.core
        .store
        .write(
            proposal_header_key(&dependency.block().digest()),
            bincode::serialize(&dependency.header).unwrap(),
        )
        .await;
    h.core.process_header(&p.header).await.unwrap();
    assert!(h.events.try_recv().is_err());
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn local_completion_cannot_replace_a_validated_header_body() {
    let mut h = Harness::new("mutated_completion", 0);
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    for vote in votes(&p.header, GrbcPhase::Ready) {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    assert_eq!(h.grades(), vec![1, 2]);
    let mut forged = p.block();
    forged.header.round += 1;
    assert!(h.core.process_certificate(forged).await.is_err());
    assert!(h.events.try_recv().is_err());
}

#[tokio::test]
async fn forged_vote_cannot_poison_the_authenticated_vote_buffer() {
    let mut h = Harness::new("forged_buffer", 0);
    let p = proposal();
    let vote = votes(&p.header, GrbcPhase::Ready).remove(1);
    let mut forged = vote.clone();
    forged.signature = Signature::default();
    assert!(h.core.process_grbc_vote(forged).await.is_err());
    assert!(h.core.pending_votes.is_empty());
    assert!(h.core.missing_proposals.is_empty());
    h.core.process_grbc_vote(vote.clone()).await.unwrap();
    h.core.process_grbc_vote(vote).await.unwrap();
    assert_eq!(h.core.pending_votes[&p.header.id].len(), 1);
    h.core.process_proposal(p.clone()).await.unwrap();
    assert_eq!(h.core.votes_aggregators[&p.header.id].ready_weight, 1);
    assert!(h.grades().is_empty());
}

#[tokio::test]
async fn actual_commit_completes_a_stored_unregistered_proposal_without_votes() {
    let mut h = Harness::new("commit_stored", 0);
    let p = proposal();
    let guard = h.core.in_flight.track(&p.header);
    let (stored, _) = crate::proposal_processor::prepare_proposal(
        p.clone(),
        committee(),
        &mut h.core.store,
        None,
        Some(guard),
    )
    .await
    .unwrap();
    h.core
        .process_grbc_vote(votes(&p.header, GrbcPhase::Ready).remove(1))
        .await
        .unwrap();
    assert_eq!(h.core.pending_votes[&p.header.id].len(), 1);
    h.core
        .process_consensus_command(ConsensusCommand::Cleanup(p.block()))
        .await
        .unwrap();
    assert!(h.core.completed.contains(&p.header.id));
    assert!(!h.core.validated.contains(&p.header.id));
    assert!(h.core.grade_one_certificates.is_empty());
    assert!(h.core.pending_votes.is_empty());
    assert!(h.core.missing_proposals.is_empty());
    assert!(h
        .core
        .store
        .read(p.block().digest().to_vec())
        .await
        .unwrap()
        .is_some());
    assert_eq!(*h.cleanup.borrow_and_update(), p.header.round);
    assert!(matches!(h._headers.try_recv().unwrap(),
        crate::header_waiter::WaiterMessage::Committed(ref blocks)
        if blocks == &vec![(p.block().digest(),p.header.id.clone())]));
    h.core.process_stored_proposal(stored).await.unwrap();
    h.core.process_header(&p.header).await.unwrap();
    for phase in [GrbcPhase::Echo, GrbcPhase::Ready] {
        for vote in votes(&p.header, phase) {
            h.core.process_grbc_vote(vote).await.unwrap();
        }
    }
    h.core.retry_missing_proposals().await;
    assert!(h.outgoing().is_empty());
    assert!(
        h.events.try_recv().is_err(),
        "A commit must not fabricate GRBC grade events"
    );
    assert!(h.core.cancel_handlers.is_empty());
    assert!(!h.core.in_flight.contains(&p.block().digest()));
    // Commitment closes the voting slot even though no local vote was needed.
    let mut conflicting = p.clone();
    let bytes = vec![7, 8, 9];
    let digest = crate::messages::batch_digest(&bytes);
    conflicting.header.payload.insert(digest.clone(), 0);
    conflicting.batches.insert(digest, bytes);
    resign(&mut conflicting);
    h.core.process_proposal(conflicting).await.unwrap();
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn commit_requires_the_exact_complete_local_proposal() {
    let mut h = Harness::new("commit_requires_data", 0);
    let p = proposal();
    assert!(h
        .core
        .process_consensus_command(ConsensusCommand::Cleanup(p.block()))
        .await
        .is_err());
    assert!(h.core.completed.is_empty());
    assert!(!h.cleanup.has_changed().unwrap());
    h.core.process_proposal(p.clone()).await.unwrap();
    let mut changed = p.block();
    changed.header.signature = Signature::default();
    assert!(h
        .core
        .process_consensus_command(ConsensusCommand::Cleanup(changed))
        .await
        .is_err());
    assert!(h.core.completed.is_empty());
    assert!(h
        .core
        .store
        .read(p.block().digest().to_vec())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn committed_parent_batch_counts_with_grbc_once_without_gating_child_echo() {
    let mut h = Harness::new("commit_parent_batch", 0);
    let parents: Vec<_> = crate::common::headers()
        .into_iter()
        .take(3)
        .map(|header| Proposal {
            header,
            batches: BTreeMap::new(),
        })
        .collect();
    // One parent completes normally; two only have complete stored data.
    h.core.process_proposal(parents[0].clone()).await.unwrap();
    for vote in votes(&parents[0].header, GrbcPhase::Ready)
        .into_iter()
        .skip(1)
        .take(2)
    {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    assert_eq!(h.grades(), vec![1, 2]);
    h.outgoing();
    for p in &parents[1..] {
        crate::proposal_processor::prepare_proposal(
            p.clone(),
            committee(),
            &mut h.core.store,
            None,
            None,
        )
        .await
        .unwrap();
    }
    let mut child = proposal();
    child.header.round = 2;
    child.header.parents = parents.iter().map(|p| p.block().digest()).collect();
    resign(&mut child);
    h.core.process_proposal(child.clone()).await.unwrap();
    assert!(h.core.validated.contains(&child.header.id));
    assert_eq!(h.outgoing()[0].phase, GrbcPhase::Echo);
    let committed: Vec<_> = parents.iter().map(Proposal::block).collect();
    h.core
        .process_consensus_command(ConsensusCommand::CleanupBatch(committed.clone()))
        .await
        .unwrap();
    let (strong, _, _, round) = h._parents.try_recv().unwrap();
    assert_eq!(round, 1);
    assert_eq!(strong.len(), 3);
    assert!(h.grades().is_empty());
    assert!(h.outgoing().is_empty());
    assert_eq!(
        *h.cleanup.borrow_and_update(),
        committed.iter().map(Certificate::round).max().unwrap()
    );
    for p in &parents {
        assert!(h
            .core
            .store
            .read(p.block().digest().to_vec())
            .await
            .unwrap()
            .is_some());
    }
    h.core.process_header(&child.header).await.unwrap();
    assert!(h.core.validated.contains(&child.header.id));
    assert!(
        h.outgoing().is_empty(),
        "Parent arrival must not repeat ECHO"
    );
    h.core
        .process_consensus_command(ConsensusCommand::CleanupBatch(committed))
        .await
        .unwrap();
    assert!(
        h._parents.try_recv().is_err(),
        "Repeated commit must not count parents twice"
    );
    assert!(h.outgoing().is_empty());
}

async fn completed_parent_fixture(label: &str) -> (Harness, Proposal) {
    let mut h = Harness::new(label, 0);
    let mut child = proposal();
    child.header.round = 2;
    child.header.parents.clear();
    for header in crate::common::headers().into_iter().take(3) {
        let parent = Proposal {
            header,
            batches: BTreeMap::new(),
        };
        h.core.process_proposal(parent.clone()).await.unwrap();
        for vote in votes(&parent.header, GrbcPhase::Ready)
            .into_iter()
            .skip(1)
            .take(2)
        {
            h.core.process_grbc_vote(vote).await.unwrap();
        }
        assert!(h.core.completed.contains(&parent.header.id));
        child.header.parents.insert(parent.block().digest());
    }
    resign(&mut child);
    h.grades();
    h.outgoing();
    (h, child)
}

#[tokio::test]
async fn ready_only_parent_does_not_block_child_echo() {
    let (mut h, mut child) = completed_parent_fixture("cached_ready_only").await;
    let parent = Proposal {
        header: crate::common::headers().pop().unwrap(),
        batches: BTreeMap::new(),
    };
    h.core.process_proposal(parent.clone()).await.unwrap();
    for vote in votes(&parent.header, GrbcPhase::Echo)
        .into_iter()
        .skip(1)
        .take(2)
    {
        h.core.process_grbc_vote(vote).await.unwrap();
    }
    assert_eq!(h.grades(), vec![1]);
    h.outgoing();
    child.header.parents.insert(parent.block().digest());
    resign(&mut child);
    h.core.process_proposal(child.clone()).await.unwrap();
    assert!(h.core.validated.contains(&child.header.id));
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
}

#[tokio::test]
async fn late_commit_after_gc_releases_dependencies_without_reviving_old_state() {
    let mut h = Harness::new("late_commit_gc", 0);
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    h.grades();
    h.outgoing();
    h.core
        .consensus_round
        .store(h.core.gc_depth + p.header.round + 1, Ordering::Relaxed);
    h.core.cleanup();
    assert!(h.core.proposal_headers.is_empty());
    assert!(h.core.votes_aggregators.is_empty());
    // A late commit can still publish the completion record for older history.
    // No newer cleanup event will arrive to remove accidentally revived state.
    h.core.process_committed(vec![p.block()]).await.unwrap();
    assert!(h
        .core
        .store
        .read(p.block().digest().to_vec())
        .await
        .unwrap()
        .is_some());
    assert!(h.core.proposal_headers.is_empty());
    assert!(h.core.completed.is_empty());
    assert!(h.core.last_voted.is_empty());
    assert!(h.core.last_ready.is_empty());
    assert!(h.core.weak_edge_candidates.is_empty());
    assert!(h._parents.try_recv().is_err());
    assert!(h._headers.try_recv().is_ok());
    // Repeated data and votes below that boundary cannot restart broadcasting.
    assert!(h.core.process_proposal(p.clone()).await.is_err());
    assert!(h
        .core
        .process_grbc_vote(votes(&p.header, GrbcPhase::Ready).remove(1))
        .await
        .is_err());
    h.core.cleanup();
    assert!(h.outgoing().is_empty());
    assert!(h.events.try_recv().is_err());
}

#[tokio::test]
async fn commit_feedback_is_serviced_inside_a_hot_vote_burst() {
    let mut h = Harness::new("commit_burst", 0);
    let p = proposal();
    let (stored, _) = crate::proposal_processor::prepare_proposal(
        p.clone(),
        committee(),
        &mut h.core.store,
        None,
        None,
    )
    .await
    .unwrap();
    let (tx, rx) = channel(1);
    h.core.rx_consensus = rx;
    tx.send(ConsensusCommand::Cleanup(p.block())).await.unwrap();
    let batch = GrbcVoteBatch::from_votes(vec![
        votes(&p.header, GrbcPhase::Echo).remove(1);
        2 * CORE_WORK_BUDGET
    ])
    .unwrap();
    h.core
        .process_primary_burst(Some(PrimaryMessage::GrbcVoteBatch(batch)))
        .await
        .unwrap();
    assert!(h.core.completed.contains(&p.header.id));
    assert!(h.core.pending_votes.is_empty());
    assert!(h.core.cancel_handlers.is_empty());
    assert_eq!(*h.cleanup.borrow_and_update(), p.header.round);
    assert!(h.core.vote_batch.len() > 0);
    h.core.process_stored_proposal(stored).await.unwrap();
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn in_flight_proposal_suppresses_repair_through_core_registration() {
    let mut h = Harness::new("in_flight_registration", 0);
    let p = proposal();
    let digest = p.block().digest();
    let guard = h.core.in_flight.track(&p.header);
    h.core
        .process_grbc_vote(votes(&p.header, GrbcPhase::Echo).remove(1))
        .await
        .unwrap();
    h.core.retry_missing_proposals().await;
    assert!(h.core.cancel_handlers.is_empty());
    let (stored, _) = crate::proposal_processor::prepare_proposal(
        p.clone(),
        committee(),
        &mut h.core.store,
        None,
        Some(guard),
    )
    .await
    .unwrap();
    let (tx, rx) = channel(1);
    h.core.rx_proposals = rx;
    tx.send(stored).await.ok().unwrap();
    assert!(h.core.in_flight.contains(&digest));
    h.core
        .process_grbc_vote(votes(&p.header, GrbcPhase::Echo).remove(2))
        .await
        .unwrap();
    h.core.retry_missing_proposals().await;
    assert!(
        h.core.cancel_handlers.is_empty(),
        "Stored but unregistered data must not be re-requested"
    );
    h.core.process_primary_burst(None).await.unwrap();
    assert!(!h.core.in_flight.contains(&digest));
    assert!(h.core.missing_proposals.is_empty());
    assert!(h.core.pending_votes.is_empty());
    assert_eq!(h.grades(), vec![1]);
}

#[tokio::test]
async fn failed_duplicate_and_cancelled_proposals_release_repair_suppression() {
    let mut h = Harness::new("in_flight_failure", 0);
    let p = proposal();
    let digest = p.block().digest();
    let guard = h.core.in_flight.track(&p.header);
    let duplicate = h.core.in_flight.track(&p.header);
    h.core
        .process_grbc_vote(votes(&p.header, GrbcPhase::Echo).remove(1))
        .await
        .unwrap();
    let mut invalid = p.clone();
    invalid.header.signature = Signature::default();
    assert!(crate::proposal_processor::prepare_proposal(
        invalid,
        committee(),
        &mut h.core.store,
        None,
        Some(guard)
    )
    .await
    .is_err());
    assert!(
        h.core.in_flight.contains(&digest),
        "One failure must not clear another live arrival"
    );
    h.core.retry_missing_proposals().await;
    assert!(h.core.cancel_handlers.is_empty());
    let (tx, rx) = channel(1);
    tx.send((p.clone(), duplicate)).await.unwrap();
    drop(rx);
    assert!(!h.core.in_flight.contains(&digest));
    h.core.retry_missing_proposals().await;
    assert_eq!(
        h.core.cancel_handlers[&p.header.round].len(),
        committee().size() - 1
    );
}

#[tokio::test]
async fn missing_proposal_request_targets_voter_then_falls_back_until_data_arrives() {
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::time::{timeout, Duration};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    let p = proposal();
    let vote = votes(&p.header, GrbcPhase::Echo).remove(1);
    assert_ne!(vote.author, vote.origin);
    let preferred = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = committee();
    c.authorities
        .get_mut(&vote.author)
        .unwrap()
        .primary
        .primary_to_primary = preferred.local_addr().unwrap();
    c.authorities
        .get_mut(&keys()[2].0)
        .unwrap()
        .primary
        .primary_to_primary = fallback.local_addr().unwrap();
    let mut h = Harness::with_committee("preferred_repair", 0, c);
    h.core.process_grbc_vote(vote).await.unwrap();
    assert_eq!(h.core.cancel_handlers[&p.header.round].len(), 1);
    let digest = p.block().digest();
    let first_seen = h.core.missing_proposals[&digest].1;
    let (socket, _) = timeout(Duration::from_secs(2), preferred.accept())
        .await
        .unwrap()
        .unwrap();
    let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
    let wire = timeout(Duration::from_secs(2), transport.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match bincode::deserialize::<PrimaryMessage>(&wire).unwrap() {
        PrimaryMessage::CertificatesRequest(digests, requestor) => {
            assert_eq!(digests, vec![digest.clone()]);
            assert_eq!(requestor, h.core.name);
        }
        _ => panic!("Expected a proposal repair request"),
    }
    // A peer can acknowledge the request yet withhold the actual proposal.
    transport.send(Bytes::from("Ack")).await.unwrap();
    h.core.retry_missing_proposals().await;
    assert_eq!(
        h.core.cancel_handlers[&p.header.round].len(),
        committee().size()
    );
    assert_eq!(h.core.missing_proposals[&digest].1, first_seen);
    let (socket, _) = timeout(Duration::from_secs(2), fallback.accept())
        .await
        .unwrap()
        .unwrap();
    let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
    let wire = timeout(Duration::from_secs(2), transport.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(bincode::deserialize::<PrimaryMessage>(&wire).unwrap(),
        PrimaryMessage::CertificatesRequest(digests, _) if digests == vec![digest])
    );

    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(h.core.missing_proposals.is_empty());
    let requests = h.core.cancel_handlers[&p.header.round].len();
    h.core.retry_missing_proposals().await;
    assert_eq!(h.core.cancel_handlers[&p.header.round].len(), requests);
}

#[tokio::test]
async fn missing_proposals_request_immediately_and_stuck_work_cannot_suppress_forever() {
    let mut h = Harness::new("repair_deadline", 0);
    let p = proposal();
    let digest = p.block().digest();
    let vote = votes(&p.header, GrbcPhase::Echo).remove(1);
    h.core.process_grbc_vote(vote.clone()).await.unwrap();
    let peers = committee().size() - 1;
    assert_eq!(h.core.cancel_handlers[&p.header.round].len(), 1);
    let first_seen = h.core.missing_proposals[&digest].1;
    h.core.process_grbc_vote(vote).await.unwrap();
    h.core
        .process_grbc_vote(votes(&p.header, GrbcPhase::Ready).remove(1))
        .await
        .unwrap();
    assert_eq!(h.core.missing_proposals[&digest].1, first_seen);
    assert_eq!(h.core.cancel_handlers[&p.header.round].len(), 1);
    let guard = h.core.in_flight.track(&p.header);
    h.core.retry_missing_proposals().await;
    assert_eq!(h.core.cancel_handlers[&p.header.round].len(), 1);
    h.core.missing_proposals.get_mut(&digest).unwrap().1 = Instant::now() - PROPOSAL_REPAIR_GRACE;
    drop(guard);
    let _new_arrival = h.core.in_flight.track(&p.header);
    h.core.retry_missing_proposals().await;
    assert_eq!(h.core.cancel_handlers[&p.header.round].len(), 1 + peers);
}

#[tokio::test]
async fn large_vote_vector_yields_to_stored_headers_and_preserves_remaining_work() {
    let mut h = Harness::new("large_vector_fairness", 0);
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    let mut later = p.clone();
    later.header = crate::common::headers().remove(1);
    let guard = h.core.in_flight.track(&later.header);
    let (stored, _) = crate::proposal_processor::prepare_proposal(
        later.clone(),
        committee(),
        &mut h.core.store,
        None,
        Some(guard),
    )
    .await
    .unwrap();
    let (tx_stored, rx_stored) = channel(1);
    h.core.rx_proposals = rx_stored;
    let echo = votes(&p.header, GrbcPhase::Echo).remove(1);
    let mut vector = vec![echo; 2 * CORE_WORK_BUDGET];
    vector.push(votes(&later.header, GrbcPhase::Echo).remove(1));
    h.core
        .process_primary_burst(Some(PrimaryMessage::GrbcVoteBatch(
            GrbcVoteBatch::from_votes(vector).unwrap(),
        )))
        .await
        .unwrap();
    assert_eq!(h.core.vote_batch.len(), CORE_WORK_BUDGET + 2);
    tx_stored.send(stored).await.ok().unwrap();
    let (tx_net, rx_net) = channel(1);
    h.core.rx_primaries = rx_net;
    tx_net
        .send(PrimaryMessage::GrbcVote(
            votes(&later.header, GrbcPhase::Ready).remove(1),
        ))
        .await
        .unwrap();
    h.core.process_primary_burst(None).await.unwrap();
    assert!(h.core.proposal_headers.contains_key(&later.header.id));
    assert_eq!(h.core.vote_batch.len(), 2);
    assert_eq!(
        h.core.rx_primaries.len(),
        1,
        "A later frame must not overtake the vector remainder"
    );
    h.core.process_primary_burst(None).await.unwrap();
    assert_eq!(h.core.vote_batch.len(), 0);
    assert_eq!(h.core.rx_primaries.len(), 0);
    assert_eq!(h.core.votes_aggregators[&p.header.id].echo_weight, 2);
    assert_eq!(h.core.votes_aggregators[&later.header.id].echo_weight, 2);
    assert_eq!(h.core.votes_aggregators[&later.header.id].ready_weight, 1);
    assert!(h.core.cancel_handlers.is_empty());
}

#[tokio::test]
async fn proposal_completed_during_a_vote_burst_is_serviced_by_the_next_burst() {
    let mut h = Harness::new("mid_burst_registration", 0);
    let (tx_votes, mut outgoing) = channel(1);
    h.core.tx_votes = tx_votes;
    let p = proposal();
    h.core.process_proposal(p.clone()).await.unwrap();
    h.core
        .process_grbc_vote(votes(&p.header, GrbcPhase::Echo).remove(1))
        .await
        .unwrap();
    let later = Proposal {
        header: crate::common::headers().remove(1),
        batches: BTreeMap::new(),
    };
    let (stored, _) = crate::proposal_processor::prepare_proposal(
        later.clone(),
        committee(),
        &mut h.core.store,
        None,
        None,
    )
    .await
    .unwrap();
    let (tx_stored, rx_stored) = channel(1);
    h.core.rx_proposals = rx_stored;
    let mut events = h.events;
    let producer = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if matches!(event, ConsensusMessage::GradeOne(_)) {
                tx_stored.send(stored).await.ok().unwrap();
                break;
            }
        }
        // The first local ECHO fills this queue. Release it only after the
        // stored header arrives, so the READY send orders this race exactly.
        while outgoing.recv().await.is_some() {}
    });
    let vector = vec![votes(&p.header, GrbcPhase::Echo).remove(2); 2 * CORE_WORK_BUDGET];
    tokio::time::timeout(
        Duration::from_secs(5),
        h.core
            .process_primary_burst(Some(PrimaryMessage::GrbcVoteBatch(
                GrbcVoteBatch::from_votes(vector).unwrap(),
            ))),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!h.core.proposal_headers.contains_key(&later.header.id));
    assert_eq!(h.core.vote_batch.len(), CORE_WORK_BUDGET + 1);
    // A proposal arriving after this burst's service point is registered at
    // the next boundary, before processing the remaining votes.
    h.core.process_primary_burst(None).await.unwrap();
    assert!(h.core.proposal_headers.contains_key(&later.header.id));
    assert_eq!(h.core.vote_batch.len(), 1);
    producer.abort();
}

#[tokio::test]
async fn no_wait_grbc_aggregates_distinct_voters_with_missing_dependencies() {
    for edge in ["strong", "weak", "virtual"] {
        let mut h = Harness::new(&format!("no_wait_{}", edge), 0);
        let mut p = proposal();
        match edge {
            "strong" => p.header.parents = (40..43).map(|n| Digest([n; 32])).collect(),
            "weak" => {
                p.header.weak_edges.insert(Digest([43; 32]));
            }
            _ => {
                p.header.virtual_edges.insert(Digest([44; 32]));
            }
        }
        resign(&mut p);
        h.core.process_proposal(p.clone()).await.unwrap();
        let outgoing = h.outgoing();
        assert_eq!(
            outgoing.len(),
            1,
            "ECHO must not wait for {} dependencies",
            edge
        );
        assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
        assert!(h.grades().is_empty());

        let echoes = votes(&p.header, GrbcPhase::Echo);
        h.core.process_grbc_vote(echoes[3].clone()).await.unwrap();
        h.core.process_grbc_vote(echoes[3].clone()).await.unwrap();
        assert!(
            h.grades().is_empty(),
            "Duplicate signer cannot supply quorum"
        );
        h.core.process_grbc_vote(echoes[1].clone()).await.unwrap();
        assert_eq!(h.grades(), vec![1]);
        let outgoing = h.outgoing();
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].phase, GrbcPhase::Ready);

        let ready = votes(&p.header, GrbcPhase::Ready);
        h.core.process_grbc_vote(ready[3].clone()).await.unwrap();
        h.core.process_grbc_vote(ready[3].clone()).await.unwrap();
        assert!(!h.core.completed.contains(&p.header.id));
        h.core.process_grbc_vote(ready[1].clone()).await.unwrap();
        assert_eq!(h.grades(), vec![2]);
        assert!(h.core.completed.contains(&p.header.id));
        assert!(
            h._headers.try_recv().is_err(),
            "GRBC must not queue dependency waits"
        );
    }
}

#[tokio::test]
async fn no_wait_committed_quorum_advances_without_local_dependencies() {
    let mut h = Harness::new("no_wait_advance", 0);
    let mut blocks = Vec::new();
    for (author, _) in keys().into_iter().take(3) {
        let mut p = proposal();
        p.header.author = author;
        p.header.round = 3;
        p.header.parents = (40..43).map(|n| Digest([n; 32])).collect();
        p.header.weak_edges.insert(Digest([43; 32]));
        p.header.virtual_edges.insert(Digest([44; 32]));
        resign(&mut p);
        crate::proposal_store::store_verified_proposal(&mut h.core.store, p.clone())
            .await
            .unwrap();
        blocks.push(p.block());
    }
    h.core
        .process_committed(blocks[..2].to_vec())
        .await
        .unwrap();
    assert!(
        h._parents.try_recv().is_err(),
        "Round advance still requires distinct authors' quorum"
    );
    h.core
        .process_committed(vec![blocks[0].clone()])
        .await
        .unwrap();
    assert!(
        h._parents.try_recv().is_err(),
        "Duplicate block cannot supply quorum"
    );
    h.core
        .process_committed(vec![blocks[2].clone()])
        .await
        .unwrap();
    let (parents, _, _, round) = h
        ._parents
        .try_recv()
        .expect("Completed quorum must advance without its local dependencies");
    assert_eq!(round, 3);
    assert_eq!(parents.len(), 3);
}
