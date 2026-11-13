use super::*;
use crate::common::{committee, keys, proposal, votes};
use crate::messages::{batch_key, proposal_header_key, GrbcVoteBatch};
use crypto::Signature;
use futures::FutureExt;
use std::collections::BTreeMap;
use tokio::sync::mpsc::channel;

struct Harness {
    core: Core,
    events: Receiver<ConsensusMessage>,
    outgoing: Receiver<VoteDispatch>,
    _repairs: Receiver<(Vec<Digest>, PublicKey)>,
    _parents: Receiver<ProposerMessage>,
    _headers: Receiver<crate::header_waiter::WaiterMessage>,
    cleanup: Receiver<Vec<Certificate>>,
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
        let sync = Synchronizer::new(&c, store.clone(), tx_headers);
        let (tx_events, events) = channel(100);
        let (tx_parents, parents) = channel(100);
        let (tx_votes, outgoing) = channel(100);
        let (tx_repairs, repairs) = channel(100);
        let (tx_cleanup, cleanup) = channel(100);
        Self {
            core: Core {
                name,
                quorum_threshold: c.quorum_threshold(),
                validity_threshold: c.validity_threshold(),
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
                missing_proposals: HashMap::new(),
                pending_votes: HashMap::new(),
                certificates_aggregators: HashMap::new(),
                grade_one_certificates: HashMap::new(),
                completed: HashSet::new(),
                round_advance_pending: HashMap::new(),
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
async fn invalid_parent_quorum_cannot_trigger_echo() {
    let mut h = Harness::new("invalid_observation", 0);
    let mut p = proposal();
    p.header.parents = p.header.parents.iter().take(2).cloned().collect();
    resign(&mut p);
    assert!(h.core.process_proposal(p).await.is_err());
    // Early observation is deliberately separate from the voting gate.
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn genesis_weak_edge_cannot_skip_round_validation() {
    let mut h = Harness::new("genesis_weak", 0);
    let mut p = proposal();
    let genesis = p.header.parents.iter().next().unwrap().clone();
    p.header.parents.remove(&genesis);
    p.header.weak_edges.insert(genesis);
    resign(&mut p);
    assert!(h.core.process_proposal(p).await.is_err());
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn delayed_weak_edge_is_checked_before_echo() {
    let mut h = Harness::new("delayed_weak", 0);
    let dependency = proposal();
    let mut p = proposal();
    p.header.weak_edges.insert(dependency.block().digest());
    resign(&mut p);
    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(!h.core.validated.contains(&p.header.id));
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert!(h.outgoing().is_empty());
    h.core
        .store
        .write(
            dependency.block().digest().to_vec(),
            bincode::serialize(&dependency.block()).unwrap(),
        )
        .await;
    assert!(h.core.process_header(&p.header).await.is_err());
    assert!(h.events.try_recv().is_err());
    assert!(h.outgoing().is_empty());
}

#[tokio::test]
async fn delayed_virtual_edge_is_checked_before_echo() {
    let mut h = Harness::new("delayed_virtual", 0);
    let dependency = proposal();
    let mut p = proposal();
    p.header.virtual_edges.insert(dependency.block().digest());
    resign(&mut p);
    h.core.process_proposal(p.clone()).await.unwrap();
    assert!(!h.core.validated.contains(&p.header.id));
    assert!(matches!(
        h.events.try_recv().unwrap(),
        ConsensusMessage::Observed(_)
    ));
    assert!(h.outgoing().is_empty());
    h.core
        .store
        .write(
            proposal_header_key(&dependency.block().digest()),
            bincode::serialize(&dependency.header).unwrap(),
        )
        .await;
    assert!(h.core.process_header(&p.header).await.is_err());
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
        false,
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
    assert_eq!(h.cleanup.try_recv().unwrap()[0].header, p.header);
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
    assert!(h.cleanup.try_recv().is_err());
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
async fn committed_parent_batch_unlocks_dependencies_and_counts_with_grbc_once() {
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
            false,
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
    assert!(!h.core.validated.contains(&child.header.id));
    assert!(h.outgoing().is_empty());
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
    assert_eq!(h.cleanup.try_recv().unwrap().len(), 3);
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
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].id, child.header.id);
    assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
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
async fn completed_parent_lookup_does_not_wait_for_storage() {
    let (mut h, child) = completed_parent_fixture("cached_parent_lookup").await;
    // A store actor cannot reply during this first poll on a current-thread
    // runtime. Completion here proves known parents incur no storage wait.
    let parents = h
        .core
        .synchronizer
        .get_parents(
            &child.header,
            &h.core.grade_one_certificates,
            &h.core.completed,
        )
        .now_or_never()
        .expect("Completed parent metadata must be immediately available")
        .unwrap();
    assert_eq!(parents.len(), 3);
    let mut mixed = child.header.clone();
    let weak = mixed.parents.pop_first().unwrap();
    mixed.weak_edges.insert(weak);
    assert_eq!(
        h.core
            .synchronizer
            .get_parents(&mixed, &h.core.grade_one_certificates, &h.core.completed,)
            .now_or_never()
            .expect("Completed weak metadata must also avoid storage waits")
            .unwrap()
            .len(),
        3
    );
    h.core.process_proposal(child.clone()).await.unwrap();
    let outgoing = h.outgoing();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
    assert_eq!(outgoing[0].id, child.header.id);

    // GC may remove both in-memory indices while the immutable record remains.
    h.core
        .consensus_round
        .store(h.core.gc_depth + 2, Ordering::Relaxed);
    h.core.cleanup();
    assert!(h.core.grade_one_certificates.is_empty());
    assert!(h.core.completed.is_empty());
    assert_eq!(
        h.core
            .synchronizer
            .get_parents(
                &child.header,
                &h.core.grade_one_certificates,
                &h.core.completed,
            )
            .await
            .unwrap()
            .len(),
        3
    );
    assert!(h
        .core
        .synchronizer
        .ready_for_round_advance(
            &child.block(),
            &h.core.grade_one_certificates,
            &h.core.completed,
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn completed_parent_round_advance_does_not_wait_for_storage() {
    let (mut h, child) = completed_parent_fixture("cached_parent_advance").await;
    assert_eq!(
        h.core
            .synchronizer
            .ready_for_round_advance(
                &child.block(),
                &h.core.grade_one_certificates,
                &h.core.completed
            )
            .now_or_never()
            .expect("Completed dependencies must not wait on the store")
            .unwrap(),
        true
    );
}

#[tokio::test]
async fn ready_only_parent_still_blocks_echo_and_round_advance() {
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
    assert!(!h.core.validated.contains(&child.header.id));
    assert!(h.outgoing().is_empty());
    assert!(!h
        .core
        .synchronizer
        .ready_for_round_advance(
            &child.block(),
            &h.core.grade_one_certificates,
            &h.core.completed
        )
        .await
        .unwrap());

    child.header.parents.remove(&parent.block().digest());
    child.header.weak_edges.insert(parent.block().digest());
    assert!(h
        .core
        .synchronizer
        .get_parents(
            &child.header,
            &h.core.grade_one_certificates,
            &h.core.completed,
        )
        .await
        .unwrap()
        .is_empty());
    assert!(!h
        .core
        .synchronizer
        .ready_for_round_advance(
            &child.block(),
            &h.core.grade_one_certificates,
            &h.core.completed,
        )
        .await
        .unwrap());

    // A virtual edge needs authenticated metadata, not local FINISH.
    child.header.weak_edges.clear();
    child.header.virtual_edges.insert(parent.block().digest());
    assert_eq!(
        h.core
            .synchronizer
            .get_parents(
                &child.header,
                &h.core.grade_one_certificates,
                &h.core.completed,
            )
            .now_or_never()
            .expect("READY virtual metadata is already available")
            .unwrap()
            .len(),
        4
    );
}

#[tokio::test]
#[ignore = "manual dependency-processing microbenchmark, not end-to-end TPS"]
async fn benchmark_completed_dependency_processing() {
    for references in [10, 11, 20] {
        let mut h = Harness::new(&format!("dependency_bench_{}", references), 0);
        let mut child = proposal();
        child.header.round = 2;
        child.header.parents.clear();
        for index in 0..references {
            let mut parent = proposal().block();
            // Unique immutable records with committee-sized reference sets.
            parent.header.payload.insert(Digest([index as u8; 32]), 0);
            parent.header.parents = (0..references).map(|i| Digest([i as u8; 32])).collect();
            parent.header.id = parent.header.digest();
            let digest = parent.digest();
            h.core
                .store
                .write(digest.to_vec(), bincode::serialize(&parent).unwrap())
                .await;
            h.core.completed.insert(parent.header.id.clone());
            h.core.grade_one_certificates.insert(digest.clone(), parent);
            child.header.parents.insert(digest);
        }
        // Flush setup writes before starting the measurements.
        h.core.store.read(vec![]).await.unwrap();
        let block = child.block();
        let iterations = 100 * references;
        let start = Instant::now();
        for _ in 0..iterations {
            assert_eq!(
                h.core
                    .synchronizer
                    .get_parents(
                        &child.header,
                        &h.core.grade_one_certificates,
                        &h.core.completed
                    )
                    .await
                    .unwrap()
                    .len(),
                references
            );
            assert!(h
                .core
                .synchronizer
                .ready_for_round_advance(&block, &h.core.grade_one_certificates, &h.core.completed)
                .await
                .unwrap());
        }
        println!(
            "DEPENDENCY_BENCH references={} pairs={} total_us={} us_per_pair={:.2}",
            references,
            iterations,
            start.elapsed().as_micros(),
            start.elapsed().as_secs_f64() * 1_000_000.0 / iterations as f64
        );
    }
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
    assert!(h.core.round_advance_pending.is_empty());
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
        false,
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
    assert_eq!(h.cleanup.try_recv().unwrap().len(), 1);
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
        false,
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
        false,
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
        false,
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
        false,
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
async fn virtual_metadata_recovers_without_requiring_a_local_ready_grade() {
    for phase in [GrbcPhase::Echo, GrbcPhase::Ready] {
        let mut h = Harness::new(&format!("virtual_recovery_{:?}", phase), 0);
        let parents = crate::common::headers();
        for header in &parents[..3] {
            let block = Certificate {
                header: header.clone(),
                votes: vec![],
            };
            h.core
                .store
                .write(block.digest().to_vec(), bincode::serialize(&block).unwrap())
                .await;
        }
        let dependency = Proposal {
            header: parents[3].clone(),
            batches: BTreeMap::new(),
        };
        let mut p = proposal();
        p.header.round = 2;
        p.header.parents = parents[..3]
            .iter()
            .map(|header| {
                Certificate {
                    header: header.clone(),
                    votes: vec![],
                }
                .digest()
            })
            .collect();
        p.header.virtual_edges.insert(dependency.block().digest());
        resign(&mut p);
        h.core.process_proposal(p.clone()).await.unwrap();
        assert!(matches!(
            h.events.try_recv().unwrap(),
            ConsensusMessage::Observed(_)
        ));
        assert!(h.outgoing().is_empty());
        for vote in votes(&p.header, phase).into_iter().skip(1) {
            h.core.process_grbc_vote(vote).await.unwrap();
        }
        assert!(
            h.core.pending_votes.is_empty(),
            "known proposal must count votes while dependencies are pending"
        );
        assert!(h.events.try_recv().is_err());
        h.core
            .store
            .write(
                proposal_header_key(&dependency.block().digest()),
                bincode::serialize(&dependency.header).unwrap(),
            )
            .await;
        assert!(h
            .core
            .store
            .read(crate::messages::grade_one_key(&dependency.block().digest()))
            .await
            .unwrap()
            .is_none());
        h.core.process_header(&p.header).await.unwrap();
        if phase == GrbcPhase::Echo {
            assert_eq!(h.grades(), vec![1]);
            for vote in votes(&p.header, GrbcPhase::Ready)
                .into_iter()
                .skip(1)
                .take(2)
            {
                h.core.process_grbc_vote(vote).await.unwrap();
            }
            assert_eq!(h.grades(), vec![2]);
        } else {
            assert_eq!(h.grades(), vec![1, 2]);
        }
        assert!(h.core.validated.contains(&p.header.id));
        let outgoing = h.outgoing();
        assert_eq!(outgoing.len(), 2);
        assert_eq!(outgoing[0].phase, GrbcPhase::Echo);
        assert_eq!(outgoing[1].phase, GrbcPhase::Ready);
    }
}
