use super::*;
use crate::common::{committee, keys, proposal, votes};
use crate::messages::{batch_key, proposal_header_key};
use crypto::Signature;
use tokio::sync::mpsc::channel;

struct Harness {
    core: Core,
    events: Receiver<ConsensusMessage>,
    _parents: Receiver<ProposerMessage>,
    _headers: Receiver<crate::header_waiter::WaiterMessage>,
    _certs: Receiver<Certificate>,
}

impl Harness {
    fn new(label: &str, node: usize) -> Self {
        let (name, secret) = keys().remove(node);
        let c = committee();
        let path = format!(".db_test_grbc_{}_{}_{}", label, node, std::process::id());
        let store = Store::new(&path).unwrap();
        let (tx_headers, headers) = channel(100);
        let (tx_certs, certs) = channel(100);
        let sync = Synchronizer::new(name, &c, store.clone(), tx_headers, tx_certs);
        let (tx_events, events) = channel(100);
        let (tx_parents, parents) = channel(100);
        Self {
            core: Core {
                name,
                committee: c,
                store,
                synchronizer: sync,
                signature_service: SignatureService::new(secret),
                consensus_round: Arc::new(AtomicU64::new(0)),
                gc_depth: 50,
                rx_primaries: channel(1).1,
                rx_header_waiter: channel(1).1,
                rx_certificate_waiter: channel(1).1,
                rx_proposer: channel(1).1,
                tx_consensus: tx_events,
                tx_proposer: tx_parents,
                rx_consensus: channel(1).1,
                gc_round: 0,
                last_voted: HashMap::new(),
                processing: HashMap::new(),
                current_header: Header::default(),
                votes_aggregators: HashMap::new(),
                observed_headers: HashMap::new(),
                proposal_headers: HashMap::new(),
                validated: HashSet::new(),
                last_ready: HashMap::new(),
                local_votes: HashMap::new(),
                missing_proposals: HashMap::new(),
                pending_votes: HashMap::new(),
                vote_outbox: Vec::new(),
                certificates_aggregators: HashMap::new(),
                grade_one_certificates: HashMap::new(),
                grbc_certificates: HashMap::new(),
                grade_two: HashSet::new(),
                round_advance_pending: HashMap::new(),
                weak_edge_candidates: HashMap::new(),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::new(),
            },
            events,
            _parents: parents,
            _headers: headers,
            _certs: certs,
        }
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
    assert!(h.core.grade_two.is_empty());
    assert_eq!(
        h.core
            .vote_outbox
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
    assert_eq!(h.core.vote_outbox.len(), 1);
    assert_eq!(h.core.vote_outbox[0].id, p.header.id);
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
    assert!(h.core.vote_outbox.is_empty());
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
            messages.extend(std::mem::take(&mut node.core.vote_outbox));
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
                    .process_primary_message(bincode::deserialize(&wire).unwrap())
                    .await
                    .unwrap();
            }
        }
        for _ in 0..3 {
            let mut messages = Vec::new();
            for node in &mut nodes {
                let mut grouped = BTreeMap::<GrbcPhase, Vec<GrbcVote>>::new();
                for vote in std::mem::take(&mut node.core.vote_outbox) {
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
                        .process_primary_message(bincode::deserialize(&wire).unwrap())
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
    assert!(h.core.vote_outbox.is_empty());
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
    assert!(h.core.vote_outbox.is_empty());
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
    assert!(h.core.vote_outbox.is_empty());
    h.core
        .store
        .write(
            dependency.block().digest().to_vec(),
            bincode::serialize(&dependency.block()).unwrap(),
        )
        .await;
    assert!(h.core.process_header(&p.header).await.is_err());
    assert!(h.events.try_recv().is_err());
    assert!(h.core.vote_outbox.is_empty());
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
    assert!(h.core.vote_outbox.is_empty());
    h.core
        .store
        .write(
            proposal_header_key(&dependency.block().digest()),
            bincode::serialize(&dependency.header).unwrap(),
        )
        .await;
    assert!(h.core.process_header(&p.header).await.is_err());
    assert!(h.events.try_recv().is_err());
    assert!(h.core.vote_outbox.is_empty());
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
async fn virtual_metadata_recovers_without_requiring_a_local_ready_grade() {
    let mut h = Harness::new("virtual_recovery", 0);
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
    assert!(h.core.vote_outbox.is_empty());
    for vote in votes(&p.header, GrbcPhase::Ready).into_iter().skip(1) {
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
    assert_eq!(h.grades(), vec![1, 2]);
    assert!(h.core.validated.contains(&p.header.id));
}
