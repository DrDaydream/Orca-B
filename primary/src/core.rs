// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, GrbcVotesAggregator};
use crate::error::{DagError, DagResult};
#[cfg(test)]
use crate::messages::Proposal;
use crate::messages::{
    local_votes_key, Certificate, ConsensusCommand, ConsensusMessage, GrbcPhase, GrbcVote, Header,
};
use crate::primary::{PrimaryMessage, Round};
use crate::proposal_processor::{
    prepare_remote_header, try_broadcast, ProposalTracker, StoredProposalHeader,
};
use crate::proposer::ProposerMessage;
use crate::synchronizer::Synchronizer;
use crate::verified_vote::VerifiedGrbcVote;
use crate::vote_sender::VoteDispatch;
use bytes::Bytes;
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService, SignatureVerifier};
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

const CORE_WORK_BUDGET: usize = 32;
// Unverified arrivals may defer repair only for a bounded time after the first
// authenticated vote. Repeated arrivals/votes cannot extend this deadline.
const PROPOSAL_REPAIR_GRACE: Duration = Duration::from_secs(2);

#[derive(Default)]
struct SavedVotes {
    votes: Vec<GrbcVote>,
    sent: usize,
}

pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    vote_verifier: SignatureVerifier,
    /// Fixed committee thresholds, computed once rather than for every vote.
    quorum_threshold: Stake,
    validity_threshold: Stake,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: Synchronizer,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Compact metadata delivered after full proposal validation and storage.
    rx_proposals: Receiver<StoredProposalHeader>,
    in_flight: ProposalTracker,
    /// Unconsumed votes from one wire batch, retained across bounded bursts.
    vote_batch: std::vec::IntoIter<GrbcVote>,
    tx_votes: Sender<VoteDispatch>,
    tx_cert_requests: Sender<crate::helper::RepairRequest>,
    /// Output all certificates to the consensus layer.
    tx_consensus: Sender<ConsensusMessage>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    tx_proposer: Sender<ProposerMessage>,
    rx_consensus: Receiver<ConsensusCommand>,
    /// Forward actual commits to GC only after publishing local completion.
    tx_cleanup: tokio::sync::watch::Sender<Round>,

    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers.
    last_voted: HashMap<Round, HashSet<PublicKey>>,
    votes_aggregators: HashMap<Digest, GrbcVotesAggregator>,
    /// Authenticated headers whose payload is available on our workers.
    proposal_headers: HashMap<Digest, Header>,
    validated: HashSet<Digest>,
    last_ready: HashMap<Round, HashSet<PublicKey>>,
    local_votes: HashMap<Digest, SavedVotes>,
    deferred_vote_blocks: usize,
    missing_proposals: HashMap<Digest, (Round, Instant)>,
    pending_votes: HashMap<Digest, Vec<VerifiedGrbcVote>>,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// Local records delivered after the ECHO quorum or READY relay threshold.
    grade_one_certificates: HashMap<Digest, Certificate>,
    /// Header IDs whose broadcast no longer needs local processing.
    completed: HashSet<Digest>,
    /// Own proposals already delivered locally but still owing normal GRBC
    /// participation to other nodes. Local delivery is not network FINISH.
    own_pending_grbc: HashSet<Digest>,
    /// One locally delivered self proposal per round, including after FINISH.
    own_local_deliveries: HashMap<Round, Digest>,
    /// Completed blocks not yet referenced by a weak edge.
    weak_edge_candidates: HashMap<Digest, Round>,
    /// Sender only for small synchronization requests.
    network: ReliableSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        synchronizer: Synchronizer,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_proposals: Receiver<StoredProposalHeader>,
        in_flight: ProposalTracker,
        tx_votes: Sender<VoteDispatch>,
        tx_cert_requests: Sender<crate::helper::RepairRequest>,
        tx_consensus: Sender<ConsensusMessage>,
        tx_proposer: Sender<ProposerMessage>,
        rx_consensus: Receiver<ConsensusCommand>,
        tx_cleanup: tokio::sync::watch::Sender<Round>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                quorum_threshold: committee.quorum_threshold(),
                validity_threshold: committee.validity_threshold(),
                vote_verifier: SignatureVerifier::new(committee.authorities.keys().copied()),
                committee,
                store,
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_proposals,
                in_flight,
                vote_batch: Vec::new().into_iter(),
                tx_votes,
                tx_cert_requests,
                tx_consensus,
                tx_proposer,
                rx_consensus,
                tx_cleanup,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                votes_aggregators: HashMap::new(),
                proposal_headers: HashMap::new(),
                validated: HashSet::new(),
                last_ready: HashMap::new(),
                local_votes: HashMap::new(),
                deferred_vote_blocks: 0,
                missing_proposals: HashMap::new(),
                pending_votes: HashMap::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                grade_one_certificates: HashMap::new(),
                completed: HashSet::new(),
                own_pending_grbc: HashSet::new(),
                own_local_deliveries: HashMap::new(),
                weak_edge_candidates: HashMap::new(),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
            }
            .run()
            .await;
        });
    }

    #[cfg(test)]
    async fn process_proposal(&mut self, proposal: Proposal) -> DagResult<()> {
        self.process_received_header(proposal.header).await
    }

    async fn process_received_header(&mut self, header: Header) -> DagResult<()> {
        ensure!(
            header.round >= self.gc_round,
            DagError::TooOld(header.id.clone(), header.round)
        );
        if self.grbc_finished(&header.id) || self.proposal_headers.contains_key(&header.id) {
            return Ok(());
        }
        if let Some(stored) = prepare_remote_header(
            header,
            &self.committee,
            &mut self.synchronizer,
            &mut self.store,
            None,
        )
        .await?
        {
            self.process_stored_proposal(stored).await?;
        }
        Ok(())
    }

    async fn process_stored_proposal(&mut self, stored: StoredProposalHeader) -> DagResult<()> {
        let header = stored.into_header();
        if self.grbc_finished(&header.id) {
            return Ok(());
        }
        ensure!(
            header.round >= self.gc_round,
            DagError::TooOld(header.id.clone(), header.round)
        );
        self.missing_proposals.remove(&header.block_digest());
        // Consensus may traverse complete, authenticated data before local
        // GRBC delivery. This experimental early path assumes no equivocation;
        // voting proceeds independently of local DAG dependency availability.
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.proposal_headers.entry(header.id.clone())
        {
            entry.insert(header.clone());
            self.tx_consensus
                .send(ConsensusMessage::Observed(header.clone()))
                .await
                .expect("Failed to send observed proposal");
        }
        if let Some(votes) = self.pending_votes.remove(&header.id) {
            for vote in votes {
                // Arrival resolves this header ID, including repair requests
                // created by signed votes with inconsistent round/origin fields.
                self.missing_proposals.remove(&vote.block_digest());
                // Invalid buffered metadata must not crash the primary or discard
                // the remaining valid votes in the same queue.
                if let Err(error) = self.record_vote(&vote) {
                    warn!("Ignoring invalid buffered GRBC vote: {}", error);
                }
            }
        }
        if header.author == self.name
            && !self.completed.contains(&header.id)
            && self
                .own_local_deliveries
                .get(&header.round)
                .map_or(true, |id| id == &header.id)
        {
            // Our worker quorum and local storage already confirmed this proposal.
            // Deliver it locally now, while continuing ECHO/READY for peers.
            let block = Certificate {
                header: header.clone(),
                votes: Vec::new(),
            };
            self.own_local_deliveries
                .insert(header.round, header.id.clone());
            self.own_pending_grbc.insert(header.id.clone());
            self.deliver_grade_one(&header).await?;
            if self.record_completed(&block).await? {
                self.tx_consensus
                    .send(ConsensusMessage::GradeTwo(block.clone()))
                    .await
                    .expect("Failed to deliver our local proposal");
                self.try_advance_completed(block).await?;
            }
        }
        self.process_header(&header).await
    }

    /// Count a fresh authenticated vote and report only threshold crossings.
    /// Proposal arrival rechecks votes collected before the proposal was verified.
    fn record_vote(&mut self, vote: &VerifiedGrbcVote) -> DagResult<bool> {
        let header = self
            .proposal_headers
            .get(&vote.id)
            .ok_or_else(|| DagError::MalformedHeader(vote.id.clone()))?;
        let state = self
            .votes_aggregators
            .entry(header.id.clone())
            .or_insert_with(GrbcVotesAggregator::new);
        let previous = match vote.phase {
            GrbcPhase::Echo => state.echo_weight,
            GrbcPhase::Ready => state.ready_weight,
        };
        if !state.append(vote, &self.committee, header)? {
            return Ok(false);
        }
        Ok(match vote.phase {
            GrbcPhase::Echo => {
                previous < self.quorum_threshold && state.echo_weight >= self.quorum_threshold
            }
            GrbcPhase::Ready => {
                (previous < self.validity_threshold
                    && state.ready_weight >= self.validity_threshold)
                    || (previous < self.quorum_threshold
                        && state.ready_weight >= self.quorum_threshold)
            }
        })
    }

    async fn save_local_vote(&mut self, vote: GrbcVote) -> DagResult<()> {
        let digest = vote.block_digest();
        let saved = self.local_votes.entry(digest.clone()).or_default();
        if saved.votes.iter().any(|v| v.phase == vote.phase) {
            return Ok(());
        }
        let already_pending = saved.sent < saved.votes.len();
        saved.votes.push(vote.clone());
        self.store
            .write(local_votes_key(&digest), bincode::serialize(&saved.votes)?)
            .await;
        if !already_pending && self.deferred_vote_blocks != 0 {
            self.deferred_vote_blocks += 1;
        } else if !already_pending {
            match self.tx_votes.try_send(VoteDispatch {
                requestor: None,
                votes: vec![vote],
            }) {
                Ok(()) => saved.sent += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.deferred_vote_blocks += 1
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("Vote sender stopped")
                }
            }
        }
        Ok(())
    }

    // A stalled vote stays in the existing repair cache, with at most two
    // signed phases per round and author. Reserve capacity in the run loop so
    // peer work cannot hold up a later stored-header callback.
    fn dispatch_saved_votes(&mut self, permit: tokio::sync::mpsc::Permit<'_, VoteDispatch>) {
        let saved = self
            .local_votes
            .values_mut()
            .find(|saved| saved.sent < saved.votes.len())
            .expect("Missing deferred vote");
        permit.send(VoteDispatch {
            requestor: None,
            votes: saved.votes[saved.sent..].to_vec(),
        });
        saved.sent = saved.votes.len();
        self.deferred_vote_blocks -= 1;
    }

    async fn process_consensus_command(&mut self, command: ConsensusCommand) -> DagResult<()> {
        match command {
            ConsensusCommand::Cleanup(block) => self.process_committed(vec![block]).await?,
            ConsensusCommand::CleanupBatch(blocks) => self.process_committed(blocks).await?,
            ConsensusCommand::AbaBroadcast(_) => {
                unreachable!("ABA broadcasts use the dedicated broadcaster")
            }
            ConsensusCommand::LeaderRequest(round, leader) => {
                let addresses = self
                    .committee
                    .others_primaries(&self.name)
                    .iter()
                    .map(|(_, x)| x.primary_to_primary)
                    .collect();
                let request = PrimaryMessage::LeaderRequest(round, leader, self.name);
                let bytes =
                    bincode::serialize(&request).expect("Failed to serialize leader request");
                // The request is retried by its originating protocol timer.
                let (handlers, _) = try_broadcast(&mut self.network, addresses, Bytes::from(bytes));
                self.cancel_handlers
                    .entry(round)
                    .or_default()
                    .extend(handlers);
            }
        }
        Ok(())
    }

    /// These commands come only from this node's actual consensus output.
    /// A committed block with complete local data can satisfy dependencies and
    /// count toward the next round without reconstructing its GRBC vote quorum.
    async fn process_committed(&mut self, blocks: Vec<Certificate>) -> DagResult<()> {
        let mut completed = Vec::new();
        let mut notifications = Vec::new();
        for block in &blocks {
            let header = &block.header;
            let digest = block.digest();
            // Usually Core already observed this header. The store fallback
            // also covers a stored result still waiting for Core registration.
            ensure!(
                block.votes.is_empty(),
                DagError::MalformedHeader(header.id.clone())
            );
            if let Some(stored) = self.proposal_headers.get(&header.id) {
                ensure!(
                    stored == header,
                    DagError::MalformedHeader(header.id.clone())
                );
            } else {
                let bytes = self
                    .store
                    .read(crate::messages::proposal_header_key(&digest))
                    .await?
                    .ok_or_else(|| DagError::MalformedHeader(header.id.clone()))?;
                let stored: Header = bincode::deserialize(&bytes)?;
                ensure!(
                    stored == *header,
                    DagError::MalformedHeader(header.id.clone())
                );
                if header.round >= self.gc_round {
                    self.proposal_headers.insert(header.id.clone(), stored);
                }
            }
            notifications.push((digest.clone(), header.id.clone()));
            // Old committed history still releases disk-backed dependencies,
            // but must not repopulate state below an unchanged GC boundary.
            if header.round < self.gc_round {
                self.record_completed(block).await?;
                continue;
            }
            // Commitment also closes this author's voting slot; it must not
            // permit a later conflicting variant to elicit a fresh local vote.
            // Our stored proposal may still be queued for Core registration.
            // Preserve the real ECHO and READY contribution in that case too.
            if header.author == self.name
                && !self.completed.contains(&header.id)
                && self
                    .own_local_deliveries
                    .get(&header.round)
                    .map_or(true, |id| id == &header.id)
            {
                self.own_pending_grbc.insert(header.id.clone());
                self.own_local_deliveries
                    .entry(header.round)
                    .or_insert_with(|| header.id.clone());
            }
            if header.author != self.name && !self.own_pending_grbc.contains(&header.id) {
                self.last_voted
                    .entry(header.round)
                    .or_default()
                    .insert(header.author);
                self.last_ready
                    .entry(header.round)
                    .or_default()
                    .insert(header.author);
            }
            if self.record_completed(block).await? {
                self.missing_proposals.remove(&digest);
                // A self proposal can be committed while its stored-header
                // callback is queued. Its authenticated votes must survive
                // until registration resumes the ordinary ECHO/READY path.
                if !self.own_pending_grbc.contains(&header.id) {
                    if let Some(votes) = self.pending_votes.remove(&header.id) {
                        for vote in votes {
                            self.missing_proposals.remove(&vote.block_digest());
                        }
                    }
                }
                completed.push(block);
            }
        }
        // Publish the entire causal sequence before checking round advancement,
        // so descendants see the committed ancestors in the same batch.
        self.synchronizer.committed(notifications).await;
        for block in completed {
            self.try_advance_completed(block.clone()).await?;
        }
        let round = blocks.iter().map(Certificate::round).max().unwrap_or(0);
        if self.consensus_round.fetch_max(round, Ordering::Relaxed) < round {
            self.tx_cleanup.send_replace(round);
        }
        Ok(())
    }

    async fn process_leader_request(
        &mut self,
        round: Round,
        leader: PublicKey,
        requestor: PublicKey,
    ) -> DagResult<()> {
        if self.committee.primary(&requestor).is_err() {
            return Ok(());
        }
        let digests: Vec<_> = self
            .leader_headers(round, leader)
            .into_iter()
            .map(|header| {
                Certificate {
                    header,
                    votes: Vec::new(),
                }
                .digest()
            })
            .collect();
        if !digests.is_empty() {
            // The requestor retries leader recovery while the header is missing.
            match self
                .tx_cert_requests
                .try_send(crate::helper::RepairRequest::Proposals(digests, requestor))
            {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("Repair helper stopped")
                }
            }
        }
        Ok(())
    }

    fn leader_headers(&self, round: Round, leader: PublicKey) -> Vec<Header> {
        if let Some(block) = self
            .grade_one_certificates
            .values()
            .find(|block| block.round() == round && block.origin() == leader)
        {
            return self
                .proposal_headers
                .get(&block.header.id)
                .cloned()
                .into_iter()
                .collect();
        }
        // Before READY, return every known variant instead of an arbitrary
        // HashMap entry. Peers still have to collect their own signed votes.
        self.proposal_headers
            .values()
            .filter(|h| h.round == round && h.author == leader)
            .cloned()
            .collect()
    }

    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        debug!("Processing {:?}", header);
        if self.grbc_finished(&header.id) || !self.proposal_headers.contains_key(&header.id) {
            return Ok(());
        }
        if header.author == self.name
            && self
                .own_local_deliveries
                .get(&header.round)
                .map_or(false, |id| id != &header.id)
        {
            return Ok(());
        }
        if self.validated.contains(&header.id) {
            return self.evaluate_grbc(header).await;
        }
        // Experimental no-wait path: parent availability and parent metadata
        // validation do not gate ECHO, READY or FINISH. The signed proposal
        // was authenticated and its payload confirmed by our workers.
        self.validated.insert(header.id.clone());
        if self
            .last_voted
            .entry(header.round)
            .or_default()
            .insert(header.author)
        {
            let vote = VerifiedGrbcVote::new(
                header,
                self.name,
                GrbcPhase::Echo,
                &mut self.signature_service,
            )
            .await;
            self.record_vote(&vote)?;
            self.save_local_vote(vote.into_vote()).await?;
        }
        self.evaluate_grbc(header).await
    }

    async fn process_grbc_vote(&mut self, vote: GrbcVote) -> DagResult<()> {
        // Terminal instances cannot gain weight or trigger repair. No untrusted
        // vote fields are applied when dropping this obsolete message.
        if self.grbc_finished(&vote.id) {
            return Ok(());
        }
        ensure!(
            vote.round >= self.gc_round,
            DagError::TooOld(vote.id.clone(), vote.round)
        );
        let vote = VerifiedGrbcVote::verify(vote, &self.committee, &self.vote_verifier)?;
        if !self.proposal_headers.contains_key(&vote.id) {
            let digest = vote.block_digest();
            let pending = self.pending_votes.entry(vote.id.clone()).or_default();
            if pending
                .iter()
                .any(|v| v.author == vote.author && v.phase == vote.phase)
            {
                return Ok(());
            }
            pending.push(vote.clone());
            let first = match self.missing_proposals.entry(digest.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert((vote.round, Instant::now()));
                    true
                }
                std::collections::hash_map::Entry::Occupied(_) => false,
            };
            if first && !self.in_flight.contains(&digest) {
                // A correct voter already has the authenticated header. Try it
                // first; the existing retry path still queries every peer.
                self.request_proposal(digest, vote.round, Some(vote.author))
                    .await;
            }
            return Ok(());
        }
        if self.record_vote(&vote)? && self.validated.contains(&vote.id) {
            // Borrow metadata when counting; clone the full header only when
            // a validated instance crosses a threshold that can advance GRBC.
            let header = self.proposal_headers[&vote.id].clone();
            self.evaluate_grbc(&header).await?;
        }
        Ok(())
    }

    async fn evaluate_grbc(&mut self, header: &Header) -> DagResult<()> {
        if self.grbc_finished(&header.id) || !self.validated.contains(&header.id) {
            return Ok(());
        }
        let state = match self.votes_aggregators.get(&header.id) {
            Some(state) => state,
            None => return Ok(()),
        };
        let ready = state.echo_weight >= self.quorum_threshold
            || state.ready_weight >= self.validity_threshold;
        if ready
            && self
                .last_ready
                .entry(header.round)
                .or_default()
                .insert(header.author)
        {
            let vote = VerifiedGrbcVote::new(
                header,
                self.name,
                GrbcPhase::Ready,
                &mut self.signature_service,
            )
            .await;
            self.record_vote(&vote)?;
            // Publish local READY before enqueueing the outbound vote. The
            // independent sender needs no later Core iteration to flush it.
            self.deliver_grade_one(header).await?;
            self.save_local_vote(vote.into_vote()).await?;
        }
        let finished = self
            .votes_aggregators
            .get(&header.id)
            .map_or(false, |state| state.ready_weight >= self.quorum_threshold);
        if finished {
            let block = Certificate {
                header: header.clone(),
                votes: Vec::new(),
            };
            if !self.grbc_finished(&header.id) {
                self.process_certificate(block).await?;
            }
        }
        Ok(())
    }

    fn grbc_finished(&self, id: &Digest) -> bool {
        self.completed.contains(id) && !self.own_pending_grbc.contains(id)
    }

    async fn deliver_grade_one(&mut self, header: &Header) -> DagResult<()> {
        let block = Certificate {
            header: header.clone(),
            votes: Vec::new(),
        };
        let digest = block.digest();
        if !self.grade_one_certificates.contains_key(&digest) {
            self.grade_one_certificates.insert(digest, block.clone());
            self.tx_consensus
                .send(ConsensusMessage::GradeOne(block))
                .await
                .expect("Failed to deliver READY");
        }
        Ok(())
    }

    async fn process_certificate(&mut self, block: Certificate) -> DagResult<()> {
        // This legacy-named function accepts LOCAL completion records only.
        // Completion must satisfy our own READY count.
        // Compare the entire cached authenticated header, including its body.
        // ID-only matching would allow a mutated internal record to bypass checks.
        ensure!(
            self.proposal_headers.get(&block.header.id) == Some(&block.header),
            DagError::MalformedHeader(block.header.id.clone())
        );
        ensure!(
            block.votes.is_empty()
                && self.validated.contains(&block.header.id)
                && self
                    .votes_aggregators
                    .get(&block.header.id)
                    .map_or(false, |s| s.ready_weight >= self.quorum_threshold),
            DagError::CertificateRequiresQuorum
        );
        self.own_pending_grbc.remove(&block.header.id);
        if self.completed.contains(&block.header.id) {
            return Ok(());
        }
        self.deliver_grade_one(&block.header).await?;
        if self.record_completed(&block).await? {
            debug!("GRBC grade 2 delivered for {:?}", block);
            self.tx_consensus
                .send(ConsensusMessage::GradeTwo(block.clone()))
                .await
                .expect("Failed to send grade-2 certificate to consensus");
            self.try_advance_completed(block).await?;
        }
        Ok(())
    }

    async fn request_proposal(&mut self, digest: Digest, round: Round, peer: Option<PublicKey>) {
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .filter(|(name, _)| peer.map_or(true, |peer| peer == *name))
            .map(|(_, authority)| authority.primary_to_primary)
            .collect();
        let message = PrimaryMessage::CertificatesRequest(vec![digest], self.name);
        let bytes = bincode::serialize(&message).expect("Failed to serialize proposal request");
        // The request is retried by its originating protocol timer.
        let (handlers, _) = try_broadcast(&mut self.network, addresses, Bytes::from(bytes));
        self.cancel_handlers
            .entry(round)
            .or_default()
            .extend(handlers);
    }

    async fn record_completed(&mut self, block: &Certificate) -> DagResult<bool> {
        if self.completed.contains(&block.header.id) {
            return Ok(false);
        }
        let digest = block.digest();
        self.store
            .write(digest.to_vec(), bincode::serialize(block)?)
            .await;
        if block.round() >= self.gc_round {
            self.completed.insert(block.header.id.clone());
            self.weak_edge_candidates.insert(digest, block.round());
        }
        Ok(true)
    }

    async fn try_advance_completed(&mut self, certificate: Certificate) -> DagResult<()> {
        // A completed quorum advances without waiting for its causal dependencies.
        if let Some(parents) = self
            .certificates_aggregators
            .entry(certificate.round())
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate.clone(), &self.committee)?
        {
            let round = certificate.round();
            let virtual_edges = self
                .grade_one_certificates
                .iter()
                .filter(|(_, certificate)| {
                    certificate.round() == round && !self.completed.contains(&certificate.header.id)
                })
                .map(|(digest, _)| digest.clone())
                .collect();
            let weak_edges: Vec<_> = self
                .weak_edge_candidates
                .iter()
                .filter(|(_, block_round)| **block_round < round)
                .map(|(digest, _)| digest.clone())
                .collect();
            for digest in &parents {
                self.weak_edge_candidates.remove(digest);
            }
            for digest in &weak_edges {
                self.weak_edge_candidates.remove(digest);
            }
            self.tx_consensus
                .send(ConsensusMessage::RoundAdvanced(round + 1))
                .await
                .expect("Failed to notify consensus of round advancement");
            self.tx_proposer
                .send((parents, weak_edges, virtual_edges, round))
                .await
                .expect("Failed to send GRBC edges to proposer");
        }
        Ok(())
    }

    async fn process_primary_message(&mut self, message: PrimaryMessage) -> DagResult<()> {
        match message {
            PrimaryMessage::Header(header) => self.process_received_header(header).await,
            PrimaryMessage::GrbcVote(vote) => self.process_grbc_vote(vote).await,
            PrimaryMessage::Consensus(message) => {
                message.verify(&self.committee)?;
                let batch = bincode::deserialize::<Vec<Vec<u8>>>(&message.payload)
                    .map_err(DagError::SerializationError)?;
                self.tx_consensus
                    .send(ConsensusMessage::AbaBatch(message.author, batch))
                    .await
                    .expect("Failed to deliver ABA batch");
                Ok(())
            }
            PrimaryMessage::LeaderRequest(round, leader, requestor) => {
                self.process_leader_request(round, leader, requestor).await
            }
            _ => panic!("Unexpected core message"),
        }
    }

    /// Budget actual votes, not just envelopes. Keep the remainder of a large
    /// vector in place and preserve FIFO order across bursts.
    async fn process_primary_burst(&mut self, mut first: Option<PrimaryMessage>) -> DagResult<()> {
        if let Ok(command) = self.rx_consensus.try_recv() {
            match self.process_consensus_command(command).await {
                Err(error @ DagError::StoreError(_)) => return Err(error),
                Err(error) => warn!("Ignoring consensus command: {}", error),
                Ok(()) => (),
            }
        }
        if let Ok(stored) = self.rx_proposals.try_recv() {
            match self.process_stored_proposal(stored).await {
                Err(error @ DagError::StoreError(_)) => return Err(error),
                Err(error) => warn!("Ignoring stored proposal: {}", error),
                Ok(()) => (),
            }
        }
        for _ in 0..CORE_WORK_BUDGET {
            let message = match self.vote_batch.next() {
                Some(vote) => PrimaryMessage::GrbcVote(vote),
                None => match first.take().or_else(|| self.rx_primaries.try_recv().ok()) {
                    Some(message) => message,
                    None => return Ok(()),
                },
            };
            if let PrimaryMessage::GrbcVoteBatch(batch) = message {
                self.vote_batch = batch.into_votes().into_iter();
                continue;
            }
            match self.process_primary_message(message).await {
                Err(error @ DagError::StoreError(_)) => return Err(error),
                Err(error) => warn!("Ignoring primary message: {}", error),
                Ok(()) => (),
            }
        }
        // Only a full burst needs an explicit yield; an empty input queue goes
        // straight back to select without an extra low-load context switch.
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn retry_missing_proposals(&mut self) {
        let pending: Vec<_> = self
            .missing_proposals
            .iter()
            .filter(|(digest, (_, since))| {
                since.elapsed() >= PROPOSAL_REPAIR_GRACE || !self.in_flight.contains(digest)
            })
            .map(|(digest, (round, _))| (digest.clone(), *round))
            .collect();
        for (digest, round) in pending {
            self.request_proposal(digest, round, None).await;
        }
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        let tx_votes = self.tx_votes.clone();
        let mut repair = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            let result = tokio::select! {
                permit = tx_votes.reserve(), if self.deferred_vote_blocks > 0 => {
                    self.dispatch_saved_votes(permit.expect("Vote sender stopped"));
                    Ok(())
                }
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv(), if self.vote_batch.len() == 0 =>
                    self.process_primary_burst(Some(message)).await,
                _ = std::future::ready(()), if self.vote_batch.len() != 0 =>
                    self.process_primary_burst(None).await,

                // Validated headers from proposals and completed worker repairs share one queue.
                Some(header) = self.rx_proposals.recv() => self.process_stored_proposal(header).await,
                Some(command) = self.rx_consensus.recv() => self.process_consensus_command(command).await,
                _ = repair.tick() => {
                    self.retry_missing_proposals().await;
                    Ok(())
                },
            };
            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            self.cleanup();
        }
    }

    fn cleanup(&mut self) {
        let gc_round = self
            .consensus_round
            .load(Ordering::Relaxed)
            .saturating_sub(self.gc_depth);
        if gc_round <= self.gc_round {
            return;
        }
        self.last_voted.retain(|round, _| *round >= gc_round);
        self.last_ready.retain(|round, _| *round >= gc_round);
        self.proposal_headers.retain(|_, h| h.round >= gc_round);
        let headers = &self.proposal_headers;
        self.validated.retain(|id| headers.contains_key(id));
        self.completed.retain(|id| headers.contains_key(id));
        self.own_pending_grbc.retain(|id| headers.contains_key(id));
        self.own_local_deliveries
            .retain(|round, _| *round >= gc_round);
        self.votes_aggregators
            .retain(|id, _| headers.contains_key(id));
        self.missing_proposals
            .retain(|_, (round, _)| *round >= gc_round);
        self.local_votes
            .retain(|_, saved| saved.votes.first().map_or(false, |v| v.round >= gc_round));
        self.deferred_vote_blocks = self
            .local_votes
            .values()
            .filter(|saved| saved.sent < saved.votes.len())
            .count();
        self.pending_votes
            .retain(|_, votes| votes.first().map_or(false, |v| v.round >= gc_round));
        self.certificates_aggregators
            .retain(|round, _| *round >= gc_round);
        self.grade_one_certificates
            .retain(|_, c| c.round() >= gc_round);
        self.weak_edge_candidates
            .retain(|_, round| *round >= gc_round);
        self.cancel_handlers.retain(|round, _| *round >= gc_round);
        self.gc_round = gc_round;
    }
}
