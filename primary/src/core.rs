// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, GrbcVotesAggregator};
use crate::control_broadcaster::ControlMessage;
use crate::error::{DagError, DagResult};
use crate::messages::{
    local_votes_key, Certificate, ConsensusCommand, ConsensusMessage, GrbcPhase, GrbcVote,
    GrbcVoteBatch, Header, Proposal,
};
use crate::primary::{PrimaryMessage, Round};
use crate::proposal_repair::{PendingRepair, PrimaryInput, ProposalTracker};
use crate::proposal_store::{assemble_proposal, store_verified_proposal};
use crate::proposer::ProposerMessage;
use crate::synchronizer::Synchronizer;
use crate::verified_vote::VerifiedGrbcVote;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep_until, Duration, Instant};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
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
    rx_primaries: Receiver<PrimaryInput>,
    /// Receives loopback headers from the `HeaderWaiter`.
    rx_header_waiter: Receiver<Header>,
    /// Receives loopback certificates from the `CertificateWaiter`.
    rx_certificate_waiter: Receiver<Certificate>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    /// Output all certificates to the consensus layer.
    tx_consensus: Sender<ConsensusMessage>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    tx_proposer: Sender<ProposerMessage>,
    rx_consensus: Receiver<ConsensusCommand>,

    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers.
    last_voted: HashMap<Round, HashSet<PublicKey>>,
    /// The set of headers we are currently processing.
    processing: HashMap<Round, HashSet<Digest>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    votes_aggregators: HashMap<Digest, GrbcVotesAggregator>,
    observed_headers: HashMap<Digest, Header>,
    /// Metadata of complete proposals; payload bytes live only in batch storage.
    proposal_headers: HashMap<Digest, Header>,
    validated: HashSet<Digest>,
    last_ready: HashMap<Round, HashSet<PublicKey>>,
    local_votes: HashMap<Digest, Vec<GrbcVote>>,
    missing_proposals: HashMap<Digest, PendingRepair>,
    proposal_tracker: ProposalTracker,
    pending_votes: HashMap<Digest, Vec<VerifiedGrbcVote>>,
    vote_outbox: Vec<GrbcVote>,
    tx_control: Sender<ControlMessage>,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// Local records delivered after the ECHO quorum or READY relay threshold.
    grade_one_certificates: HashMap<Digest, Certificate>,
    /// Local records delivered after the READY quorum.
    grbc_certificates: HashMap<Digest, Certificate>,
    /// Blocks for which this node collected a READY quorum.
    grade_two: HashSet<Digest>,
    round_advance_pending: HashMap<Digest, Certificate>,
    /// Grade-2 blocks not yet referenced by a weak edge.
    weak_edge_candidates: HashMap<Digest, Round>,
    /// Small votes and synchronization requests, separate from bulk data.
    network: ReliableSender,
    proposal_network: ReliableSender,
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
        rx_primaries: Receiver<PrimaryInput>,
        proposal_tracker: ProposalTracker,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_consensus: Sender<ConsensusMessage>,
        tx_proposer: Sender<ProposerMessage>,
        rx_consensus: Receiver<ConsensusCommand>,
        tx_control: Sender<ControlMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                proposal_tracker,
                rx_header_waiter,
                rx_certificate_waiter,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                rx_consensus,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
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
                tx_control,
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                grade_one_certificates: HashMap::new(),
                grbc_certificates: HashMap::new(),
                grade_two: HashSet::new(),
                round_advance_pending: HashMap::new(),
                weak_edge_candidates: HashMap::new(),
                network: ReliableSender::new(),
                proposal_network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
            }
            .run()
            .await;
        });
    }

    async fn observe_header(&mut self, header: &Header) {
        if self
            .observed_headers
            .insert(header.id.clone(), header.clone())
            .is_none()
        {
            self.tx_consensus
                .send(ConsensusMessage::Observed(header.clone()))
                .await
                .expect("Failed to send observed proposal");
        }
    }

    async fn process_proposal(&mut self, proposal: Proposal) -> DagResult<()> {
        self.sanitize_header(&proposal.header)?;
        proposal.verify(&self.committee)?;
        self.process_verified_proposal(proposal).await
    }

    // Private entry point: callers either verify the network input above or
    // return the locally verified value from broadcast_own_header below.
    async fn process_verified_proposal(&mut self, proposal: Proposal) -> DagResult<()> {
        let header = proposal.header.clone();
        let digest = proposal.block().digest();
        self.missing_proposals.remove(&digest);
        if !self.proposal_headers.contains_key(&header.id) {
            // Atomically retain missing bytes and their compact header marker.
            // Existing local-worker batches are not written a second time.
            let stored_header = store_verified_proposal(&mut self.store, proposal).await?;
            self.proposal_headers
                .insert(header.id.clone(), stored_header);
        }
        // Consensus may traverse complete, authenticated data before local
        // GRBC delivery. This experimental early path assumes no equivocation;
        // voting still waits for the DAG dependency checks below.
        self.observe_header(&header).await;
        if let Some(votes) = self.pending_votes.remove(&header.id) {
            for vote in votes {
                // Arrival resolves this header ID, including repair requests
                // created by signed votes with inconsistent round/origin fields.
                self.missing_proposals.remove(&vote.block_digest());
                // Invalid buffered metadata must not crash the primary or discard
                // the remaining valid votes in the same queue.
                if let Err(error) = self.record_vote(&vote, &header) {
                    warn!("Ignoring invalid buffered GRBC vote: {}", error);
                }
            }
        }
        self.process_header(&header).await
    }

    fn record_vote(&mut self, vote: &VerifiedGrbcVote, header: &Header) -> DagResult<bool> {
        self.votes_aggregators
            .entry(header.id.clone())
            .or_insert_with(GrbcVotesAggregator::new)
            .append(vote, &self.committee, header)
    }

    async fn save_local_vote(&mut self, vote: GrbcVote) -> DagResult<()> {
        let digest = vote.block_digest();
        let votes = self.local_votes.entry(digest.clone()).or_default();
        if !votes.iter().any(|v| v.phase == vote.phase) {
            votes.push(vote.clone());
        }
        self.store
            .write(local_votes_key(&digest), bincode::serialize(votes)?)
            .await;
        self.vote_outbox.push(vote);
        Ok(())
    }

    async fn process_consensus_command(&mut self, command: ConsensusCommand) -> DagResult<()> {
        match command {
            ConsensusCommand::Cleanup(_) | ConsensusCommand::CleanupBatch(_) => unreachable!(),
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
                let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(round)
                    .or_default()
                    .extend(handlers);
            }
        }
        Ok(())
    }

    async fn process_leader_request(
        &mut self,
        round: Round,
        leader: PublicKey,
        requestor: PublicKey,
    ) -> DagResult<()> {
        let address = match self.committee.primary(&requestor) {
            Ok(address) => address.primary_to_primary,
            Err(_) => return Ok(()),
        };
        for header in self.leader_headers(round, leader) {
            let proposal = assemble_proposal(&mut self.store, header).await?;
            let digest = proposal.block().digest();
            let message = PrimaryMessage::Proposal(proposal);
            let bytes = bincode::serialize(&message)?;
            let handler = self
                .proposal_network
                .send(address, Bytes::from(bytes))
                .await;
            self.cancel_handlers.entry(round).or_default().push(handler);
            for vote in self.local_votes.get(&digest).cloned().unwrap_or_default() {
                let bytes = bincode::serialize(&PrimaryMessage::GrbcVote(vote))?;
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers.entry(round).or_default().push(handler);
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

    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        let proposal = self.broadcast_own_header(header).await?;
        // Network connections can transmit while the local atomic write waits.
        self.process_verified_proposal(proposal).await
    }

    async fn broadcast_own_header(&mut self, header: Header) -> DagResult<Proposal> {
        self.sanitize_header(&header)?;
        self.current_header = header.clone();
        let proposal = assemble_proposal(&mut self.store, header).await?;
        proposal.verify(&self.committee)?;
        // Move the payload into the wire wrapper instead of cloning every batch.
        let round = proposal.header.round;
        let message = PrimaryMessage::Proposal(proposal);
        let bytes = bincode::serialize(&message)?;
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, a)| a.primary_to_primary)
            .collect();
        let handlers = self
            .proposal_network
            .broadcast(addresses, Bytes::from(bytes))
            .await;
        self.cancel_handlers
            .entry(round)
            .or_default()
            .extend(handlers);
        match message {
            PrimaryMessage::Proposal(proposal) => Ok(proposal),
            _ => unreachable!(),
        }
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        debug!("Processing {:?}", header);
        if !self.proposal_headers.contains_key(&header.id) {
            return Ok(());
        }
        if self.validated.contains(&header.id) {
            return self.evaluate_grbc(header).await;
        }
        // Indicate that we are processing this header.
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // Ensure we have the parents. If at least one parent is missing, the synchronizer returns an empty
        // vector; it will gather the missing parents (as well as all ancestors) from other nodes and then
        // reschedule processing of this header.
        let referenced = self.synchronizer.get_parents(header).await?;
        if referenced.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", header.id);
            return Ok(());
        }

        // Check the parent certificates. Ensure the parents form a quorum and are all from the previous round.
        let mut stake = 0;
        let mut parent_authors = HashSet::new();
        for x in referenced
            .iter()
            .filter(|x| header.parents.contains(&x.digest()))
        {
            ensure!(
                x.round() + 1 == header.round,
                DagError::MalformedHeader(header.id.clone())
            );
            ensure!(
                parent_authors.insert(x.origin()),
                DagError::MalformedHeader(header.id.clone())
            );
            stake += self.committee.stake(&x.origin());
        }
        ensure!(
            stake >= self.committee.quorum_threshold(),
            DagError::HeaderRequiresQuorum(header.id.clone())
        );

        // Weak edges must point strictly below the previous round.
        for x in referenced
            .iter()
            .filter(|x| header.weak_edges.contains(&x.digest()))
        {
            ensure!(
                x.round() + 1 < header.round,
                DagError::MalformedHeader(header.id.clone())
            );
        }

        // Virtual edges reference grade-1 blocks left in VDag at the end of
        // the immediately preceding round.
        for x in referenced
            .iter()
            .filter(|x| header.virtual_edges.contains(&x.digest()))
        {
            ensure!(
                x.round() + 1 == header.round,
                DagError::MalformedHeader(header.id.clone())
            );
        }

        // Proposal verification has already checked and stored every payload byte.
        self.validated.insert(header.id.clone());
        self.store
            .write(header.id.to_vec(), bincode::serialize(header)?)
            .await;
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
            self.record_vote(&vote, header)?;
            self.save_local_vote(vote.into_vote()).await?;
        }
        self.evaluate_grbc(header).await?;
        self.retry_round_advance_pending().await?;
        Ok(())
    }

    async fn process_grbc_vote(&mut self, vote: GrbcVote) -> DagResult<()> {
        ensure!(
            vote.round >= self.gc_round,
            DagError::TooOld(vote.id.clone(), vote.round)
        );
        let vote = VerifiedGrbcVote::verify(vote, &self.committee)?;
        let header = match self.proposal_headers.get(&vote.id).cloned() {
            Some(header) => header,
            None => {
                let digest = vote.block_digest();
                let pending = self.pending_votes.entry(vote.id.clone()).or_default();
                if pending
                    .iter()
                    .any(|v| v.author == vote.author && v.phase == vote.phase)
                {
                    return Ok(());
                }
                pending.push(vote.clone());
                // A vote may overtake data on the independent connection.
                // Repeated votes never reset this deadline or issue extra requests.
                self.missing_proposals
                    .entry(digest)
                    .or_insert_with(|| PendingRepair::new(&vote, Instant::now()));
                return Ok(());
            }
        };
        if self.record_vote(&vote, &header)? {
            self.evaluate_grbc(&header).await?;
        }
        Ok(())
    }

    async fn evaluate_grbc(&mut self, header: &Header) -> DagResult<()> {
        if !self.validated.contains(&header.id) {
            return Ok(());
        }
        let state = match self.votes_aggregators.get(&header.id) {
            Some(state) => state,
            None => return Ok(()),
        };
        let ready = state.echo_weight >= self.committee.quorum_threshold()
            || state.ready_weight >= self.committee.validity_threshold();
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
            self.record_vote(&vote, header)?;
            self.save_local_vote(vote.into_vote()).await?;
            // Deliver grade 1 before publishing our READY outbox. Neither event
            // implies FINISH, and one incoming vote can never create grade 1.
            self.deliver_grade_one(header).await?;
        }
        let finished = self
            .votes_aggregators
            .get(&header.id)
            .map_or(false, |state| {
                state.ready_weight >= self.committee.quorum_threshold()
            });
        if finished {
            let block = Certificate {
                header: header.clone(),
                votes: Vec::new(),
            };
            if !self.grade_two.contains(&block.digest()) {
                self.process_certificate(block).await?;
            }
        }
        Ok(())
    }

    async fn deliver_grade_one(&mut self, header: &Header) -> DagResult<()> {
        let block = Certificate {
            header: header.clone(),
            votes: Vec::new(),
        };
        let digest = block.digest();
        if !self.grade_one_certificates.contains_key(&digest) {
            self.store
                .write(
                    crate::messages::grade_one_key(&digest),
                    bincode::serialize(&block)?,
                )
                .await;
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
        // All callers, including waiter loopbacks, must satisfy our own READY count.
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
                    .map_or(false, |s| s.ready_weight
                        >= self.committee.quorum_threshold()),
            DagError::CertificateRequiresQuorum
        );
        let digest = block.digest();
        if self.grade_two.contains(&digest) {
            return Ok(());
        }
        self.deliver_grade_one(&block.header).await?;
        self.store
            .write(digest.to_vec(), bincode::serialize(&block)?)
            .await;
        self.grbc_certificates.insert(digest, block.clone());
        self.deliver_grade_two(block).await?;
        self.retry_round_advance_pending().await?;
        Ok(())
    }

    async fn flush_grbc_outboxes(&mut self) {
        let mut grouped = BTreeMap::<(Round, GrbcPhase), Vec<GrbcVote>>::new();
        for vote in std::mem::take(&mut self.vote_outbox) {
            grouped
                .entry((vote.round, vote.phase))
                .or_default()
                .push(vote);
        }
        if !grouped.is_empty() {
            let batches = grouped
                .into_values()
                .map(|votes| GrbcVoteBatch::from_votes(votes).expect("Mixed local GRBC votes"))
                .collect();
            self.tx_control
                .send(ControlMessage::Votes(batches))
                .await
                .expect("Failed to send GRBC batches");
        }
    }

    async fn broadcast_grbc_batch(&mut self, message: PrimaryMessage, round: Round) {
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, authority)| authority.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&message).expect("Failed to serialize GRBC batch");
        let network = if matches!(&message, PrimaryMessage::Proposal(_)) {
            &mut self.proposal_network
        } else {
            &mut self.network
        };
        let handlers = network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(round)
            .or_default()
            .extend(handlers);
    }

    async fn deliver_grade_two(&mut self, certificate: Certificate) -> DagResult<()> {
        let digest = certificate.digest();
        if self.grade_two.insert(digest.clone()) {
            debug!("GRBC grade 2 delivered for {:?}", certificate);
            self.tx_consensus
                .send(ConsensusMessage::GradeTwo(certificate.clone()))
                .await
                .expect("Failed to send grade-2 certificate to consensus");
            self.weak_edge_candidates
                .insert(digest, certificate.round());
            self.try_advance_grade_two(certificate).await?;
        }
        Ok(())
    }

    async fn try_advance_grade_two(&mut self, certificate: Certificate) -> DagResult<()> {
        if !self
            .synchronizer
            .ready_for_round_advance(&certificate)
            .await?
        {
            self.round_advance_pending
                .insert(certificate.digest(), certificate);
            return Ok(());
        }
        self.round_advance_pending.remove(&certificate.digest());
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
                .filter(|(digest, certificate)| {
                    certificate.round() == round && !self.grade_two.contains(*digest)
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

    async fn retry_round_advance_pending(&mut self) -> DagResult<()> {
        let pending: Vec<_> = self.round_advance_pending.values().cloned().collect();
        for certificate in pending {
            self.try_advance_grade_two(certificate).await?;
        }
        Ok(())
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.round,
            DagError::TooOld(header.id.clone(), header.round)
        );

        // Full Proposal verification authenticates the header exactly once.

        // TODO [issue #3]: Prevent bad nodes from sending junk headers with high round numbers.

        Ok(())
    }

    async fn process_primary_message(&mut self, message: PrimaryMessage) -> DagResult<()> {
        match message {
            PrimaryMessage::Proposal(proposal) => self.process_proposal(proposal).await,
            PrimaryMessage::GrbcVote(vote) => self.process_grbc_vote(vote).await,
            PrimaryMessage::GrbcVoteBatch(batch) => {
                for vote in batch.into_votes() {
                    if let Err(error) = self.process_grbc_vote(vote).await {
                        match error {
                            DagError::StoreError(_) => return Err(error),
                            _ => warn!("Ignoring invalid GRBC vote: {}", error),
                        }
                    }
                }
                Ok(())
            }
            PrimaryMessage::LeaderRequest(round, leader, requestor) => {
                self.process_leader_request(round, leader, requestor).await
            }
            _ => panic!("Unexpected core message"),
        }
    }

    async fn process_input(&mut self, input: PrimaryInput) -> DagResult<()> {
        let PrimaryInput { message, guard } = input;
        let result = self.process_primary_message(message).await;
        drop(guard);
        result
    }

    async fn repair_missing_proposals(&mut self, now: Instant) -> DagResult<()> {
        let tracker = &self.proposal_tracker;
        let requests: Vec<_> = self
            .missing_proposals
            .iter_mut()
            .filter_map(|(digest, repair)| {
                repair
                    .due(tracker, now)
                    .map(|peer| (digest.clone(), repair.round, peer))
            })
            .collect();
        for (digest, round, peer) in requests {
            debug!(
                "Requesting missing proposal {} (round {}) from {:?}",
                digest, round, peer
            );
            let message = PrimaryMessage::CertificatesRequest(vec![digest], self.name);
            if let Some(peer) = peer.filter(|peer| *peer != self.name) {
                let address = self
                    .committee
                    .primary(&peer)
                    .expect("Verified voter outside committee")
                    .primary_to_primary;
                let handler = self
                    .network
                    .send(address, Bytes::from(bincode::serialize(&message)?))
                    .await;
                self.cancel_handlers.entry(round).or_default().push(handler);
            } else {
                self.broadcast_grbc_batch(message, round).await;
            }
        }
        Ok(())
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        loop {
            let repair_deadline = self
                .missing_proposals
                .values()
                .map(|r| r.deadline)
                .min()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            let result = tokio::select! {
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    let mut result = self.process_input(message).await;
                    // Drain a bounded burst while the channel is already hot.
                    // This amortizes scheduler/select overhead without starving
                    // proposer, waiter, or consensus-command channels.
                    for _ in 1..32 {
                        if result.is_err() {
                            break;
                        }
                        match self.rx_primaries.try_recv() {
                            Ok(message) => result = self.process_input(message).await,
                            Err(_) => break,
                        }
                    }
                    result
                },

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                Some(header) = self.rx_header_waiter.recv() => self.process_header(&header).await,

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                Some(certificate) = self.rx_certificate_waiter.recv() => self.process_certificate(certificate).await,

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header).await,
                Some(command) = self.rx_consensus.recv() => self.process_consensus_command(command).await,
                _ = sleep_until(repair_deadline), if !self.missing_proposals.is_empty() => {
                    self.repair_missing_proposals(Instant::now()).await
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
            self.flush_grbc_outboxes().await;

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.last_ready.retain(|k, _| k >= &gc_round);
                self.proposal_headers.retain(|_, h| h.round >= gc_round);
                let proposal_headers = &self.proposal_headers;
                self.validated
                    .retain(|id| proposal_headers.contains_key(id));
                self.missing_proposals
                    .retain(|_, repair| repair.round >= gc_round);
                self.local_votes
                    .retain(|_, votes| votes.first().map_or(false, |v| v.round >= gc_round));
                self.processing.retain(|k, _| k >= &gc_round);
                self.observed_headers
                    .retain(|_, header| header.round >= gc_round);
                let live_headers: HashSet<_> = self.observed_headers.keys().cloned().collect();
                self.votes_aggregators
                    .retain(|digest, _| live_headers.contains(digest));
                self.pending_votes
                    .retain(|_, votes| votes.first().map_or(false, |vote| vote.round >= gc_round));
                self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                self.grade_one_certificates
                    .retain(|_, certificate| certificate.round() >= gc_round);
                self.grbc_certificates.retain(|_, x| x.round() >= gc_round);
                let live: HashSet<_> = self.grbc_certificates.keys().cloned().collect();
                self.grade_two.retain(|k| live.contains(k));
                self.round_advance_pending
                    .retain(|digest, _| live.contains(digest));
                self.weak_edge_candidates
                    .retain(|_, round| *round >= gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
            }
        }
    }
}
