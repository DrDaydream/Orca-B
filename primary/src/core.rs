// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, GradeOneVotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{
    Certificate, ConsensusCommand, ConsensusMessage, GradeOneVote, GradeOneVoteBatch, Header,
};
use crate::primary::{PrimaryMessage, Round};
use crate::proposer::ProposerMessage;
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature, SignatureService};
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

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
    rx_primaries: Receiver<PrimaryMessage>,
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
    votes_aggregators: HashMap<Digest, GradeOneVotesAggregator>,
    observed_headers: HashMap<Digest, Header>,
    pending_votes: HashMap<Digest, Vec<GradeOneVote>>,
    vote_outbox: Vec<GradeOneVote>,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// One-vote certificates delivered locally at GRBC grade 1.
    grade_one_certificates: HashMap<Digest, Certificate>,
    /// Quorum certificates delivered at GRBC grade 2.
    grbc_certificates: HashMap<Digest, Certificate>,
    /// Certificates carrying a verified grade-2 proof.
    grade_two: HashSet<Digest>,
    round_advance_pending: HashMap<Digest, Certificate>,
    /// Grade-2 blocks not yet referenced by a weak edge.
    weak_edge_candidates: HashMap<Digest, Round>,
    /// A network sender to send the batches to the other workers.
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
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_consensus: Sender<ConsensusMessage>,
        tx_proposer: Sender<ProposerMessage>,
        rx_consensus: Receiver<ConsensusCommand>,
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
                pending_votes: HashMap::new(),
                vote_outbox: Vec::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                grade_one_certificates: HashMap::new(),
                grbc_certificates: HashMap::new(),
                grade_two: HashSet::new(),
                round_advance_pending: HashMap::new(),
                weak_edge_candidates: HashMap::new(),
                network: ReliableSender::new(),
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
                .expect("Failed to send observed header to consensus");
        }
        if let Some(votes) = self.pending_votes.remove(&header.id) {
            for vote in votes {
                self.process_grade_one_vote(vote)
                    .await
                    .expect("Invalid pending vote");
            }
        }
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
        let certificate = self
            .grbc_certificates
            .values()
            .find(|c| c.round() == round && c.origin() == leader)
            .cloned();
        if let Some(certificate) = certificate {
            if let Ok(address) = self.committee.primary(&requestor) {
                let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate))
                    .expect("Failed to serialize leader response");
                let handler = self
                    .network
                    .send(address.primary_to_primary, Bytes::from(bytes))
                    .await;
                self.cancel_handlers.entry(round).or_default().push(handler);
            }
        }
        Ok(())
    }

    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        // Retain the latest local header for ABA broadcast round tagging.
        self.current_header = header.clone();

        // Broadcast the new header in a reliable manner.
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
            .expect("Failed to serialize our own header");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(header.round)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Process the header.
        self.process_header(&header).await
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        debug!("Processing {:?}", header);
        self.observe_header(header).await;
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
        for x in referenced
            .iter()
            .filter(|x| header.parents.contains(&x.digest()))
        {
            ensure!(
                x.round() + 1 == header.round,
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

        // Ensure we have the payload. If we don't, the synchronizer will ask our workers to get it, and then
        // reschedule processing of this header once we have it.
        if self.synchronizer.missing_payload(header).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // Store the header.
        let bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), bytes).await;

        // Check if we can vote for this header.
        if self
            .last_voted
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.author)
        {
            // Ordinary GRBC votes are all-to-all, allowing every node to form
            // the same certificate; a vote may arrive before its header.
            let vote = GradeOneVote::new(header, &self.name, &mut self.signature_service).await;
            debug!("Created {:?}", vote);
            self.vote_outbox.push(vote.clone());
            self.process_grade_one_vote(vote).await?;
        }
        self.retry_round_advance_pending().await?;
        Ok(())
    }

    #[async_recursion]
    async fn process_grade_one_vote(&mut self, vote: GradeOneVote) -> DagResult<()> {
        debug!("Processing {:?}", vote);

        let header = match self.observed_headers.get(&vote.id).cloned() {
            Some(header) => header,
            None => {
                let pending = self.pending_votes.entry(vote.id.clone()).or_default();
                if !pending
                    .iter()
                    .any(|candidate| candidate.author == vote.author)
                {
                    pending.push(vote);
                }
                return Ok(());
            }
        };

        ensure!(
            vote.round == header.round && vote.origin == header.author,
            DagError::UnexpectedGradeOneVote(vote.id.clone())
        );
        self.deliver_grade_one(&header, vote.author, vote.signature.clone())
            .await;

        // A quorum of grade-1 votes is the grade-2 certificate.
        if let Some(certificate) = self
            .votes_aggregators
            .entry(header.id.clone())
            .or_insert_with(GradeOneVotesAggregator::new)
            .append(vote, &self.committee, &header)?
        {
            debug!("Assembled {:?}", certificate);

            // Broadcast the certificate.
            let addresses = self
                .committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                .expect("Failed to serialize our own certificate");
            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
            self.cancel_handlers
                .entry(certificate.round())
                .or_insert_with(Vec::new)
                .extend(handlers);

            // Process the new certificate.
            self.process_certificate(certificate)
                .await
                .expect("Failed to process valid certificate");
        }
        Ok(())
    }

    async fn deliver_grade_one(
        &mut self,
        header: &Header,
        author: PublicKey,
        signature: Signature,
    ) {
        let certificate = Certificate {
            header: header.clone(),
            votes: vec![(author, signature)],
        };
        let digest = certificate.digest();
        if self
            .grade_one_certificates
            .insert(digest, certificate.clone())
            .is_none()
        {
            self.tx_consensus
                .send(ConsensusMessage::GradeOne(certificate))
                .await
                .expect("Failed to send grade-1 block to consensus");
        }
    }

    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!("Processing {:?}", certificate);

        // Process the header embedded in the certificate if we haven't already voted for it (if we already
        // voted, it means we already processed it). Since this header got certified, we are sure that all
        // the data it refers to (ie. its payload and its parents) are available. We can thus continue the
        // processing of the certificate even if we don't have them in store right now.
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or_else(|| false, |x| x.contains(&certificate.header.id))
        {
            // This function may still throw an error if the storage fails.
            self.process_header(&certificate.header).await?;
        }

        // Ensure we have all the ancestors of this certificate yet. If we don't, the synchronizer will gather
        // them and trigger re-processing of this certificate.
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            debug!(
                "Processing of {:?} suspended: missing ancestors",
                certificate
            );
            return Ok(());
        }

        let digest = certificate.digest();

        if self.grade_two.contains(&digest) {
            self.try_advance_grade_two(certificate).await?;
            self.retry_round_advance_pending().await?;
            return Ok(());
        }

        if !self.grade_one_certificates.contains_key(&digest) {
            if let Some((author, signature)) = certificate.votes.first() {
                self.deliver_grade_one(&certificate.header, *author, signature.clone())
                    .await;
            }
        }

        // The quorum certificate is the Grade-2 proof.
        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(digest.to_vec(), bytes).await;
        self.grbc_certificates
            .insert(digest.clone(), certificate.clone());
        self.deliver_grade_two(certificate).await?;
        self.retry_round_advance_pending().await?;
        Ok(())
    }

    async fn flush_grbc_outboxes(&mut self) {
        if !self.vote_outbox.is_empty() {
            let mut by_round = BTreeMap::<Round, Vec<GradeOneVote>>::new();
            for vote in std::mem::take(&mut self.vote_outbox) {
                by_round.entry(vote.round).or_default().push(vote);
            }
            for (round, votes) in by_round {
                let batch = GradeOneVoteBatch::from_votes(votes)
                    .expect("local GRBC batch mixed voters or rounds");
                self.broadcast_grbc_batch(PrimaryMessage::GradeOneVoteBatch(batch), round)
                    .await;
            }
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
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
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

        // Verify the header's signature.
        header.verify(&self.committee)?;

        // TODO [issue #3]: Prevent bad nodes from sending junk headers with high round numbers.

        Ok(())
    }

    fn sanitize_grade_one_vote(&mut self, vote: &GradeOneVote) -> DagResult<()> {
        ensure!(
            self.gc_round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );
        ensure!(
            self.committee.stake(&vote.origin) > 0,
            DagError::UnknownAuthority(vote.origin)
        );

        // Verify the vote.
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.round(),
            DagError::TooOld(certificate.digest(), certificate.round())
        );

        // Verify the certificate (and the embedded header).
        certificate.verify(&self.committee).map_err(DagError::from)
    }

    async fn process_primary_message(&mut self, message: PrimaryMessage) -> DagResult<()> {
        match message {
            PrimaryMessage::Header(header) => {
                self.sanitize_header(&header)?;
                self.process_header(&header).await
            }
            PrimaryMessage::GradeOneVote(vote) => {
                self.sanitize_grade_one_vote(&vote)?;
                self.process_grade_one_vote(vote).await
            }
            PrimaryMessage::GradeOneVoteBatch(batch) => {
                for vote in batch.into_votes() {
                    self.sanitize_grade_one_vote(&vote)?;
                    self.process_grade_one_vote(vote).await?;
                }
                Ok(())
            }
            PrimaryMessage::Certificate(certificate) => {
                self.sanitize_certificate(&certificate)?;
                self.process_certificate(certificate).await
            }
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

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    let mut result = self.process_primary_message(message).await;
                    // Drain a bounded burst while the channel is already hot.
                    // This amortizes scheduler/select overhead without starving
                    // proposer, waiter, or consensus-command channels.
                    for _ in 1..32 {
                        if result.is_err() {
                            break;
                        }
                        match self.rx_primaries.try_recv() {
                            Ok(message) => result = self.process_primary_message(message).await,
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
