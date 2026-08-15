// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, GradeVotesAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{
    Certificate, ConsensusCommand, ConsensusMessage, ConsensusNetworkMessage, GradeVote,
    GradedCertificate, Header, Vote,
};
use crate::primary::{PrimaryMessage, Round};
use crate::proposer::ProposerMessage;
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
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
    /// Aggregates votes into a certificate.
    votes_aggregator: VotesAggregator,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// Certificates delivered at GRBC grade 1, indexed by their digest.
    grbc_certificates: HashMap<Digest, Certificate>,
    /// Grade-1 acknowledgements collected by the certificate origin.
    grade_aggregators: HashMap<Digest, GradeVotesAggregator>,
    /// Certificates for which this primary already emitted a grade vote.
    grade_voted: HashSet<Digest>,
    /// Certificates carrying a verified grade-2 proof.
    grade_two: HashSet<Digest>,
    graded_certificates: HashMap<Digest, GradedCertificate>,
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
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                grbc_certificates: HashMap::new(),
                grade_aggregators: HashMap::new(),
                grade_voted: HashSet::new(),
                grade_two: HashSet::new(),
                graded_certificates: HashMap::new(),
                weak_edge_candidates: HashMap::new(),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
            }
            .run()
            .await;
        });
    }

    async fn process_consensus_command(&mut self, command: ConsensusCommand) -> DagResult<()> {
        match command {
            ConsensusCommand::Cleanup(_) => unreachable!(),
            ConsensusCommand::AbaBroadcast(messages) => {
                let payload = bincode::serialize(&messages).expect("Failed to serialize ABA batch");
                let message =
                    ConsensusNetworkMessage::new(payload, self.name, &mut self.signature_service)
                        .await;
                let addresses = self
                    .committee
                    .others_primaries(&self.name)
                    .iter()
                    .map(|(_, x)| x.primary_to_primary)
                    .collect();
                let bytes = bincode::serialize(&PrimaryMessage::Consensus(message))
                    .expect("Failed to serialize ABA message");
                let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(self.current_header.round)
                    .or_default()
                    .extend(handlers);
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
            let digest = certificate.digest();
            let response = self
                .graded_certificates
                .get(&digest)
                .cloned()
                .map(PrimaryMessage::GradedCertificate)
                .unwrap_or_else(|| PrimaryMessage::Certificate(certificate));
            if let Ok(address) = self.committee.primary(&requestor) {
                let bytes =
                    bincode::serialize(&response).expect("Failed to serialize leader response");
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
        // Reset the votes aggregator.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();

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
            // Make a vote and send it to the header's creator.
            let vote = Vote::new(header, &self.name, &mut self.signature_service).await;
            debug!("Created {:?}", vote);
            if vote.origin == self.name {
                self.process_vote(vote)
                    .await
                    .expect("Failed to process our own vote");
            } else {
                let address = self
                    .committee
                    .primary(&header.author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(header.round)
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing {:?}", vote);

        // Add it to the votes' aggregator and try to make a new certificate.
        if let Some(certificate) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?
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

        // A looped-back or retransmitted certificate must not be delivered twice.
        if self.grbc_certificates.contains_key(&digest) {
            return Ok(());
        }

        // Store the certificate. This is the grade-1 delivery point of GRBC.
        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(digest.to_vec(), bytes).await;
        self.grbc_certificates
            .insert(digest.clone(), certificate.clone());

        // Grade 1 enters VDag only; it is not inserted into Tusk's Dag yet.
        let id = certificate.header.id.clone();
        if let Err(e) = self
            .tx_consensus
            .send(ConsensusMessage::GradeOne(certificate.clone()))
            .await
        {
            warn!(
                "Failed to deliver certificate {} to the consensus: {}",
                id, e
            );
        }

        // Acknowledge grade-1 delivery. A quorum of these signed acknowledgements
        // forms a portable grade-2 proof at the certificate origin.
        if self.grade_voted.insert(digest) {
            let vote = GradeVote::new(&certificate, &self.name, &mut self.signature_service).await;
            if certificate.origin() == self.name {
                self.process_grade_vote(vote).await?;
            } else {
                let address = match self.committee.primary(&certificate.origin()) {
                    Ok(authority) => authority.primary_to_primary,
                    Err(error) => {
                        // This is unreachable for a certificate accepted through the network,
                        // but keeps internal/test callers from crashing the primary task.
                        warn!("Skipping GRBC grade vote for unknown origin: {}", error);
                        return Ok(());
                    }
                };
                let bytes = bincode::serialize(&PrimaryMessage::GradeVote(vote))
                    .expect("Failed to serialize grade vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(certificate.round())
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
        }
        Ok(())
    }

    async fn process_grade_vote(&mut self, vote: GradeVote) -> DagResult<()> {
        let certificate = self
            .grbc_certificates
            .get(&vote.id)
            .cloned()
            .ok_or_else(|| DagError::UnexpectedGradeVote(vote.id.clone()))?;
        let digest = vote.id.clone();
        let proof = self
            .grade_aggregators
            .entry(digest.clone())
            .or_insert_with(GradeVotesAggregator::new)
            .append(vote, &self.committee, &certificate)?;

        if let Some(proof) = proof {
            debug!("Assembled GRBC grade-2 proof for {:?}", certificate);
            let addresses = self
                .committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::GradedCertificate(proof.clone()))
                .expect("Failed to serialize grade-2 proof");
            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
            self.cancel_handlers
                .entry(certificate.round())
                .or_insert_with(Vec::new)
                .extend(handlers);
            self.process_graded_certificate(proof).await?;
        }
        Ok(())
    }

    async fn process_graded_certificate(&mut self, proof: GradedCertificate) -> DagResult<()> {
        let digest = proof.certificate.digest();
        self.graded_certificates
            .insert(digest.clone(), proof.clone());
        if self.grade_two.insert(digest.clone()) {
            debug!("GRBC grade 2 delivered for {:?}", proof.certificate);
            self.tx_consensus
                .send(ConsensusMessage::GradeTwo(proof.certificate.clone()))
                .await
                .expect("Failed to send grade-2 certificate to consensus");
            self.weak_edge_candidates
                .insert(digest, proof.certificate.round());

            // Only grade-2 blocks are strong parents and can advance the round.
            if let Some(parents) = self
                .certificates_aggregators
                .entry(proof.certificate.round())
                .or_insert_with(|| Box::new(CertificatesAggregator::new()))
                .append(proof.certificate.clone(), &self.committee)?
            {
                let round = proof.certificate.round();
                let virtual_edges = self
                    .grbc_certificates
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
                // Strong parents and emitted weak edges have now been
                // referenced and must not be emitted again as weak edges.
                for digest in &parents {
                    self.weak_edge_candidates.remove(digest);
                }
                for digest in &weak_edges {
                    self.weak_edge_candidates.remove(digest);
                }
                self.tx_proposer
                    .send((parents, weak_edges, virtual_edges, round))
                    .await
                    .expect("Failed to send GRBC edges to proposer");
            }
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

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        ensure!(
            self.current_header.round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );

        // Ensure we receive a vote on the expected header.
        ensure!(
            vote.id == self.current_header.id
                && vote.origin == self.current_header.author
                && vote.round == self.current_header.round,
            DagError::UnexpectedVote(vote.id.clone())
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

    fn sanitize_grade_vote(&mut self, vote: &GradeVote) -> DagResult<()> {
        ensure!(
            self.gc_round <= vote.round,
            DagError::TooOld(vote.id.clone(), vote.round)
        );
        ensure!(
            vote.origin == self.name,
            DagError::UnexpectedGradeVote(vote.id.clone())
        );
        vote.verify(&self.committee)
    }

    fn sanitize_graded_certificate(&mut self, proof: &GradedCertificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= proof.certificate.round(),
            DagError::TooOld(proof.certificate.digest(), proof.certificate.round())
        );
        proof.verify(&self.committee)
    }

    async fn process_primary_message(&mut self, message: PrimaryMessage) -> DagResult<()> {
        match message {
            PrimaryMessage::Header(header) => {
                self.sanitize_header(&header)?;
                self.process_header(&header).await
            }
            PrimaryMessage::Vote(vote) => {
                self.sanitize_vote(&vote)?;
                self.process_vote(vote).await
            }
            PrimaryMessage::Certificate(certificate) => {
                self.sanitize_certificate(&certificate)?;
                self.process_certificate(certificate).await
            }
            PrimaryMessage::GradeVote(vote) => {
                self.sanitize_grade_vote(&vote)?;
                self.process_grade_vote(vote).await
            }
            PrimaryMessage::GradedCertificate(proof) => {
                self.sanitize_graded_certificate(&proof)?;
                self.process_graded_certificate(proof).await
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

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.processing.retain(|k, _| k >= &gc_round);
                self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                self.grbc_certificates.retain(|_, x| x.round() >= gc_round);
                let live: HashSet<_> = self.grbc_certificates.keys().cloned().collect();
                self.grade_aggregators.retain(|k, _| live.contains(k));
                self.grade_voted.retain(|k| live.contains(k));
                self.grade_two.retain(|k| live.contains(k));
                self.graded_certificates.retain(|k, _| live.contains(k));
                self.weak_edge_candidates
                    .retain(|_, round| *round >= gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
            }
        }
    }
}
