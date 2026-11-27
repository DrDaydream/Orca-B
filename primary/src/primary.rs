// Copyright(C) Facebook, Inc. and its affiliates.
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::{Helper, RepairRequest};
use crate::messages::{
    ConsensusCommand, ConsensusMessage, ConsensusNetworkMessage, GrbcVote,
    GrbcVoteBatch as VoteBatch, Header,
};
use crate::payload_receiver::PayloadReceiver;
use crate::proposal_processor::{
    spawn_proposal_processors, spawn_repaired_headers, ProposalTracker, QueuedProposal,
};
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use crate::vote_sender::spawn_vote_sender;
use async_trait::async_trait;
use bincode::Options;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use futures::stream::{FuturesUnordered, StreamExt as _};
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
#[cfg(feature = "benchmark")]
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "benchmark")]
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[cfg(test)]
#[path = "tests/vote_transport_tests.rs"]
mod vote_transport_tests;

/// The round number.
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    GrbcVote(GrbcVote),
    GrbcVoteBatch(VoteBatch),
    Consensus(ConsensusNetworkMessage),
    LeaderRequest(Round, PublicKey, /* requestor */ PublicKey),
    CertificatesRequest(Vec<Digest>, /* requestor */ PublicKey),
    /// Request saved ECHO/READY votes for a locally available header.
    VotesRequest(Vec<Digest>, /* requestor */ PublicKey),
}

/// The messages sent by the primary to its workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// The primary indicates that the worker need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The primary indicates a round update.
    Cleanup(Round),
    /// Pause or resume local batch production for a proposer round.
    BatchSilent(Round, bool),
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// A locally authored batch is stored and has reached the worker quorum.
    OurBatch(Digest, WorkerId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(Digest, WorkerId),
}

/// Per-primary counters for the dedicated ABA transport, shared only by its
/// sender, ingress handler, and observer. Local ABA self-delivery uses no socket.
#[cfg(feature = "benchmark")]
#[derive(Clone, Default)]
struct AbaByteCounters {
    sent: Arc<AtomicU64>,
    received: Arc<AtomicU64>,
}

#[cfg(feature = "benchmark")]
impl AbaByteCounters {
    fn spawn() -> Self {
        let counters = Self::default();
        let started = Instant::now();
        info!("Benchmark ABA instrumentation version 1");
        // Establish a zero baseline before starting either ABA socket task.
        counters.log_sample(started);
        let observer = counters.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                // Keep sampling during idle periods as well. Rate denominators
                // use the monotonic elapsed delta, never ABA active duration.
                observer.log_sample(started);
            }
        });
        counters
    }

    fn log_sample(&self, started: Instant) {
        // Wall time aligns observations with other logs; only Instant provides
        // the denominator, so a wall-clock correction cannot change the rate.
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(at) => info!(
                "Benchmark ABA bytes at_us {} elapsed_us {} sent {} received {}",
                at.as_micros(),
                started.elapsed().as_micros(),
                self.sent.load(Ordering::Relaxed),
                self.received.load(Ordering::Relaxed),
            ),
            Err(error) => log::warn!("Benchmark ABA timestamp unavailable: {}", error),
        }
    }
}

fn spawn_aba_broadcaster(
    name: PublicKey,
    committee: Committee,
    mut signature_service: SignatureService,
    mut rx_messages: Receiver<Vec<Vec<u8>>>,
    #[cfg(feature = "benchmark")] aba_bytes: AbaByteCounters,
) {
    tokio::spawn(async move {
        let addresses: Vec<_> = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, address)| address.aba_to_aba)
            .collect();
        #[cfg(not(feature = "benchmark"))]
        let mut network = crate::adversary::reliable_sender();
        #[cfg(feature = "benchmark")]
        let mut network = crate::adversary::reliable_sender_with_counter(aba_bytes.sent);
        let mut pending = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(mut messages) = rx_messages.recv() => {
                    // Coalesce independent ABA instances before signing. Other
                    // consensus commands are handled by the separate router.
                    let deadline = sleep(Duration::from_millis(1));
                    tokio::pin!(deadline);
                    loop {
                        tokio::select! {
                            () = &mut deadline => break,
                            next = rx_messages.recv() => match next {
                                Some(mut more) => messages.append(&mut more),
                                None => break,
                            },
                        }
                    }
                    let groups = if crate::adversary::schedule_aba() {
                        crate::adversary::group_aba_messages(messages)
                    } else { vec![messages] };
                    for messages in groups {
                        let payload = bincode::serialize(&messages)
                            .expect("Failed to serialize ABA batch");
                        let message = ConsensusNetworkMessage::new(
                            payload,
                            name,
                            &mut signature_service,
                        ).await;
                        let bytes = bincode::serialize(&message)
                            .expect("Failed to serialize ABA network message");
                        pending.extend(network.broadcast(addresses.clone(), Bytes::from(bytes)).await);
                    }
                }
                Some(_) = pending.next(), if !pending.is_empty() => {}
                else => break,
            }
        }
    });
}

fn spawn_consensus_router(
    mut rx_consensus: Receiver<ConsensusCommand>,
    tx_consensus_commands: Sender<ConsensusCommand>,
    tx_aba_broadcast: Sender<Vec<Vec<u8>>>,
) {
    tokio::spawn(async move {
        let mut spool = crate::aba_spool::AbaSpool::default();
        let mut input_closed = false;
        while !input_closed || !spool.is_empty() {
            tokio::select! {
                permit = tx_aba_broadcast.reserve(), if !spool.is_empty() => {
                    permit.expect("ABA broadcaster stopped").send(spool.pop().await);
                }
                command = rx_consensus.recv(), if !input_closed => match command {
                    Some(ConsensusCommand::AbaBroadcast(batch)) => {
                        if spool.is_empty() {
                            match tx_aba_broadcast.try_send(batch) {
                                Ok(()) => continue,
                                Err(tokio::sync::mpsc::error::TrySendError::Full(batch)) => spool.push(batch).await,
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => panic!("ABA broadcaster stopped"),
                            }
                        } else {
                            spool.push(batch).await;
                        }
                    }
                    Some(command) => tx_consensus_commands.send(command).await.expect("Core stopped"),
                    None => input_closed = true,
                }
            }
        }
    });
}

pub struct Primary;

impl Primary {
    pub fn spawn(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_consensus: Sender<ConsensusMessage>,
        rx_consensus: Receiver<ConsensusCommand>,
    ) {
        crate::adversary::initialize(keypair.name, &committee)
            .expect("Invalid BENCH_ADVERSARY_CONFIG");
        let (tx_others_digests, rx_others_digests) = channel(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        // Keep incoming proposal processing separate from signed control votes.
        let (tx_remote_proposals, rx_remote_proposals) = channel(32);
        let (tx_stored_proposals, rx_stored_proposals) = channel(CHANNEL_CAPACITY);
        let (tx_votes, rx_votes) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_commands, rx_consensus_commands) = channel(CHANNEL_CAPACITY);
        let (tx_aba_broadcast, rx_aba_broadcast) = channel(CHANNEL_CAPACITY);
        let (tx_cleanup, rx_cleanup) = tokio::sync::watch::channel(0);

        spawn_consensus_router(rx_consensus, tx_consensus_commands, tx_aba_broadcast);

        // Write the parameters to the logs.
        parameters.log();

        // Parse the public and secret key of this authority.
        let name = keypair.name;
        let secret = keypair.secret;

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. Core publishes this watermark after applying each committed batch.
        let consensus_round = Arc::new(AtomicU64::new(0));
        let in_flight = ProposalTracker::default();
        #[cfg(feature = "benchmark")]
        let aba_bytes = AbaByteCounters::spawn();

        // Spawn the network receiver listening to messages from the other primaries.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_primary_messages,
                tx_remote_proposals,
                in_flight: in_flight.clone(),
                tx_cert_requests: tx_cert_requests.clone(),
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, address
        );

        // ABA has a dedicated socket and bypasses the GRBC Core input queue.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .aba_to_aba;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            AbaReceiverHandler {
                committee: committee.clone(),
                tx_consensus: tx_consensus.clone(),
                #[cfg(feature = "benchmark")]
                aba_bytes: aba_bytes.clone(),
            },
        );
        info!("Primary {} listening to ABA messages on {}", name, address);

        // Spawn the network receiver listening to messages from our workers.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_our_digests,
                tx_others_digests,
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, address
        );

        let synchronizer = Synchronizer::new(name, store.clone(), tx_sync_headers);

        // The `SignatureService` is used to require signatures on specific digests.
        let signature_service = SignatureService::new(secret);
        spawn_aba_broadcaster(
            name,
            committee.clone(),
            signature_service.clone(),
            rx_aba_broadcast,
            #[cfg(feature = "benchmark")]
            aba_bytes,
        );

        spawn_vote_sender(
            name,
            committee.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            rx_votes,
        );
        spawn_repaired_headers(
            store.clone(),
            rx_headers_loopback,
            tx_stored_proposals.clone(),
        );
        spawn_proposal_processors(
            name,
            committee.clone(),
            store.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            rx_remote_proposals,
            synchronizer.clone(),
            rx_headers,
            tx_stored_proposals,
            in_flight.clone(),
        );

        // Core handles compact validated headers and GRBC votes.
        Core::spawn(
            name,
            committee.clone(),
            store.clone(),
            synchronizer,
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            /* rx_primaries */ rx_primary_messages,
            rx_stored_proposals,
            in_flight,
            tx_votes.clone(),
            tx_cert_requests,
            tx_consensus,
            /* tx_proposer */ tx_parents,
            rx_consensus_commands,
            tx_cleanup,
        );

        // Keeps track of the latest consensus round and allows other tasks to clean up their their internal state
        GarbageCollector::spawn(&name, &committee, rx_cleanup);

        // Only other authors' digests feed the worker availability store.
        PayloadReceiver::spawn(store.clone(), rx_others_digests);

        // Resume a header when all its local worker receipts arrive.
        HeaderWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            consensus_round,
            parameters.gc_depth,
            parameters.sync_retry_delay,
            parameters.sync_retry_nodes,
            /* rx_synchronizer */ rx_sync_headers,
            /* tx_core */ tx_headers_loopback,
        );

        // When the `Core` collects enough parent certificates, the `Proposer` generates a new header with new batch
        // digests from our workers and it back to the `Core`.
        Proposer::spawn(
            name,
            &committee,
            signature_service,
            parameters.header_size,
            parameters.max_header_delay,
            /* rx_core */ rx_parents,
            /* rx_workers */ rx_our_digests,
            /* tx_core */ tx_headers,
        );

        // The `Helper` is dedicated to reply to certificates requests from other primaries.
        Helper::spawn(committee.clone(), store, rx_cert_requests, tx_votes);

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            name,
            committee
                .primary(&name)
                .expect("Our public key or worker id is not in the committee")
                .primary_to_primary
                .ip()
        );
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_remote_proposals: Sender<QueuedProposal>,
    in_flight: ProposalTracker,
    tx_cert_requests: Sender<RepairRequest>,
}

/// Dedicated ABA ingress. Authentication and decoding happen outside Core;
/// consensus remains the single ordered state mutator.
#[derive(Clone)]
struct AbaReceiverHandler {
    committee: Committee,
    tx_consensus: Sender<ConsensusMessage>,
    #[cfg(feature = "benchmark")]
    aba_bytes: AbaByteCounters,
}

#[async_trait]
impl MessageHandler for AbaReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // The receiver has decoded one complete frame and removed its default
        // four-byte length prefix. Count the signed envelope before validation,
        // including complete invalid frames and duplicate/retried deliveries.
        #[cfg(feature = "benchmark")]
        self.aba_bytes
            .received
            .fetch_add(serialized.len() as u64 + 4, Ordering::Relaxed);
        let message: ConsensusNetworkMessage =
            bincode::deserialize(&serialized).map_err(DagError::SerializationError)?;
        message.verify(&self.committee)?;
        let batch = bincode::deserialize::<Vec<Vec<u8>>>(&message.payload)
            .map_err(DagError::SerializationError)?;

        let _ = writer.send(Bytes::from("Ack")).await;
        self.tx_consensus
            .send(ConsensusMessage::AbaBatch(message.author, batch))
            .await
            .expect("Failed to deliver ABA batch");
        Ok(())
    }
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Large frames decode away from runtime threads. Independent TCP
        // connections and queues let votes bypass a full bulk proposal queue.
        let message: PrimaryMessage = if serialized.len() > 64 * 1024 {
            tokio::task::spawn_blocking(move || decode_frame::<PrimaryMessage>(&serialized))
                .await??
        } else {
            decode_frame(&serialized)?
        };
        match message {
            PrimaryMessage::Header(header) => {
                // Register before waiting for space in the bounded bulk queue.
                let guard = self.in_flight.track(&header);
                self.tx_remote_proposals
                    .send((header, guard))
                    .await
                    .expect("Failed to enqueue remote proposal");
            }
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send(RepairRequest::Proposals(missing, requestor))
                .await
                .expect("Failed to send primary message"),
            PrimaryMessage::VotesRequest(missing, requestor) => self
                .tx_cert_requests
                .send(RepairRequest::Votes(missing, requestor))
                .await
                .expect("Failed to send vote repair request"),
            request => self
                .tx_primary_messages
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}

// Both digest-only interfaces must consume the complete frame. A legacy
// inline-batch message must not be accepted as a prefix-compatible header.
pub(crate) fn decode_frame<T: serde::de::DeserializeOwned>(frame: &[u8]) -> bincode::Result<T> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(frame.len() as u64)
        .reject_trailing_bytes()
        .deserialize(frame)
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<(Digest, WorkerId)>,
    tx_others_digests: Sender<(Digest, WorkerId)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match decode_frame(&serialized)? {
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => self
                .tx_our_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => self
                .tx_others_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
        }
        Ok(())
    }
}
