// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::messages::{
    Certificate, ConsensusCommand, ConsensusMessage, ConsensusNetworkMessage, GradeOneVote, Header,
};
use crate::payload_receiver::PayloadReceiver;
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use futures::stream::{FuturesUnordered, StreamExt as _};
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, ReliableSender, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The round number.
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    GradeOneVote(GradeOneVote),
    GradeOneVoteBatch(Vec<GradeOneVote>),
    Certificate(Certificate),
    Consensus(ConsensusNetworkMessage),
    LeaderRequest(Round, PublicKey, /* requestor */ PublicKey),
    CertificatesRequest(Vec<Digest>, /* requestor */ PublicKey),
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
    /// The worker indicates it sealed a new batch.
    OurBatch(Digest, WorkerId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(Digest, WorkerId),
}

fn spawn_aba_broadcaster(
    name: PublicKey,
    committee: Committee,
    mut signature_service: SignatureService,
    mut rx_messages: Receiver<Vec<Vec<u8>>>,
) {
    tokio::spawn(async move {
        let addresses: Vec<_> = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, address)| address.aba_to_aba)
            .collect();
        let mut network = ReliableSender::new();
        let mut pending = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(messages) = rx_messages.recv() => {
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
                Some(_) = pending.next(), if !pending.is_empty() => {}
                else => break,
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
        mut rx_consensus: Receiver<ConsensusCommand>,
    ) {
        let (tx_others_digests, rx_others_digests) = channel(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_commands, rx_consensus_commands) = channel(CHANNEL_CAPACITY);
        let (tx_aba_broadcast, rx_aba_broadcast) = channel(CHANNEL_CAPACITY);
        let (tx_cleanup, rx_cleanup) = channel(CHANNEL_CAPACITY);

        tokio::spawn(async move {
            while let Some(command) = rx_consensus.recv().await {
                match command {
                    ConsensusCommand::Cleanup(certificate) => tx_cleanup
                        .send(certificate)
                        .await
                        .expect("Failed to send cleanup"),
                    ConsensusCommand::CleanupBatch(certificates) => {
                        for certificate in certificates {
                            tx_cleanup
                                .send(certificate)
                                .await
                                .expect("Failed to send cleanup batch");
                        }
                    }
                    ConsensusCommand::AbaBroadcast(mut messages) => {
                        // Coalesce bursts from independent ABA instances. The
                        // Core signs the resulting batch once.
                        let deadline = sleep(Duration::from_millis(1));
                        tokio::pin!(deadline);
                        loop {
                            tokio::select! {
                                () = &mut deadline => break,
                                next = rx_consensus.recv() => match next {
                                    Some(ConsensusCommand::AbaBroadcast(mut more)) => {
                                        messages.append(&mut more);
                                    }
                                    Some(ConsensusCommand::Cleanup(certificate)) => {
                                        tx_cleanup.send(certificate).await.expect("Failed to send cleanup");
                                    }
                                    Some(ConsensusCommand::CleanupBatch(certificates)) => {
                                        for certificate in certificates {
                                            tx_cleanup.send(certificate).await.expect("Failed to send cleanup batch");
                                        }
                                    }
                                    Some(command) => {
                                        tx_consensus_commands.send(command).await.expect("Failed to send consensus command");
                                    }
                                    None => break,
                                }
                            }
                        }
                        tx_aba_broadcast
                            .send(messages)
                            .await
                            .expect("Failed to send ABA batch");
                    }
                    command => tx_consensus_commands
                        .send(command)
                        .await
                        .expect("Failed to send consensus command"),
                }
            }
        });

        // Write the parameters to the logs.
        parameters.log();

        // Parse the public and secret key of this authority.
        let name = keypair.name;
        let secret = keypair.secret;

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. The only tasks that write into this variable is `GarbageCollector`.
        let consensus_round = Arc::new(AtomicU64::new(0));

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
                tx_cert_requests,
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

        // The `Synchronizer` provides auxiliary methods helping to `Core` to sync.
        let synchronizer = Synchronizer::new(
            name,
            &committee,
            store.clone(),
            /* tx_header_waiter */ tx_sync_headers,
            /* tx_certificate_waiter */ tx_sync_certificates,
        );

        // The `SignatureService` is used to require signatures on specific digests.
        let signature_service = SignatureService::new(secret);
        spawn_aba_broadcaster(
            name,
            committee.clone(),
            signature_service.clone(),
            rx_aba_broadcast,
        );

        // The `Core` receives and handles headers, votes, and certificates from the other primaries.
        Core::spawn(
            name,
            committee.clone(),
            store.clone(),
            synchronizer,
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            /* rx_primaries */ rx_primary_messages,
            /* rx_header_waiter */ rx_headers_loopback,
            /* rx_certificate_waiter */ rx_certificates_loopback,
            /* rx_proposer */ rx_headers,
            tx_consensus,
            /* tx_proposer */ tx_parents,
            rx_consensus_commands,
        );

        // Keeps track of the latest consensus round and allows other tasks to clean up their their internal state
        GarbageCollector::spawn(&name, &committee, consensus_round.clone(), rx_cleanup);

        // Receives batch digests from other workers. They are only used to validate headers.
        PayloadReceiver::spawn(store.clone(), /* rx_workers */ rx_others_digests);

        // Whenever the `Synchronizer` does not manage to validate a header due to missing parent certificates of
        // batch digests, it commands the `HeaderWaiter` to synchronizer with other nodes, wait for their reply, and
        // re-schedule execution of the header once we have all missing data.
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

        // The `CertificateWaiter` waits to receive all the ancestors of a certificate before looping it back to the
        // `Core` for further processing.
        CertificateWaiter::spawn(
            store.clone(),
            /* rx_synchronizer */ rx_sync_certificates,
            /* tx_core */ tx_certificates_loopback,
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
        Helper::spawn(committee.clone(), store, rx_cert_requests);

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
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,
}

/// Dedicated ABA ingress. Authentication and decoding happen outside Core;
/// consensus remains the single ordered state mutator.
#[derive(Clone)]
struct AbaReceiverHandler {
    committee: Committee,
    tx_consensus: Sender<ConsensusMessage>,
}

#[async_trait]
impl MessageHandler for AbaReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
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

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
            request => self
                .tx_primary_messages
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
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
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
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
