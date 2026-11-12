// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::control_broadcaster::{spawn_control_broadcaster, ControlMessage};
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::messages::{
    batch_digest, batch_key, ConsensusCommand, ConsensusMessage, ConsensusNetworkMessage,
    ControlFrame, GrbcVote, GrbcVoteBatch as VoteBatch, Proposal, MAX_PROPOSAL_BYTES,
};
use crate::payload_receiver::PayloadReceiver;
use crate::proposal_repair::{PrimaryInput, ProposalTracker};
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The round number.
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Proposal(Proposal),
    GrbcVote(GrbcVote),
    GrbcVoteBatch(VoteBatch),
    LeaderRequest(Round, PublicKey, /* requestor */ PublicKey),
    CertificatesRequest(Vec<Digest>, /* requestor */ PublicKey),
    Control(ControlFrame),
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
    OurBatch(Vec<u8>, WorkerId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(Digest, WorkerId),
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
    ) -> Sender<ControlMessage> {
        let (tx_others_digests, rx_others_digests) = channel(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let proposal_tracker = ProposalTracker::default();
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_commands, rx_consensus_commands) = channel(CHANNEL_CAPACITY);
        let (tx_control, rx_control) = channel(CHANNEL_CAPACITY);
        let name = keypair.name;
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
                tx_consensus: tx_consensus.clone(),
                proposal_tracker: proposal_tracker.clone(),
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
                store: store.clone(),
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
        spawn_control_broadcaster(
            name,
            committee.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            rx_control,
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
            proposal_tracker,
            /* rx_header_waiter */ rx_headers_loopback,
            /* rx_certificate_waiter */ rx_certificates_loopback,
            /* rx_proposer */ rx_headers,
            tx_consensus,
            /* tx_proposer */ tx_parents,
            rx_consensus_commands,
            tx_control.clone(),
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
        // Bypass cleanup/repair feedback when consensus emits ABA messages.
        tx_control
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryInput>,
    tx_consensus: Sender<ConsensusMessage>,
    proposal_tracker: ProposalTracker,
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,
}

/// Unsigned experimental ABA ingress, bypassing the GRBC Core queue.
#[derive(Clone)]
struct AbaReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
}

#[async_trait]
impl MessageHandler for AbaReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let message: ConsensusNetworkMessage =
            bincode::deserialize(&serialized).map_err(DagError::SerializationError)?;
        let _ = writer.send(Bytes::from("Ack")).await;
        self.tx_consensus
            .send(ConsensusMessage::AbaBatch(message.author, message.messages))
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

        self.route(bincode::deserialize(&serialized).map_err(DagError::SerializationError)?)
            .await;
        Ok(())
    }
}

impl PrimaryReceiverHandler {
    async fn route(&self, message: PrimaryMessage) {
        match message {
            PrimaryMessage::Control(frame) => {
                // Deliver ABA first, without waiting for proposal processing or
                // a free Core slot. Core independently verifies every GRBC vote.
                if let Some(aba) = frame.aba {
                    self.tx_consensus
                        .send(ConsensusMessage::AbaBatch(aba.author, aba.messages))
                        .await
                        .expect("Failed to deliver piggybacked ABA");
                }
                for votes in frame.grbc {
                    self.forward(PrimaryMessage::GrbcVoteBatch(votes)).await;
                }
            }
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
            request => self.forward(request).await,
        }
    }

    async fn forward(&self, message: PrimaryMessage) {
        self.tx_primary_messages
            .send(PrimaryInput::new(message, &self.proposal_tracker))
            .await
            .expect("Failed to send primary message");
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    store: Store,
    tx_our_digests: Sender<(Digest, WorkerId, usize)>,
    tx_others_digests: Sender<(Digest, WorkerId)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            WorkerPrimaryMessage::OurBatch(batch, worker_id) => {
                if batch.len() > MAX_PROPOSAL_BYTES {
                    return Err("batch exceeds complete-proposal payload limit".into());
                }
                let digest = batch_digest(&batch);
                let size = batch.len();
                let mut store = self.store.clone();
                store
                    .write_batch_if_absent(std::iter::once((batch_key(&digest), batch)).collect())
                    .await?;
                self.tx_our_digests
                    .send((digest, worker_id, size))
                    .await
                    .expect("Failed to send local batch");
            }
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => {
                self.tx_others_digests
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send worker digest");
            }
        }
        let _ = writer.send(Bytes::from("Ack")).await;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/primary_receiver_tests.rs"]
mod tests;
