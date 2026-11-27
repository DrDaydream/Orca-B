// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::Processor;
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bincode::Options;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{error, info, warn};
use network::{MessageHandler, Receiver, Writer};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use std::error::Error;
#[cfg(feature = "benchmark")]
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::watch;

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The primary round number.
// TODO: Move to the primary.
pub type Round = u64;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// The message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

pub struct Worker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        #[cfg(feature = "benchmark")]
        info!("Benchmark latency instrumentation version 1");

        // Define a worker instance.
        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        let (tx_batch_silent, rx_batch_silent) = watch::channel((Round::default(), false));
        worker.handle_primary_messages(tx_batch_silent, tx_primary.clone());
        worker.handle_clients_transactions(tx_primary.clone(), rx_batch_silent);
        worker.handle_workers_messages(tx_primary);

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
        );

        // NOTE: This log entry is used to compute performance.
        info!(
            "Worker {} successfully booted on {}",
            id,
            worker
                .committee
                .worker(&worker.name, &worker.id)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from our primary.
    fn handle_primary_messages(
        &self,
        tx_batch_silent: watch::Sender<(Round, bool)>,
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from our primary.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_synchronizer,
                tx_batch_silent,
            },
        );

        // The `Synchronizer` is responsible to keep the worker in sync with the others. It handles the commands
        // it receives from the primary (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_synchronizer,
            tx_primary,
        );

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
        rx_batch_silent: watch::Receiver<(Round, bool)>,
    ) {
        let (tx_quorum, rx_quorum) = channel(CHANNEL_CAPACITY);
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        // Workers disseminate batches while the primary advances DAG rounds.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            rx_batch_maker,
            rx_batch_silent,
            tx_quorum,
            self.committee
                .others_workers(&self.name, &self.id)
                .into_iter()
                .map(|(name, address)| (name, address.worker_to_worker))
                .collect(),
        );
        QuorumWaiter::spawn(
            self.committee.clone(),
            self.committee.stake(&self.name),
            rx_quorum,
            tx_processor,
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other workers.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_helper,
                store: self.store.clone(),
                id: self.id,
                tx_primary,
            },
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        #[cfg(feature = "benchmark")]
        if message.len() >= 9 && message[0] == 0 {
            // Capture ingress before decoding and before the batching channel can block.
            let ingress_at = SystemTime::now().duration_since(UNIX_EPOCH);
            let mut sample_id_bytes = [0u8; 8];
            sample_id_bytes.copy_from_slice(&message[1..9]);
            let sample_id = u64::from_be_bytes(sample_id_bytes);
            match ingress_at {
                Ok(ingress_at) => info!(
                    "Benchmark sample ingress id {} at_us {}",
                    sample_id,
                    ingress_at.as_micros()
                ),
                Err(error) => warn!(
                    "Benchmark sample ingress timestamp unavailable id {}: {}",
                    sample_id, error
                ),
            }
        }

        // Send the transaction to the batch maker.
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    store: Store,
    id: WorkerId,
    tx_primary: Sender<SerializedBatchDigestMessage>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        match bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(serialized.len() as u64)
            .reject_trailing_bytes()
            .deserialize(&serialized)?
        {
            WorkerMessage::Batch(_) => {
                let mut store = self.store.clone();
                Processor::process(
                    &mut store,
                    self.id,
                    serialized.to_vec(),
                    false,
                    &self.tx_primary,
                )
                .await?;
            }
            WorkerMessage::BatchRequest(missing, requestor) => {
                self.tx_helper
                    .send((missing, requestor))
                    .await
                    .expect("Failed to send batch request");
            }
        }
        writer.send(Bytes::from_static(b"Ack")).await?;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
    tx_batch_silent: watch::Sender<(Round, bool)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize the message and send it to the synchronizer.
        match bincode::deserialize(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(PrimaryWorkerMessage::BatchSilent(round, silent)) => {
                let is_newer = round >= self.tx_batch_silent.borrow().0;
                if is_newer {
                    let _ = self.tx_batch_silent.send((round, silent));
                }
            }
            Ok(message) => self
                .tx_synchronizer
                .send(message)
                .await
                .expect("Failed to send primary command"),
        }
        Ok(())
    }
}
