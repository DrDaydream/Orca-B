// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use crate::worker::WorkerMessage;
#[cfg(feature = "benchmark")]
use crypto::Digest;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Current proposer round and whether local batch production is paused.
    rx_batch_silent: watch::Receiver<(u64, bool)>,
    /// Output channel to deliver sealed batches to the local `Processor`.
    tx_message: Sender<SerializedBatchMessage>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        rx_batch_silent: watch::Receiver<(u64, bool)>,
        tx_message: Sender<SerializedBatchMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size: batch_size.min(primary::MAX_PROPOSAL_BYTES - 1024),
                max_batch_delay,
                rx_transaction,
                rx_batch_silent,
                tx_message,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            if self.rx_batch_silent.borrow().1 {
                if self.rx_batch_silent.changed().await.is_err() {
                    return;
                }
                if !self.rx_batch_silent.borrow().1 {
                    timer
                        .as_mut()
                        .reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
                continue;
            }

            tokio::select! {
                biased;

                changed = self.rx_batch_silent.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if !self.rx_batch_silent.borrow().1 {
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    if transaction.len() + 32 > primary::MAX_PROPOSAL_BYTES {
                        log::warn!("Transaction exceeds complete-proposal payload limit");
                        continue;
                    }
                    if self.current_batch_size + transaction.len() + 8 * (self.current_batch.len() + 1) + 12
                        > primary::MAX_PROPOSAL_BYTES && !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and enqueue the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx.len() > 8 && tx[0] == 0u8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Store and hand the complete bytes to our primary. PROPOSE is the
        // first cross-authority transmission, after its DAG references are fixed.
        self.tx_message
            .send(serialized)
            .await
            .expect("Failed to deliver local batch");
    }
}
