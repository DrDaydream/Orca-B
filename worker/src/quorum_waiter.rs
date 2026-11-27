// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::ProcessorBatch;
use config::{Committee, Stake};
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: Vec<u8>,
    /// Reuse the digest logged by the batch maker until the processor hashes the bytes.
    #[cfg(feature = "benchmark")]
    pub digest: Digest,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The committee information.
    committee: Committee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<ProcessorBatch>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<ProcessorBatch>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
            }
            .run()
            .await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(message) = self.rx_message.recv().await {
            let threshold = self.committee.quorum_threshold();
            let mut total_stake = self.stake;
            #[cfg(feature = "benchmark")]
            let digest = message.digest;
            #[cfg(feature = "benchmark")]
            let mut ack_stake = self.stake;
            #[cfg(feature = "benchmark")]
            log::info!(target: "orca_pipeline",
                "event=quorum_started batch={:?} source=own peers={} total_stake={} ack_stake={} threshold={}",
                digest, message.handlers.len(), total_stake, ack_stake, threshold);

            let mut wait_for_quorum: FuturesUnordered<_> = message
                .handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    async move { (name, stake, handler.await) }
                })
                .collect();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the primary (that will include it into
            // the dag). This should reduce the amount of synching.
            while let Some((_peer, stake, _reply)) = wait_for_quorum.next().await {
                // The original quorum counts every completed handle, even a closed one.
                total_stake += stake;
                #[cfg(feature = "benchmark")]
                {
                    let outcome = match _reply {
                        Ok(bytes) if bytes.as_ref() == b"Ack" => {
                            ack_stake += stake;
                            "ack"
                        }
                        Ok(_) => "unexpected_reply",
                        Err(_) => "handler_closed",
                    };
                    log::info!(target: "orca_pipeline",
                        "event=quorum_tally batch={:?} source=own peer={:?} outcome={} peer_stake={} total_stake={} ack_stake={} threshold={} remaining={}",
                        digest, _peer, outcome, stake, total_stake, ack_stake, threshold, wait_for_quorum.len());
                }
                if total_stake >= threshold {
                    #[cfg(feature = "benchmark")]
                    log::info!(target: "orca_pipeline",
                        "event=quorum_reached batch={:?} source=own total_stake={} ack_stake={} threshold={}",
                        digest, total_stake, ack_stake, threshold);
                    self.tx_batch
                        .send(ProcessorBatch {
                            serialized: message.batch,
                            #[cfg(feature = "benchmark")]
                            sealed_digest: Some(digest.clone()),
                        })
                        .await
                        .expect("Failed to deliver batch");
                    #[cfg(feature = "benchmark")]
                    log::info!(target: "orca_pipeline", "event=processor_enqueued batch={:?} source=own", digest);
                    break;
                }
            }
        }
    }
}
