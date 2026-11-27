// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// A serialized worker batch. Only the author already has its digest before hashing.
#[derive(Debug)]
pub struct ProcessorBatch {
    pub serialized: Vec<u8>,
    #[cfg(feature = "benchmark")]
    pub sealed_digest: Option<Digest>,
}

impl From<Vec<u8>> for ProcessorBatch {
    fn from(serialized: Vec<u8>) -> Self {
        Self {
            serialized,
            #[cfg(feature = "benchmark")]
            sealed_digest: None,
        }
    }
}

pub(crate) fn batch_digest(serialized: &[u8]) -> Digest {
    Digest(
        Sha512::digest(serialized).as_slice()[..32]
            .try_into()
            .unwrap(),
    )
}

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<ProcessorBatch>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            #[cfg(feature = "benchmark")]
            let source = if own_digest { "own" } else { "other" };
            while let Some(batch) = rx_batch.recv().await {
                #[cfg(feature = "benchmark")]
                if let Some(sealed_digest) = &batch.sealed_digest {
                    log::info!(target: "orca_pipeline", "event=processor_dequeued batch={:?} source={} worker={}",
                        sealed_digest, source, id);
                }
                // Hash the batch.
                let digest = batch_digest(&batch.serialized);
                #[cfg(feature = "benchmark")]
                log::info!(target: "orca_pipeline", "event=processor_hashed batch={:?} source={} worker={} wire_bytes={}",
                    digest, source, id, batch.serialized.len());

                // Store the batch.
                #[cfg(not(feature = "benchmark"))]
                store.write(digest.to_vec(), batch.serialized).await;
                #[cfg(feature = "benchmark")]
                {
                    let stored_digest = digest.clone();
                    store.write_observed(digest.to_vec(), batch.serialized, move || {
                        log::info!(target: "orca_pipeline", "event=store_put_done batch={:?} source={} worker={}",
                            stored_digest, source, id);
                    }).await;
                    log::info!(target: "orca_pipeline", "event=store_enqueued batch={:?} source={} worker={}",
                        digest, source, id);
                }

                // Deliver the batch's digest.
                #[cfg(feature = "benchmark")]
                let trace_digest = digest.clone();
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(SerializedBatchDigestMessage {
                        bytes: message,
                        #[cfg(feature = "benchmark")]
                        trace: (trace_digest.clone(), id, source),
                    })
                    .await
                    .expect("Failed to send digest");
                #[cfg(feature = "benchmark")]
                log::info!(target: "orca_pipeline", "event=digest_enqueued batch={:?} source={} worker={}",
                    trace_digest, source, id);
            }
        });
    }
}
