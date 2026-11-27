// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::Header;
use crypto::{Digest, PublicKey};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::Sender;

/// Checks local worker receipts and delegates missing batches to the HeaderWaiter.
#[derive(Clone)]
pub struct Synchronizer {
    name: PublicKey,
    store: Store,
    tx_header_waiter: Sender<WaiterMessage>,
}

impl Synchronizer {
    pub fn new(name: PublicKey, store: Store, tx_header_waiter: Sender<WaiterMessage>) -> Self {
        Self {
            name,
            store,
            tx_header_waiter,
        }
    }

    /// Returns `true` if we have all transactions of the payload. If we don't, we return false,
    /// synchronize with other nodes (through our workers), and re-schedule processing of the
    /// header for when we will have its complete payload.
    pub async fn missing_payload(&mut self, header: &Header) -> DagResult<bool> {
        // We don't store the payload of our own workers.
        if header.author == self.name {
            #[cfg(feature = "benchmark")]
            log::info!(
                target: "orca_pipeline",
                "event=payload_check_skipped header={:?} round={} author={:?} reason=own_author",
                header.id, header.round, header.author
            );
            return Ok(false);
        }

        let mut missing = HashMap::new();
        for (digest, worker_id) in header.payload.iter() {
            // Check whether we have the batch. If one of our worker has the batch, the primary stores the pair
            // (digest, worker_id) in its own storage. It is important to verify that we received the batch
            // from the correct worker id to prevent the following attack:
            //      1. A Bad node sends a batch X to 2f good nodes through their worker #0.
            //      2. The bad node proposes a malformed block containing the batch X and claiming it comes
            //         from worker #1.
            //      3. The 2f good nodes do not need to sync and thus don't notice that the header is malformed.
            //         The bad node together with the 2f good nodes thus certify a block containing the batch X.
            //      4. The last good node will never be able to sync as it will keep sending its sync requests
            //         to workers #1 (rather than workers #0). Also, clients will never be able to retrieve batch
            //         X as they will be querying worker #1.
            let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
            if self.store.read(key).await?.is_none() {
                #[cfg(feature = "benchmark")]
                log::info!(target: "orca_pipeline",
                    "event=payload_missing header={:?} round={} author={:?} batch={:?} worker={}",
                    header.id, header.round, header.author, digest, worker_id);
                missing.insert(digest.clone(), *worker_id);
            }
        }

        #[cfg(feature = "benchmark")]
        log::info!(
            target: "orca_pipeline",
            "event=payload_checked header={:?} round={} author={:?} batches={} missing={}",
            header.id, header.round, header.author, header.payload.len(), missing.len()
        );
        if missing.is_empty() {
            return Ok(false);
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches(missing, header.clone()))
            .await
            .expect("Failed to send sync batch request");
        Ok(true)
    }

    pub async fn committed(&mut self, blocks: Vec<(Digest, Digest)>) {
        if !blocks.is_empty() {
            self.tx_header_waiter
                .send(WaiterMessage::Committed(blocks))
                .await
                .expect("Failed to clear committed dependency requests");
        }
    }
}
