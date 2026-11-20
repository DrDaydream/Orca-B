// Copyright(C) Facebook, Inc. and its affiliates.
use crate::header_waiter::WaiterMessage;
use crypto::Digest;
use tokio::sync::mpsc::Sender;

/// Publishes committed records to the repair worker. Dependency availability
/// no longer gates GRBC processing or round advancement in this experiment.
pub struct Synchronizer {
    tx_header_waiter: Sender<WaiterMessage>,
}

impl Synchronizer {
    pub fn new(tx_header_waiter: Sender<WaiterMessage>) -> Self {
        Self { tx_header_waiter }
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
