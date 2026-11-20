// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::{PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use network::SimpleSender;
use tokio::sync::watch::Receiver;

/// Forwards the most recent committed round to workers.
pub struct GarbageCollector;

impl GarbageCollector {
    pub fn spawn(name: &PublicKey, committee: &Committee, mut rounds: Receiver<Round>) {
        let addresses: Vec<_> = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();
        tokio::spawn(async move {
            let mut network = SimpleSender::new();
            while rounds.changed().await.is_ok() {
                let round = *rounds.borrow_and_update();
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                    .expect("Failed to serialize our own message");
                network
                    .broadcast(addresses.clone(), Bytes::from(bytes))
                    .await;
            }
        });
    }
}
