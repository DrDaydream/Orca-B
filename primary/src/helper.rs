// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{local_votes_key, proposal_key, GrbcVote, Proposal};
use crate::primary::PrimaryMessage;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;

/// A task dedicated to help other authorities by replying to their certificates requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive certificates requests.
    rx_primaries: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries: Receiver<(Vec<Digest>, PublicKey)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_primaries.recv().await {
            // TODO [issue #195]: Do some accounting to prevent bad nodes from monopolizing our resources.

            // get the requestors address.
            let address = match self.committee.primary(&origin) {
                Ok(x) => x.primary_to_primary,
                Err(e) => {
                    warn!("Unexpected certificate request: {}", e);
                    continue;
                }
            };

            // Reply to the request (the best we can).
            for digest in digests {
                match self.store.read(proposal_key(&digest)).await {
                    Ok(Some(data)) => {
                        let proposal: Proposal = bincode::deserialize(&data)
                            .expect("Failed to deserialize stored proposal");
                        let bytes = bincode::serialize(&PrimaryMessage::Proposal(proposal))
                            .expect("Failed to serialize proposal response");
                        self.network.send(address, Bytes::from(bytes)).await;
                        if let Some(data) = self
                            .store
                            .read(local_votes_key(&digest))
                            .await
                            .ok()
                            .flatten()
                        {
                            let votes: Vec<GrbcVote> = bincode::deserialize(&data)
                                .expect("Failed to deserialize local votes");
                            for vote in votes {
                                let bytes =
                                    bincode::serialize(&PrimaryMessage::GrbcVote(vote)).unwrap();
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                        }
                    }
                    Ok(None) => (),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}
