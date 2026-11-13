// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{local_votes_key, GrbcVote};
use crate::primary::PrimaryMessage;
use crate::proposal_store::load_proposal;
use crate::vote_sender::VoteDispatch;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

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
    tx_votes: Sender<VoteDispatch>,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries: Receiver<(Vec<Digest>, PublicKey)>,
        tx_votes: Sender<VoteDispatch>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries,
                network: SimpleSender::new(),
                tx_votes,
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
                // Queue saved votes before reading or serializing the full payload.
                if let Some(data) = self
                    .store
                    .read(local_votes_key(&digest))
                    .await
                    .ok()
                    .flatten()
                {
                    let votes: Vec<GrbcVote> =
                        bincode::deserialize(&data).expect("Failed to deserialize local votes");
                    self.tx_votes
                        .send(VoteDispatch {
                            requestor: Some(origin),
                            votes,
                        })
                        .await
                        .expect("Failed to enqueue repair votes");
                }
                match load_proposal(&mut self.store, &digest).await {
                    Ok(Some(proposal)) => {
                        let bytes = tokio::task::spawn_blocking(move || {
                            bincode::serialize(&PrimaryMessage::Proposal(proposal))
                        })
                        .await
                        .expect("Proposal response task failed")
                        .expect("Failed to serialize proposal response");
                        self.network.send(address, Bytes::from(bytes)).await;
                    }
                    Ok(None) => (),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}
