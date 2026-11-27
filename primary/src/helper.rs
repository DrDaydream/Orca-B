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

pub(crate) enum RepairRequest {
    Proposals(Vec<Digest>, PublicKey),
    Votes(Vec<Digest>, PublicKey),
}

/// A task dedicated to help other authorities by replying to their certificates requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive certificates requests.
    rx_primaries: Receiver<RepairRequest>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
    tx_votes: Sender<VoteDispatch>,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries: Receiver<RepairRequest>,
        tx_votes: Sender<VoteDispatch>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries,
                network: crate::adversary::simple_sender(),
                tx_votes,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(request) = self.rx_primaries.recv().await {
            let (digests, origin, include_header) = match request {
                RepairRequest::Proposals(digests, origin) => (digests, origin, true),
                RepairRequest::Votes(digests, origin) => (digests, origin, false),
            };
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
                // Queue saved votes before reading or serializing the signed header.
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
                if !include_header {
                    continue;
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
