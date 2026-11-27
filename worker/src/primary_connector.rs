// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{SerializedBatchDigestMessage, CHANNEL_CAPACITY};
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt as _};
use network::ReliableSender;
use std::net::SocketAddr;
use tokio::sync::mpsc::Receiver;

// Send batches' digests to the primary.
pub struct PrimaryConnector {
    /// The primary network address.
    primary_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: ReliableSender,
}

impl PrimaryConnector {
    pub fn spawn(primary_address: SocketAddr, rx_digest: Receiver<SerializedBatchDigestMessage>) {
        tokio::spawn(async move {
            Self {
                primary_address,
                rx_digest,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut acks = FuturesUnordered::<network::CancelHandler>::new();
        let mut input_open = true;
        while input_open || !acks.is_empty() {
            tokio::select! {
                digest = self.rx_digest.recv(), if input_open && acks.len() < CHANNEL_CAPACITY => match digest {
                    Some(digest) => acks.push(self.network.send(self.primary_address, Bytes::from(digest)).await),
                    None => input_open = false,
                },
                Some(ack) = acks.next(), if !acks.is_empty() => {
                    ack.expect("Local primary closed before acknowledging worker digest");
                }
            }
        }
    }
}
