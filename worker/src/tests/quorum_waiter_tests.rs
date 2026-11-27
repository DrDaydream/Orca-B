// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch, committee_with_base_port, keys, listener};
use crate::worker::WorkerMessage;
use bytes::Bytes;
use futures::future::try_join_all;
use network::ReliableSender;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn wait_for_quorum() {
    let (tx_message, rx_message) = channel(1);
    let (tx_batch, mut rx_batch) = channel(1);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(7_000);

    // Spawn a `QuorumWaiter` instance.
    QuorumWaiter::spawn(committee.clone(), /* stake */ 1, rx_message, tx_batch);

    // Make a batch.
    let message = WorkerMessage::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();
    let expected = Bytes::from(serialized.clone());

    // Spawn enough listeners to acknowledge our batches.
    let mut names = Vec::new();
    let mut addresses = Vec::new();
    let mut listener_handles = Vec::new();
    for (name, address) in committee.others_workers(&myself, /* id */ &0) {
        let address = address.worker_to_worker;
        let handle = listener(address, Some(expected.clone()));
        names.push(name);
        addresses.push(address);
        listener_handles.push(handle);
    }

    // Broadcast the batch through the network.
    let bytes = Bytes::from(serialized.clone());
    let handlers = ReliableSender::new().broadcast(addresses, bytes).await;

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let message = QuorumWaiterMessage {
        batch: serialized.clone(),
        handlers: names.into_iter().zip(handlers.into_iter()).collect(),
    };
    tx_message.send(message).await.unwrap();

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    let output = rx_batch.recv().await.unwrap();
    assert_eq!(output, serialized);

    // Ensure the other listeners correctly received the batch.
    assert!(try_join_all(listener_handles).await.is_ok());
}

#[tokio::test]
async fn cancelled_or_invalid_ack_does_not_release_a_batch() {
    let (input, rx) = channel(2);
    let (out, mut received) = channel(2);
    let c = committee_with_base_port(7100);
    let peers = keys();
    QuorumWaiter::spawn(c, 1, rx, out);
    for (value, valid) in [(1, false), (2, true)] {
        let (first, ack1) = tokio::sync::oneshot::channel();
        let (second, ack2) = tokio::sync::oneshot::channel();
        let (third, ack3) = tokio::sync::oneshot::channel();
        first.send(Bytes::from_static(b"Ack")).unwrap();
        if valid {
            second.send(Bytes::from_static(b"Ack")).unwrap();
        } else {
            drop(second);
        }
        third.send(Bytes::from_static(b"wrong")).unwrap();
        input
            .send(QuorumWaiterMessage {
                batch: vec![value],
                handlers: vec![(peers[0].0, ack1), (peers[1].0, ack2), (peers[2].0, ack3)],
            })
            .await
            .unwrap();
    }
    assert_eq!(
        tokio::time::timeout(tokio::time::Duration::from_secs(2), received.recv())
            .await
            .unwrap(),
        Some(vec![2])
    );
}
