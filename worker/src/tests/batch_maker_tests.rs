// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use tokio::sync::mpsc::channel;
use tokio::sync::watch;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let (_tx_batch_silent, rx_batch_silent) = watch::channel((0, false));

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
        rx_batch_silent,
        tx_message,
    );

    // Send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = vec![transaction(), transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let (_tx_batch_silent, rx_batch_silent) = watch::channel((0, false));

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        rx_transaction,
        rx_batch_silent,
        tx_message,
    );

    // Do not send enough transactions to seal a batch..
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = vec![transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn silent_batch_maker_resumes_without_dropping_transactions() {
    let (tx_transaction, rx_transaction) = channel(2);
    let (tx_message, mut rx_message) = channel(1);
    let (tx_batch_silent, rx_batch_silent) = watch::channel((1, true));

    BatchMaker::spawn(200, 20, rx_transaction, rx_batch_silent, tx_message);

    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();
    assert!(timeout(Duration::from_millis(60), rx_message.recv())
        .await
        .is_err());

    tx_batch_silent.send((2, false)).unwrap();
    let batch = timeout(Duration::from_millis(200), rx_message.recv())
        .await
        .unwrap()
        .unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, vec![transaction(), transaction()]),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn full_batches_fit_propose_and_preserve_transactions_across_splits() {
    let (tx_transaction, rx_transaction) = channel(3);
    let (tx_message, mut rx_message) = channel(3);
    let (_tx_silent, rx_silent) = watch::channel((0, false));
    BatchMaker::spawn(
        primary::MAX_PROPOSAL_BYTES,
        50,
        rx_transaction,
        rx_silent,
        tx_message,
    );
    let transaction = vec![1; 3 * 1024 * 1024];
    tx_transaction.send(transaction.clone()).await.unwrap();
    tx_transaction.send(transaction.clone()).await.unwrap();
    for _ in 0..2 {
        let bytes = timeout(Duration::from_secs(2), rx_message.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(bytes.len() <= primary::MAX_PROPOSAL_BYTES);
        match bincode::deserialize(&bytes).unwrap() {
            WorkerMessage::Batch(batch) => assert_eq!(batch, vec![transaction.clone()]),
            _ => panic!("Unexpected message"),
        }
    }
}
