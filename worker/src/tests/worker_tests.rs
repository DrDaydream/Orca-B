// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener, transaction};
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::fs;

#[test]
fn peer_batch_borrows_transactions_from_the_wire() {
    let transactions = vec![vec![], vec![3, 4], vec![7; 512]];
    let frame = bincode::serialize(&WorkerMessage::Batch(transactions.clone())).unwrap();
    match bincode::deserialize::<ReceivedWorkerMessage>(&frame).unwrap() {
        ReceivedWorkerMessage::Batch(batch) => {
            assert_eq!(batch.len(), transactions.len());
            for (received, expected) in batch.iter().zip(transactions) {
                assert_eq!(*received, &expected[..]);
                if !expected.is_empty() {
                    assert!(frame.as_ptr_range().contains(&received.as_ptr()));
                }
            }
        }
        _ => panic!("Expected batch"),
    }
}

#[test]
fn peer_decoder_accepts_bullshark_frames_and_rejects_truncation() {
    let requestor = PublicKey([3; 32]);
    let missing = vec![Digest([1; 32]), Digest([2; 32])];
    let request = WorkerMessage::BatchRequest(missing.clone(), requestor);
    let request_frame = bincode::serialize(&request).unwrap();
    match bincode::deserialize::<ReceivedWorkerMessage>(&request_frame).unwrap() {
        ReceivedWorkerMessage::BatchRequest(digests, author) => {
            assert_eq!(digests, missing);
            assert_eq!(author, requestor);
        }
        _ => panic!("Expected batch request"),
    }

    let batch = WorkerMessage::Batch(vec![vec![42]]);
    // Bullshark bincode frame: variant 0, one transaction, one byte of payload.
    let batch_frame = [
        0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 42,
    ];
    assert_eq!(bincode::serialize(&batch).unwrap(), batch_frame);
    for frame in [&batch_frame[..], &request_frame[..]] {
        for len in 0..frame.len() {
            assert!(bincode::deserialize::<ReceivedWorkerMessage>(&frame[..len]).is_err());
        }
    }
}

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(name, id, committee.clone(), parameters, store);

    // Spawn a network listener to receive our batch's digest.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    let expected = bincode::serialize(&WorkerPrimaryMessage::OurBatch(batch_digest(), id)).unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected)));

    // Spawn enough workers' listeners to acknowledge our batches.
    for (_, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        let _ = listener(address, /* expected */ None);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the primary received the batch's digest (ie. it did not panic).
    assert!(handle.await.is_ok());
}
