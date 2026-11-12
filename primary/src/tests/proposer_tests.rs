// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee, keys};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn propose_empty() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
    );

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert!(header.payload.is_empty());
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
async fn propose_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
    );

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id, 100))
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
async fn propose_grbc_edges() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let (tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        1_000,
        20,
        rx_parents,
        rx_our_digests,
        tx_headers,
    );

    // Consume the genesis-based round-1 proposal.
    rx_headers.recv().await.unwrap();
    let parent = Digest([1; 32]);
    let weak = Digest([2; 32]);
    let virtual_edge = Digest([3; 32]);
    tx_parents
        .send((
            vec![parent.clone()],
            vec![weak.clone()],
            vec![virtual_edge.clone()],
            1,
        ))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 2);
    assert!(header.parents.contains(&parent));
    assert!(header.weak_edges.contains(&weak));
    assert!(header.virtual_edges.contains(&virtual_edge));
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
async fn payload_budget_keeps_remaining_batches_for_the_next_round() {
    let (name, secret) = keys().pop().unwrap();
    let (tx_parents, rx_parents) = channel(10);
    let (tx_batches, rx_batches) = channel(10);
    let (tx_headers, mut rx_headers) = channel(10);
    Proposer::spawn(
        name,
        &committee(),
        SignatureService::new(secret),
        64,
        50,
        rx_parents,
        rx_batches,
        tx_headers,
    );
    let first = Digest([1; 32]);
    let second = Digest([2; 32]);
    tx_batches
        .send((first.clone(), 0, MAX_PROPOSAL_BYTES / 2 + 1))
        .await
        .unwrap();
    tx_batches
        .send((second.clone(), 0, MAX_PROPOSAL_BYTES / 2 + 1))
        .await
        .unwrap();
    let one = tokio::time::timeout(Duration::from_secs(2), rx_headers.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(one.payload.len(), 1);
    assert!(one.payload.contains_key(&first));
    tx_parents
        .send((vec![Digest([3; 32])], vec![], vec![], 1))
        .await
        .unwrap();
    let two = tokio::time::timeout(Duration::from_secs(2), rx_headers.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(two.round, 2);
    assert_eq!(two.payload.len(), 1);
    assert!(two.payload.contains_key(&second));
}
