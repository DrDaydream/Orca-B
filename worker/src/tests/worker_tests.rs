// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, serialized_batch, transaction};
use futures::{SinkExt as _, StreamExt as _};
use primary::WorkerPrimaryMessage;
use std::fs;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn worker_sends_only_a_digest_after_quorum_acknowledges_the_batch() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200,
        ..Parameters::default()
    };
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let primary = TcpListener::bind(committee.primary(&name).unwrap().worker_to_primary)
        .await
        .unwrap();
    let mut peers = Vec::new();
    for (_, address) in committee.others_workers(&name, &id).into_iter().take(2) {
        peers.push(TcpListener::bind(address.worker_to_worker).await.unwrap());
    }
    Worker::spawn(name, id, committee.clone(), parameters, store.clone());
    let address = committee.worker(&name, &id).unwrap().transactions;
    timeout(Duration::from_secs(5), async {
        let client = loop {
            if let Ok(stream) = TcpStream::connect(address).await {
                break stream;
            }
            tokio::task::yield_now().await;
        };
        let mut client = Framed::new(client, LengthDelimitedCodec::new());
        client.send(Bytes::from(transaction())).await.unwrap();
        client.send(Bytes::from(transaction())).await.unwrap();
        let mut streams = Vec::new();
        for peer in peers {
            let (socket, _) = peer.accept().await.unwrap();
            let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
            assert_eq!(
                stream.next().await.unwrap().unwrap().as_ref(),
                serialized_batch().as_slice()
            );
            streams.push(stream);
        }
        streams[0].send(Bytes::from_static(b"Ack")).await.unwrap();
        assert!(timeout(Duration::from_millis(40), primary.accept())
            .await
            .is_err());
        streams[1].send(Bytes::from_static(b"Ack")).await.unwrap();
        let (socket, _) = primary.accept().await.unwrap();
        let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
        let notice = stream.next().await.unwrap().unwrap();
        let expected =
            bincode::serialize(&WorkerPrimaryMessage::OurBatch(batch_digest(), id)).unwrap();
        assert_eq!(notice.as_ref(), expected.as_slice());
        assert_eq!(
            store.read(batch_digest().to_vec()).await.unwrap(),
            Some(serialized_batch())
        );
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn worker_digest_notifications_can_be_in_flight_together() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tx, rx) = channel(4);
    PrimaryConnector::spawn(listener.local_addr().unwrap(), rx);
    tx.send(vec![1]).await.unwrap();
    tx.send(vec![2]).await.unwrap();
    drop(tx);
    timeout(Duration::from_secs(2), async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
        for value in [1u8, 2] {
            assert_eq!(frames.next().await.unwrap().unwrap().as_ref(), &[value]);
        }
        for _ in 0..2 {
            frames.send(Bytes::from_static(b"Ack")).await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn peer_ack_follows_storage_and_notification_of_the_actual_batch_digest() {
    let mut storage =
        Store::new(&format!(".db_test_peer_batch_ack_{}", std::process::id())).unwrap();
    let (notices, mut received) = channel(1);
    notices.send(Vec::new()).await.unwrap();
    let handler = WorkerReceiverHandler {
        store: storage.clone(),
        id: 0,
        tx_primary: notices,
        tx_helper: channel(1).0,
    };
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (peer, _) = socket.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(peer, LengthDelimitedCodec::new()).split();
        let batch = reader.next().await.unwrap().unwrap();
        handler.dispatch(&mut writer, batch.freeze()).await.unwrap();
    });
    let digest = batch_digest();
    let batch = serialized_batch();
    let mut sender = network::ReliableSender::new();
    timeout(Duration::from_secs(3), async {
        let mut ack = sender.send(addr, Bytes::from(batch.clone())).await;
        assert_eq!(storage.notify_read(digest.to_vec()).await.unwrap(), batch);
        assert!(timeout(Duration::from_millis(40), &mut ack).await.is_err());
        assert!(received.recv().await.unwrap().is_empty());
        let notice = received.recv().await.unwrap();
        assert!(matches!(bincode::deserialize(&notice).unwrap(),
            WorkerPrimaryMessage::OthersBatch(found, 0) if found == digest));
        assert_eq!(ack.await.unwrap().as_ref(), b"Ack");
        server.await.unwrap();
    })
    .await
    .unwrap();
}
