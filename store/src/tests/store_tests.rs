// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use std::fs;

#[test]
fn database_progress_does_not_depend_on_polling_the_async_runtime() {
    // A busy protocol runtime must not also suspend the database actor. Exercise
    // the real store, including the atomic write acknowledgement, outside block_on.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let path = format!(".db_test_independent_actor_{}", std::process::id());
    let _ = fs::remove_dir_all(&path);
    let store = runtime.block_on(async { Store::new(&path).unwrap() });
    let (sender, mut receiver) = oneshot::channel();
    store
        .channel
        .try_send(StoreCommand::WriteBatchIfAbsent(
            [(vec![4], vec![9])].iter().cloned().collect(),
            sender,
        ))
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let result = loop {
        match receiver.try_recv() {
            Ok(result) => break Some(result),
            Err(oneshot::error::TryRecvError::Empty) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            _ => break None,
        }
    };
    drop(store);
    drop(runtime);
    assert_eq!(
        result
            .expect("database stalled behind the async runtime")
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn create_store() {
    // Create new store.
    let path = ".db_test_create_store";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path);
    assert!(store.is_ok());
}

#[tokio::test]
async fn read_write_value() {
    // Create new store.
    let path = ".db_test_read_write_value";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Write value to the store.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let value = vec![4u8, 5u8, 6u8, 7u8];
    store.write(key.clone(), value.clone()).await;

    // Read value.
    let result = store.read(key).await;
    assert!(result.is_ok());
    let read_value = result.unwrap();
    assert!(read_value.is_some());
    assert_eq!(read_value.unwrap(), value);
}

#[tokio::test]
async fn read_unknown_key() {
    // Create new store.
    let path = ".db_test_read_unknown_key";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Try to read unknown key.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let result = store.read(key).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn read_notify() {
    // Create new store.
    let path = ".db_test_read_notify";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Try to read a kew that does not yet exist. Then write a value
    // for that key and check that notify read returns the result.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let value = vec![4u8, 5u8, 6u8, 7u8];

    // Try to read a missing value.
    let mut store_copy = store.clone();
    let key_copy = key.clone();
    let value_copy = value.clone();
    let handle = tokio::spawn(async move {
        match store_copy.notify_read(key_copy).await {
            Ok(v) => assert_eq!(v, value_copy),
            _ => panic!("Failed to read from store"),
        }
    });

    // Write the missing value and ensure the handle terminates correctly.
    store.write(key, value).await;
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn immutable_batch_inserts_only_missing_values() {
    let path = format!(".db_test_immutable_batch_{}", std::process::id());
    let mut store = Store::new(&path).unwrap();
    let entries = [(vec![1], vec![10]), (vec![2], vec![20])]
        .iter()
        .cloned()
        .collect();
    assert_eq!(store.write_batch_if_absent(entries).await.unwrap(), 2);
    let overlap = [(vec![1], vec![99]), (vec![3], vec![30])]
        .iter()
        .cloned()
        .collect();
    assert_eq!(store.write_batch_if_absent(overlap).await.unwrap(), 1);
    assert_eq!(store.read(vec![1]).await.unwrap(), Some(vec![10]));
    assert_eq!(store.read(vec![2]).await.unwrap(), Some(vec![20]));
    assert_eq!(store.read(vec![3]).await.unwrap(), Some(vec![30]));
    let duplicate = [(vec![1], vec![99])].iter().cloned().collect();
    assert_eq!(store.write_batch_if_absent(duplicate).await.unwrap(), 0);
}
