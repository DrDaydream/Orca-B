// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use std::fs;

#[test]
fn store_processes_writes_on_the_async_runtime() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let started_thread = Arc::new(AtomicBool::new(false));
    let on_start = Arc::clone(&started_thread);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .on_thread_start(move || on_start.store(true, Ordering::SeqCst))
        .build()
        .unwrap();
    let path = format!(".db_test_runtime_put_{}", std::process::id());
    let _ = fs::remove_dir_all(&path);
    runtime.block_on(async {
        let mut store = Store::new(&path).unwrap();
        store.write(vec![4], vec![9]).await;
        assert_eq!(store.read(vec![4]).await.unwrap(), Some(vec![9]));
    });
    assert!(!started_thread.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_put_wakes_registered_readers() {
    let path = format!(".db_test_read_only_put_{}", std::process::id());
    let _ = fs::remove_dir_all(&path);
    drop(rocksdb::DB::open_default(&path).unwrap());
    let db = rocksdb::DB::open_for_read_only(&rocksdb::Options::default(), &path, false).unwrap();
    let (notify, reader) = oneshot::channel();
    let mut waiters = ReadWaiters::new();
    waiters.entry(vec![4]).or_default().push_back(notify);
    Store::put(&db, &mut waiters, vec![4], vec![9]);

    assert_eq!(reader.await.unwrap().unwrap(), vec![9]);
    assert!(db.get(&[4u8]).unwrap().is_none());
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

#[tokio::test]
async fn restores_only_legacy_worker_receipts_before_primary_startup() {
    let path = format!(".db_test_receipt_migration_{}", std::process::id());
    let mut store = Store::new(&path).unwrap();
    let digest = [7u8; 32];
    let key = [digest.as_ref(), &2u32.to_le_bytes()].concat();
    let old = [b"orca-worker-batch-v1/".as_ref(), &key].concat();
    let metadata = [b"orca-proposal-header-v3/".as_ref(), digest.as_ref()].concat();
    store.write(old, Vec::new()).await;
    store.write(metadata, vec![1]).await;
    store.migrate_worker_receipts().await.unwrap();
    store.migrate_worker_receipts().await.unwrap();
    assert_eq!(store.read(key).await.unwrap(), Some(Vec::new()));
    let other_worker = [digest.as_ref(), &3u32.to_le_bytes()].concat();
    assert_eq!(store.read(other_worker).await.unwrap(), None);
}
