// Copyright(C) Facebook, Inc. and its affiliates.
use rocksdb::{Direction, IteratorMode};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/store_tests.rs"]
pub mod store_tests;

pub type StoreError = rocksdb::Error;
type StoreResult<T> = Result<T, StoreError>;

type Key = Vec<u8>;
type Value = Vec<u8>;
type ReadWaiters = HashMap<Key, VecDeque<oneshot::Sender<StoreResult<Value>>>>;

pub enum StoreCommand {
    Write(Key, Value),
    WriteBatchIfAbsent(BTreeMap<Key, Value>, oneshot::Sender<StoreResult<usize>>),
    RestoreWorkerReceipts(oneshot::Sender<StoreResult<()>>),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    NotifyRead(Key, oneshot::Sender<StoreResult<Value>>),
}

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    /// Test-only actor injection for controlling storage acknowledgements.
    #[cfg(feature = "test-utils")]
    pub fn from_channel(channel: Sender<StoreCommand>) -> Self {
        Self { channel }
    }

    pub fn new(path: &str) -> StoreResult<Self> {
        let db = rocksdb::DB::open_default(path)?;
        let mut obligations = ReadWaiters::new();
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            // HeaderWaiter cancels incomplete reads during GC, including keys that may never arrive.
            let mut next_prune = Instant::now() + Duration::from_secs(1);
            while let Some(command) = rx.recv().await {
                if !obligations.is_empty() && Instant::now() >= next_prune {
                    obligations.retain(|_, waiters| {
                        waiters.retain(|waiter| !waiter.is_closed());
                        !waiters.is_empty()
                    });
                    next_prune = Instant::now() + Duration::from_secs(1);
                }
                match command {
                    StoreCommand::Write(key, value) => {
                        Self::put(&db, &mut obligations, key, value);
                    }
                    StoreCommand::WriteBatchIfAbsent(entries, sender) => {
                        // This actor owns every write, so the existence check and
                        // atomic batch cannot race with another writer.
                        let result = (|| -> StoreResult<BTreeMap<Key, Value>> {
                            let mut batch = rocksdb::WriteBatch::default();
                            let mut inserted = BTreeMap::new();
                            for (key, value) in entries {
                                // A pinned read avoids copying an existing payload.
                                if db.get_pinned(&key)?.is_none() {
                                    batch.put(&key, &value);
                                    inserted.insert(key, value);
                                }
                            }
                            if !inserted.is_empty() {
                                db.write(batch)?;
                            }
                            Ok(inserted)
                        })();
                        match result {
                            Ok(inserted) => {
                                let count = inserted.len();
                                // Publish notifications only after the entire
                                // batch succeeds, including its metadata marker.
                                for (key, value) in inserted {
                                    if let Some(senders) = obligations.remove(&key) {
                                        for waiter in senders {
                                            let _ = waiter.send(Ok(value.clone()));
                                        }
                                    }
                                }
                                let _ = sender.send(Ok(count));
                            }
                            Err(error) => {
                                let _ = sender.send(Err(error));
                            }
                        }
                    }
                    StoreCommand::RestoreWorkerReceipts(sender) => {
                        let _ = sender.send(Self::restore_worker_receipts(&db));
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(&key);
                        let _ = sender.send(response);
                    }
                    StoreCommand::NotifyRead(key, sender) => {
                        let response = db.get(&key);
                        match response {
                            Ok(None) => obligations
                                .entry(key)
                                .or_insert_with(VecDeque::new)
                                .push_back(sender),
                            _ => {
                                let _ = sender.send(response.map(|x| x.unwrap()));
                            }
                        }
                    }
                }
            }
        });
        Ok(Self { channel: tx })
    }

    fn put(db: &rocksdb::DB, obligations: &mut ReadWaiters, key: Key, value: Value) {
        if let Err(error) = db.put(&key, &value) {
            log::error!(
                "RocksDB write failed in {} for key {:?}: {}",
                db.path().display(),
                key,
                error
            );
        }
        // Like main, notify registered readers with the submitted value after each put attempt.
        if let Some(mut senders) = obligations.remove(&key) {
            while let Some(sender) = senders.pop_front() {
                let _ = sender.send(Ok(value.clone()));
            }
        }
    }

    /// Upgrade the primary's receipt keys from the first digest-proposal release.
    /// This command is enqueued before primary startup. Each chunk and the final marker
    /// can be safely replayed if the process stops partway through the scan.
    fn restore_worker_receipts(db: &rocksdb::DB) -> StoreResult<()> {
        const PREFIX: &[u8] = b"orca-worker-batch-v1/";
        const DONE: &[u8] = b"orca-narwhal-worker-receipts-migrated-v1";
        if db.get_pinned(DONE)?.is_some() {
            return Ok(());
        }
        let mut batch = rocksdb::WriteBatch::default();
        let mut records = db.iterator(IteratorMode::From(PREFIX, Direction::Forward));
        for (key, value) in records.by_ref() {
            if !key.starts_with(PREFIX) {
                break;
            }
            let receipt = &key[PREFIX.len()..];
            if receipt.len() == 36 && value.is_empty() {
                batch.put(receipt, &[]);
                if batch.len() == 4096 {
                    db.write(batch)?;
                    batch = rocksdb::WriteBatch::default();
                }
            }
        }
        records.status()?;
        batch.put(DONE, &[]);
        db.write(batch)
    }

    pub async fn migrate_worker_receipts(&mut self) -> StoreResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.channel
            .send(StoreCommand::RestoreWorkerReceipts(sender))
            .await
            .expect("Failed to request worker receipt migration");
        receiver.await.expect("Worker receipt migration stopped")
    }

    pub async fn write(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
            panic!("Failed to send Write command to store: {}", e);
        }
    }

    pub async fn read(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read command from store")
    }

    /// Atomically insert missing immutable values and return the number inserted.
    /// Existing values are neither copied out nor rewritten. Success acknowledges
    /// RocksDB's write, using its default durability options (not an fsync).
    pub async fn write_batch_if_absent(
        &mut self,
        entries: BTreeMap<Key, Value>,
    ) -> StoreResult<usize> {
        let (sender, receiver) = oneshot::channel();
        self.channel
            .send(StoreCommand::WriteBatchIfAbsent(entries, sender))
            .await
            .expect("Failed to send immutable batch to store");
        receiver
            .await
            .expect("Failed to receive immutable write result")
    }

    pub async fn notify_read(&mut self, key: Key) -> StoreResult<Value> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::NotifyRead(key, sender))
            .await
        {
            panic!("Failed to send NotifyRead command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to NotifyRead command from store")
    }
}
