// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{proposal_header_key, Certificate, Header};
use config::Committee;
use crypto::Digest;
use crypto::Hash as _;
use store::Store;
use tokio::sync::mpsc::Sender;

/// Checks proposal dependencies and requests missing records through HeaderWaiter.
pub struct Synchronizer {
    /// The persistent storage.
    store: Store,
    /// Send commands to the `HeaderWaiter`.
    tx_header_waiter: Sender<WaiterMessage>,
    /// The genesis and its digests.
    genesis: Vec<(Digest, Certificate)>,
}

impl Synchronizer {
    pub fn new(
        committee: &Committee,
        store: Store,
        tx_header_waiter: Sender<WaiterMessage>,
    ) -> Self {
        Self {
            store,
            tx_header_waiter,
            genesis: Certificate::genesis(committee)
                .into_iter()
                .map(|x| (x.digest(), x))
                .collect(),
        }
    }

    /// Returns the parents of a header if we have them all. If at least one parent is missing,
    /// we return an empty vector, synchronize with other nodes, and re-schedule processing
    /// of the header for when we will have all the parents.
    pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
        let mut missing_required = Vec::new();
        let mut missing_virtual = Vec::new();
        let mut parents = Vec::new();
        for digest in &header.parents {
            if let Some(genesis) = self
                .genesis
                .iter()
                .find(|(x, _)| x == digest)
                .map(|(_, x)| x)
            {
                parents.push(genesis.clone());
                continue;
            }

            match self.store.read(digest.to_vec()).await? {
                Some(certificate) => parents.push(bincode::deserialize(&certificate)?),
                None => missing_required.push(digest.clone()),
            };
        }
        for digest in &header.weak_edges {
            if let Some((_, genesis)) = self
                .genesis
                .iter()
                .find(|(candidate, _)| candidate == digest)
            {
                parents.push(genesis.clone());
                continue;
            }
            match self.store.read(digest.to_vec()).await? {
                Some(certificate) => parents.push(bincode::deserialize(&certificate)?),
                None => missing_required.push(digest.clone()),
            }
        }
        for digest in &header.virtual_edges {
            if let Some((_, genesis)) = self
                .genesis
                .iter()
                .find(|(candidate, _)| candidate == digest)
            {
                parents.push(genesis.clone());
                continue;
            }
            // Validate the target's signed metadata, not its local grade. One
            // honest node reaching READY does not imply every peer can reach
            // READY, so requiring a local grade here would block honest proposals.
            match self.store.read(proposal_header_key(digest)).await? {
                Some(bytes) => parents.push(Certificate {
                    header: bincode::deserialize(&bytes)?,
                    votes: Vec::new(),
                }),
                None => missing_virtual.push(digest.clone()),
            }
        }
        if !missing_required.is_empty() || !missing_virtual.is_empty() {
            self.tx_header_waiter
                .send(WaiterMessage::SyncParents(
                    missing_required,
                    missing_virtual,
                    header.clone(),
                ))
                .await
                .expect("Failed to send sync parents request");
            return Ok(Vec::new());
        }
        Ok(parents)
    }

    pub async fn ready_for_round_advance(&mut self, certificate: &Certificate) -> DagResult<bool> {
        for digest in certificate
            .header
            .parents
            .iter()
            .chain(&certificate.header.weak_edges)
        {
            if self
                .genesis
                .iter()
                .any(|(candidate, _)| candidate == digest)
            {
                continue;
            }
            if self.store.read(digest.to_vec()).await?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn committed(&mut self, blocks: Vec<(Digest, Digest)>) {
        if !blocks.is_empty() {
            self.tx_header_waiter
                .send(WaiterMessage::Committed(blocks))
                .await
                .expect("Failed to clear committed dependency requests");
        }
    }
}
