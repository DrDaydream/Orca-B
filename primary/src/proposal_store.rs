//! Complete proposals on the wire, content-addressed batches and headers on disk.
use crate::error::{DagError, DagResult};
use crate::messages::{batch_key, proposal_header_key, Header, Proposal};
use crypto::{Digest, Hash as _};
use std::collections::BTreeMap;
use store::Store;

/// Caller must verify the complete proposal first. The header is also a marker
/// that all referenced bytes were received: publish it atomically with the data.
/// This marker says nothing about local READY, FINISH, or DAG validation.
pub(crate) async fn store_verified_proposal(
    store: &mut Store,
    proposal: Proposal,
) -> DagResult<Header> {
    let digest = proposal.block().digest();
    let Proposal { header, batches } = proposal;
    let mut entries: BTreeMap<_, _> = batches
        .into_iter()
        .map(|(digest, bytes)| (batch_key(&digest), bytes))
        .collect();
    entries.insert(proposal_header_key(&digest), bincode::serialize(&header)?);
    store.write_batch_if_absent(entries).await?;
    Ok(header)
}

/// Only data transmission needs to assemble payload bytes. Dependency checks
/// read the compact header marker directly.
pub(crate) async fn assemble_proposal(store: &mut Store, header: Header) -> DagResult<Proposal> {
    let mut batches = BTreeMap::new();
    for digest in header.payload.keys() {
        let bytes = store
            .read(batch_key(digest))
            .await?
            .ok_or_else(|| DagError::MissingBatch(digest.clone()))?;
        batches.insert(digest.clone(), bytes);
    }
    Ok(Proposal { header, batches })
}

pub(crate) async fn load_proposal(
    store: &mut Store,
    digest: &Digest,
) -> DagResult<Option<Proposal>> {
    match store.read(proposal_header_key(digest)).await? {
        Some(bytes) => {
            let header = bincode::deserialize(&bytes)?;
            assemble_proposal(store, header).await.map(Some)
        }
        None => Ok(None),
    }
}

#[cfg(test)]
#[path = "tests/proposal_store_tests.rs"]
mod tests;
