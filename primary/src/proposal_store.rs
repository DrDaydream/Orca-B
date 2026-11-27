//! Durable, authenticated proposal metadata. Transaction bytes live on workers.
use crate::error::DagResult;
use crate::messages::{proposal_header_key, Header};
use crypto::Digest;
use store::Store;

/// Only publish after authentication and local worker availability.
pub(crate) async fn store_verified_proposal(
    store: &mut Store,
    header: Header,
) -> DagResult<Header> {
    let key = proposal_header_key(&header.block_digest());
    let bytes = bincode::serialize(&header)?;
    store
        .write_batch_if_absent(std::iter::once((key, bytes)).collect())
        .await?;
    Ok(header)
}

pub(crate) async fn load_proposal(store: &mut Store, digest: &Digest) -> DagResult<Option<Header>> {
    let bytes = match store.read(proposal_header_key(digest)).await? {
        Some(bytes) => Some(bytes),
        // Old primaries also stored signed headers. They can serve a repair,
        // but the receiver still needs fresh receipts from its own workers.
        None => {
            store
                .read([b"orca-proposal-header-v3/".as_ref(), digest.as_ref()].concat())
                .await?
        }
    };
    Ok(bytes
        .map(|bytes| bincode::deserialize(&bytes))
        .transpose()?)
}

#[cfg(test)]
#[path = "tests/proposal_store_tests.rs"]
mod tests;
