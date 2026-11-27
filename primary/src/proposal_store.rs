//! Durable, authenticated proposal metadata. Transaction bytes live on workers.
use crate::error::DagResult;
use crate::messages::{proposal_header_key, worker_batch_key, Header, Proposal};
use config::{Committee, WorkerId};
use crypto::{Digest, Hash as _};
use std::collections::BTreeMap;
use store::Store;

pub(crate) async fn verify_worker_payload(
    committee: &Committee,
    store: &mut Store,
    proposal: &Proposal,
) -> DagResult<BTreeMap<Digest, WorkerId>> {
    proposal.verify(committee)?;
    let mut missing = BTreeMap::new();
    for (digest, &worker) in &proposal.header.payload {
        if store
            .read(worker_batch_key(digest, worker))
            .await?
            .is_none()
        {
            missing.insert(digest.clone(), worker);
        }
    }
    Ok(missing)
}

/// Only publish after authentication and local worker availability.
pub(crate) async fn store_verified_proposal(
    store: &mut Store,
    proposal: Proposal,
) -> DagResult<Header> {
    let key = proposal_header_key(&proposal.block().digest());
    let bytes = bincode::serialize(&proposal.header)?;
    store
        .write_batch_if_absent(std::iter::once((key, bytes)).collect())
        .await?;
    Ok(proposal.header)
}

pub(crate) async fn load_proposal(
    store: &mut Store,
    digest: &Digest,
) -> DagResult<Option<Proposal>> {
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
    bytes
        .map(|bytes| {
            Ok(Proposal {
                header: bincode::deserialize(&bytes)?,
            })
        })
        .transpose()
}

#[cfg(test)]
#[path = "tests/proposal_store_tests.rs"]
mod tests;
