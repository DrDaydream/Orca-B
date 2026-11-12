//! Complete payload work stays outside the GRBC vote processing loop.
use crate::error::{DagError, DagResult};
use crate::messages::{Header, Proposal};
use crate::primary::{PrimaryMessage, Round};
use crate::proposal_store::{assemble_proposal, store_verified_proposal};
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use log::{debug, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Created only after complete authentication, payload checks and atomic storage.
pub(crate) struct StoredProposalHeader(Header);

impl StoredProposalHeader {
    pub(crate) fn into_header(self) -> Header {
        self.0
    }
}

pub(crate) async fn prepare_proposal(
    proposal: Proposal,
    committee: Committee,
    store: &mut Store,
    outbound: bool,
) -> DagResult<(StoredProposalHeader, Option<Bytes>)> {
    let (proposal, bytes) = tokio::task::spawn_blocking(move || {
        proposal.verify(&committee)?;
        let message = PrimaryMessage::Proposal(proposal);
        let bytes = if outbound {
            Some(Bytes::from(bincode::serialize(&message)?))
        } else {
            None
        };
        match message {
            PrimaryMessage::Proposal(proposal) => Ok::<_, DagError>((proposal, bytes)),
            _ => unreachable!(),
        }
    })
    .await
    .expect("Proposal validation task failed")?;
    let header = store_verified_proposal(store, proposal).await?;
    Ok((StoredProposalHeader(header), bytes))
}

fn report(result: DagResult<()>) {
    match result {
        Ok(()) => (),
        Err(DagError::StoreError(e)) => panic!("Storage failure: {}", e),
        Err(e @ DagError::TooOld(..)) => debug!("{}", e),
        Err(e) => warn!("Ignoring invalid proposal: {}", e),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_proposal_processors(
    name: PublicKey,
    committee: Committee,
    store: Store,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    mut rx_remote: Receiver<Proposal>,
    mut rx_local: Receiver<Header>,
    tx: Sender<StoredProposalHeader>,
) {
    // Separate bounded consumers prevent a remote payload backlog from blocking
    // local proposals. At most one blocking validation job per consumer is active.
    let remote_committee = committee.clone();
    let mut remote_store = store.clone();
    let remote_round = consensus_round.clone();
    let remote_tx = tx.clone();
    tokio::spawn(async move {
        while let Some(proposal) = rx_remote.recv().await {
            if proposal.header.round
                < remote_round
                    .load(Ordering::Relaxed)
                    .saturating_sub(gc_depth)
            {
                continue;
            }
            report(
                async {
                    let (header, _) = prepare_proposal(
                        proposal,
                        remote_committee.clone(),
                        &mut remote_store,
                        false,
                    )
                    .await?;
                    remote_tx
                        .send(header)
                        .await
                        .expect("Failed to deliver stored proposal");
                    Ok(())
                }
                .await,
            );
        }
    });
    tokio::spawn(async move {
        let mut store = store;
        let mut network = ReliableSender::new();
        let addresses: Vec<_> = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, a)| a.primary_to_primary)
            .collect();
        let mut pending = BTreeMap::<Round, Vec<CancelHandler>>::new();
        let mut cleanup = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                header = rx_local.recv() => {
                    let header = match header { Some(header) => header, None => break };
                    if header.round < consensus_round.load(Ordering::Relaxed).saturating_sub(gc_depth) { continue; }
                    report(async {
                        let round = header.round;
                        let proposal = assemble_proposal(&mut store, header).await?;
                        let (header, bytes) = prepare_proposal(proposal, committee.clone(), &mut store, true).await?;
                        pending.entry(round).or_default().extend(
                            network.broadcast(addresses.clone(), bytes.expect("Missing outbound proposal")).await
                        );
                        tx.send(header).await.expect("Failed to deliver local proposal");
                        Ok(())
                    }.await);
                }
                _ = cleanup.tick() => {}
            }
            let oldest = consensus_round
                .load(Ordering::Relaxed)
                .saturating_sub(gc_depth);
            pending.retain(|round, handlers| {
                if *round < oldest {
                    return false;
                }
                handlers.retain_mut(|handler| {
                    matches!(
                        handler.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                    )
                });
                !handlers.is_empty()
            });
        }
    });
}
