//! Authentication is performed once, before buffering or counting a remote vote.
use crate::error::DagResult;
use crate::messages::{GrbcPhase, GrbcVote, Header};
use config::Committee;
use crypto::{PublicKey, SignatureService};
use std::ops::Deref;

/// Not deserializable or mutable: only verification or local signing creates it.
#[derive(Clone)]
pub(crate) struct VerifiedGrbcVote(GrbcVote);

impl VerifiedGrbcVote {
    pub(crate) fn verify(vote: GrbcVote, committee: &Committee) -> DagResult<Self> {
        vote.verify(committee)?;
        Ok(Self(vote))
    }

    pub(crate) async fn new(
        header: &Header,
        author: PublicKey,
        phase: GrbcPhase,
        signer: &mut SignatureService,
    ) -> Self {
        Self(GrbcVote::new(header, author, phase, signer).await)
    }

    pub(crate) fn into_vote(self) -> GrbcVote {
        self.0
    }
}

impl Deref for VerifiedGrbcVote {
    type Target = GrbcVote;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
