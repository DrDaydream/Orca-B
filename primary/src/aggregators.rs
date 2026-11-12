// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, GrbcPhase, GrbcVote, Header};
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use std::collections::HashSet;

#[cfg(test)]
#[path = "tests/grbc_tests.rs"]
pub mod grbc_tests;

/// Counts distinct authenticated senders independently for ECHO and READY.
pub struct GrbcVotesAggregator {
    echoes: HashSet<PublicKey>,
    readies: HashSet<PublicKey>,
    pub echo_weight: Stake,
    pub ready_weight: Stake,
}

impl GrbcVotesAggregator {
    pub fn new() -> Self {
        Self {
            echoes: HashSet::new(),
            readies: HashSet::new(),
            echo_weight: 0,
            ready_weight: 0,
        }
    }

    pub fn append(
        &mut self,
        vote: &GrbcVote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<bool> {
        ensure!(
            vote.id == header.id && vote.round == header.round && vote.origin == header.author,
            DagError::UnexpectedGradeOneVote(vote.id.clone())
        );
        vote.verify(committee)?;
        let (used, weight) = match vote.phase {
            GrbcPhase::Echo => (&mut self.echoes, &mut self.echo_weight),
            GrbcPhase::Ready => (&mut self.readies, &mut self.ready_weight),
        };
        if !used.insert(vote.author) {
            return Ok(false);
        }
        *weight += committee.stake(&vote.author);
        Ok(true)
    }
}

/// Aggregate certificates and check if we reach a quorum.
pub struct CertificatesAggregator {
    weight: Stake,
    certificates: Vec<Digest>,
    used: HashSet<PublicKey>,
}

impl CertificatesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            certificates: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> DagResult<Option<Vec<Digest>>> {
        let origin = certificate.origin();

        // Ensure it is the first time this authority votes.
        if !self.used.insert(origin) {
            return Ok(None);
        }

        self.certificates.push(certificate.digest());
        self.weight += committee.stake(&origin);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(self.certificates.drain(..).collect()));
        }
        Ok(None)
    }
}
