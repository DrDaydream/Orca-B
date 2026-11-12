// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsensusNetworkMessage {
    pub author: PublicKey,
    pub payload: Vec<u8>,
    pub signature: Signature,
}

impl ConsensusNetworkMessage {
    pub async fn new(
        payload: Vec<u8>,
        author: PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        let mut message = Self {
            author,
            payload,
            signature: Signature::default(),
        };
        message.signature = signature_service.request_signature(message.digest()).await;
        message
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        self.signature
            .verify(&self.digest(), &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for ConsensusNetworkMessage {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(b"orca-consensus-network-v1");
        hasher.update(&self.author);
        hasher.update((self.payload.len() as u64).to_le_bytes());
        hasher.update(&self.payload);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Header {
    pub author: PublicKey,
    pub round: Round,
    pub payload: BTreeMap<Digest, WorkerId>,
    pub parents: BTreeSet<Digest>,
    /// References to blocks promoted into Dag from rounds older than r - 1.
    pub weak_edges: BTreeSet<Digest>,
    /// References to grade-1 blocks still in VDag when the previous round ended.
    pub virtual_edges: BTreeSet<Digest>,
    pub id: Digest,
    pub signature: Signature,
}

impl Header {
    pub async fn new(
        author: PublicKey,
        round: Round,
        payload: BTreeMap<Digest, WorkerId>,
        parents: BTreeSet<Digest>,
        weak_edges: BTreeSet<Digest>,
        virtual_edges: BTreeSet<Digest>,
        signature_service: &mut SignatureService,
    ) -> Self {
        let header = Self {
            author,
            round,
            payload,
            parents,
            weak_edges,
            virtual_edges,
            id: Digest::default(),
            signature: Signature::default(),
        };
        let id = header.digest();
        let signature = signature_service.request_signature(id.clone()).await;
        Self {
            id,
            signature,
            ..header
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the header id is well formed.
        ensure!(self.digest() == self.id, DagError::InvalidHeaderId);

        // One digest must have exactly one edge type.
        ensure!(
            self.parents.is_disjoint(&self.weak_edges)
                && self.parents.is_disjoint(&self.virtual_edges)
                && self.weak_edges.is_disjoint(&self.virtual_edges),
            DagError::MalformedHeader(self.id.clone())
        );

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, DagError::UnknownAuthority(self.author));

        // Ensure all worker ids are correct.
        for worker_id in self.payload.values() {
            committee
                .worker(&self.author, &worker_id)
                .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
        }

        // Check the signature.
        self.signature
            .verify(&self.id, &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for Header {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.author);
        hasher.update(self.round.to_le_bytes());
        for (x, y) in &self.payload {
            hasher.update(x);
            hasher.update(y.to_le_bytes());
        }
        hasher.update(b"strong-edges");
        hasher.update((self.parents.len() as u64).to_le_bytes());
        for x in &self.parents {
            hasher.update(x);
        }
        hasher.update(b"weak-edges");
        hasher.update((self.weak_edges.len() as u64).to_le_bytes());
        for x in &self.weak_edges {
            hasher.update(x);
        }
        hasher.update(b"virtual-edges");
        hasher.update((self.virtual_edges.len() as u64).to_le_bytes());
        for x in &self.virtual_edges {
            hasher.update(x);
        }
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}({}, {})",
            self.id,
            self.round,
            self.author,
            self.payload.keys().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.round, self.author)
    }
}

/// Bound the data portion so a complete proposal fits the network frame limit.
pub const MAX_PROPOSAL_BYTES: usize = 4 * 1024 * 1024;
/// Leave room for the message tag below the transport's 8 MiB frame limit.
pub const MAX_PROPOSAL_WIRE_BYTES: u64 = 7 * 1024 * 1024;

pub fn batch_digest(bytes: &[u8]) -> Digest {
    Digest(Sha512::digest(bytes).as_slice()[..32].try_into().unwrap())
}

pub fn batch_key(digest: &Digest) -> Vec<u8> {
    [b"orca-batch-v2/".as_ref(), digest.as_ref()].concat()
}

pub fn grade_one_key(digest: &Digest) -> Vec<u8> {
    [b"orca-ready-v2/".as_ref(), digest.as_ref()].concat()
}

pub fn proposal_key(digest: &Digest) -> Vec<u8> {
    [b"orca-proposal-v2/".as_ref(), digest.as_ref()].concat()
}

pub fn local_votes_key(digest: &Digest) -> Vec<u8> {
    [b"orca-local-votes-v2/".as_ref(), digest.as_ref()].concat()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Proposal {
    pub header: Header,
    pub batches: BTreeMap<Digest, Vec<u8>>,
}

impl Proposal {
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        self.header.verify(committee)?;
        ensure!(
            bincode::serialized_size(self)? <= MAX_PROPOSAL_WIRE_BYTES,
            DagError::MalformedHeader(self.header.id.clone())
        );
        ensure!(
            self.batches.len() == self.header.payload.len()
                && self.batches.values().map(Vec::len).sum::<usize>() <= MAX_PROPOSAL_BYTES,
            DagError::MalformedHeader(self.header.id.clone())
        );
        for digest in self.header.payload.keys() {
            ensure!(
                self.batches
                    .get(digest)
                    .map_or(false, |bytes| batch_digest(bytes) == *digest),
                DagError::MalformedHeader(self.header.id.clone())
            );
        }
        Ok(())
    }

    pub fn block(&self) -> Certificate {
        Certificate {
            header: self.header.clone(),
            votes: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum GrbcPhase {
    Echo,
    Ready,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrbcVote {
    pub id: Digest,
    pub round: Round,
    pub origin: PublicKey,
    pub author: PublicKey,
    pub phase: GrbcPhase,
    pub signature: Signature,
}

impl GrbcVote {
    pub async fn new(
        header: &Header,
        author: PublicKey,
        phase: GrbcPhase,
        signature_service: &mut SignatureService,
    ) -> Self {
        let mut vote = Self {
            id: header.id.clone(),
            round: header.round,
            origin: header.author,
            author,
            phase,
            signature: Signature::default(),
        };
        vote.signature = signature_service.request_signature(vote.digest()).await;
        vote
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        ensure!(
            committee.stake(&self.origin) > 0,
            DagError::UnknownAuthority(self.origin)
        );
        self.signature
            .verify(&self.digest(), &self.author)
            .map_err(DagError::from)
    }

    pub fn block_digest(&self) -> Digest {
        Certificate {
            header: Header {
                id: self.id.clone(),
                round: self.round,
                author: self.origin,
                ..Header::default()
            },
            votes: Vec::new(),
        }
        .digest()
    }
}

impl Hash for GrbcVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(b"orca-grbc-v2");
        hasher.update([match self.phase {
            GrbcPhase::Echo => 0,
            GrbcPhase::Ready => 1,
        }]);
        hasher.update(&self.id);
        hasher.update(self.round.to_le_bytes());
        hasher.update(&self.origin);
        hasher.update(&self.author);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

/// Batches contain votes by one sender, not a quorum certificate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrbcVoteBatch {
    pub round: Round,
    pub author: PublicKey,
    pub phase: GrbcPhase,
    pub votes: Vec<(Digest, PublicKey, Signature)>,
}

impl GrbcVoteBatch {
    pub fn from_votes(votes: Vec<GrbcVote>) -> Option<Self> {
        let first = votes.first()?;
        let (round, author, phase) = (first.round, first.author, first.phase);
        if votes
            .iter()
            .any(|v| (v.round, v.author, v.phase) != (round, author, phase))
        {
            return None;
        }
        Some(Self {
            round,
            author,
            phase,
            votes: votes
                .into_iter()
                .map(|v| (v.id, v.origin, v.signature))
                .collect(),
        })
    }

    pub fn into_votes(self) -> Vec<GrbcVote> {
        let (round, author, phase) = (self.round, self.author, self.phase);
        self.votes
            .into_iter()
            .map(|(id, origin, signature)| GrbcVote {
                id,
                origin,
                round,
                author,
                phase,
                signature,
            })
            .collect()
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
/// Legacy name for a LOCAL delivery record. Never sent as a GRBC proof.
pub struct Certificate {
    pub header: Header,
    pub votes: Vec<(PublicKey, Signature)>,
}

/// Delivery events emitted by the primary to the consensus layer.
#[derive(Clone, Debug)]
pub enum ConsensusMessage {
    /// The proposer can enter this round after a strong-parent quorum formed.
    RoundAdvanced(Round),
    /// Complete author-signed data observed before DAG dependency validation or
    /// local GRBC delivery, for the experimental early commit/traversal path.
    Observed(Header),
    GradeOne(Certificate),
    GradeTwo(Certificate),
    Aba(PublicKey, Vec<u8>),
    AbaBatch(PublicKey, Vec<Vec<u8>>),
}

#[derive(Debug)]
pub enum ConsensusCommand {
    Cleanup(Certificate),
    CleanupBatch(Vec<Certificate>),
    AbaBroadcast(Vec<Vec<u8>>),
    LeaderRequest(Round, PublicKey),
}

impl Certificate {
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities
            .keys()
            .map(|name| Self {
                header: Header {
                    author: *name,
                    ..Header::default()
                },
                ..Self::default()
            })
            .collect()
    }

    pub fn round(&self) -> Round {
        self.header.round
    }

    pub fn origin(&self) -> PublicKey {
        self.header.author
    }
}

impl Hash for Certificate {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.header.id);
        hasher.update(self.round().to_le_bytes());
        hasher.update(&self.origin());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: C{}({}, {})",
            self.digest(),
            self.round(),
            self.origin(),
            self.header.id
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header.id == other.header.id;
        ret &= self.round() == other.round();
        ret &= self.origin() == other.origin();
        ret
    }
}
