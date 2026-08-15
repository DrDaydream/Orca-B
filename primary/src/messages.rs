// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub id: Digest,
    pub round: Round,
    pub origin: PublicKey,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        header: &Header,
        author: &PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        let vote = Self {
            id: header.id.clone(),
            round: header.round,
            origin: header.author,
            author: *author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature
            .verify(&self.digest(), &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.id);
        hasher.update(self.round.to_le_bytes());
        hasher.update(&self.origin);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V{}({}, {})",
            self.digest(),
            self.round,
            self.author,
            self.id
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Certificate {
    pub header: Header,
    pub votes: Vec<(PublicKey, Signature)>,
}

/// The delivery level produced by a graded reliable broadcast instance.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Grade {
    One,
    Two,
}

/// A signed acknowledgement that an authority delivered a certificate at grade 1.
#[derive(Clone, Serialize, Deserialize)]
pub struct GradeVote {
    pub id: Digest,
    pub round: Round,
    pub origin: PublicKey,
    pub author: PublicKey,
    pub signature: Signature,
}

impl GradeVote {
    pub async fn new(
        certificate: &Certificate,
        author: &PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        let vote = Self {
            id: certificate.digest(),
            round: certificate.round(),
            origin: certificate.origin(),
            author: *author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
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

impl Hash for GradeVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        // Domain-separate grade votes from ordinary header votes.
        hasher.update(b"narwhal-grbc-grade-vote-v1");
        hasher.update(&self.id);
        hasher.update(self.round.to_le_bytes());
        hasher.update(&self.origin);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for GradeVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "G{}({}, {})", self.round, self.author, self.id)
    }
}

/// Proof that more than two thirds of the committee delivered the same certificate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GradedCertificate {
    pub certificate: Certificate,
    pub grade: Grade,
    pub votes: Vec<(PublicKey, Signature)>,
}

/// Delivery events emitted by the primary to the consensus layer.
#[derive(Clone, Debug)]
pub enum ConsensusMessage {
    GradeOne(Certificate),
    GradeTwo(Certificate),
    Aba(PublicKey, Vec<u8>),
    AbaBatch(PublicKey, Vec<Vec<u8>>),
}

#[derive(Debug)]
pub enum ConsensusCommand {
    Cleanup(Certificate),
    AbaBroadcast(Vec<Vec<u8>>),
    LeaderRequest(Round, PublicKey),
}

impl GradedCertificate {
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        self.certificate.verify(committee)?;
        ensure!(self.grade == Grade::Two, DagError::InvalidGrade);

        let template = GradeVote {
            id: self.certificate.digest(),
            round: self.certificate.round(),
            origin: self.certificate.origin(),
            author: PublicKey::default(),
            signature: Signature::default(),
        };
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in &self.votes {
            ensure!(used.insert(*name), DagError::AuthorityReuse(*name));
            let stake = committee.stake(name);
            ensure!(stake > 0, DagError::UnknownAuthority(*name));
            weight += stake;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            DagError::GradeRequiresQuorum
        );
        Signature::verify_batch(&template.digest(), &self.votes).map_err(DagError::from)
    }
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

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Genesis certificates are always valid.
        if Self::genesis(committee).contains(self) {
            return Ok(());
        }

        // Check the embedded header.
        self.header.verify(committee)?;

        // Ensure the certificate has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), DagError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, DagError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        // Check the signatures.
        Signature::verify_batch(&self.digest(), &self.votes).map_err(DagError::from)
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
