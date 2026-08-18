// Copyright(C) Facebook, Inc. and its affiliates.
use crate::adversary;
use crate::messages::{Certificate, Header};
use crate::primary::{PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use network::{CancelHandler, ReliableSender};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// Strong parents, weak edges, virtual edges, and the completed round.
pub type ProposerMessage = (Vec<Digest>, Vec<Digest>, Vec<Digest>, Round);

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Authorities in the same deterministic order used by consensus to
    /// designate each round's leader.
    leader_schedule: Vec<PublicKey>,
    /// Number of dynamically silent authorities in each round.
    adversary_faults: usize,
    /// Makes the random adversary schedule reproducible.
    adversary_seed: u64,
    /// Cached decision for the current round.
    silent: bool,
    /// Local workers that must pause batch production with this proposer.
    worker_addresses: Vec<SocketAddr>,
    worker_network: ReliableSender,
    worker_handlers: Vec<CancelHandler>,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<ProposerMessage>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    last_weak_edges: Vec<Digest>,
    last_virtual_edges: Vec<Digest>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<ProposerMessage>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        tx_core: Sender<Header>,
    ) {
        let mut leader_schedule: Vec<_> = committee.authorities.keys().cloned().collect();
        leader_schedule.sort();
        let adversary_faults = std::env::var("ORCA_FAULTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let adversary_seed = std::env::var("ORCA_ADVERSARY_SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let worker_addresses = committee
            .our_workers(&name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|worker| worker.primary_to_worker)
            .collect();
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();

        tokio::spawn(async move {
            Self {
                name,
                leader_schedule,
                adversary_faults,
                adversary_seed,
                silent: false,
                worker_addresses,
                worker_network: ReliableSender::new(),
                worker_handlers: Vec::new(),
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                last_parents: genesis,
                last_weak_edges: Vec::new(),
                last_virtual_edges: Vec::new(),
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    fn compute_silence(&self, round: Round) -> bool {
        if !adversary::selected(
            &self.name,
            &self.leader_schedule,
            round,
            self.adversary_faults,
            self.adversary_seed,
            None,
        ) {
            return false;
        }

        let leader = self.leader_schedule[round as usize % self.leader_schedule.len()];
        if self.name == leader {
            match std::env::var("ORCA_RULE3_BEHAVIOR").as_deref() {
                Ok("silent") => true,
                Ok("participate") => false,
                _ => adversary::mixed_silence(&self.name, round, self.adversary_seed),
            }
        } else {
            true
        }
    }

    async fn enter_round(&mut self, round: Round) {
        self.round = round;
        self.silent = self.compute_silence(round);
        if self.adversary_faults == 0 {
            return;
        }
        let message = PrimaryWorkerMessage::BatchSilent(round, self.silent);
        let bytes = bincode::serialize(&message).expect("Failed to serialize batch state");
        self.worker_handlers = self
            .worker_network
            .broadcast(self.worker_addresses.clone(), Bytes::from(bytes))
            .await;
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).collect(),
            self.last_weak_edges.drain(..).collect(),
            self.last_virtual_edges.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        info!(
            "Header created round {} digest {:?}",
            header.round,
            header.digest()
        );

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);
        self.enter_round(self.round).await;

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();
            if (timer_expired || enough_digests) && enough_parents {
                if self.silent {
                    // Consume this round's parent set so an elapsed timer does
                    // not spin. Keep payload digests for the next round.
                    self.last_parents.clear();
                    self.last_weak_edges.clear();
                    self.last_virtual_edges.clear();
                } else {
                    // Make a new header.
                    self.make_header().await;
                    self.payload_size = 0;
                }

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, weak_edges, virtual_edges, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    self.enter_round(round + 1).await;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
                    self.last_weak_edges = weak_edges;
                    self.last_virtual_edges = virtual_edges;
                }
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
