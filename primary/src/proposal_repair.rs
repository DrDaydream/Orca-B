//! Track queued proposals without treating arrival order as evidence of loss.
use crate::messages::GrbcVote;
use crate::primary::{PrimaryMessage, Round};
use crypto::{Digest, PublicKey};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, Instant};

/// Only a missing-data request waits for this grace, never ECHO or READY.
pub(crate) const REPAIR_GRACE: Duration = Duration::from_millis(50);
pub(crate) const REPAIR_RETRY: Duration = Duration::from_secs(1);
const MAX_PROCESSING_WAIT: Duration = Duration::from_secs(1);

#[derive(Clone, Default, Debug)]
pub(crate) struct ProposalTracker(Arc<Mutex<HashMap<Digest, (usize, Instant)>>>);

impl ProposalTracker {
    pub(crate) fn track(&self, id: Digest) -> ProposalGuard {
        let mut pending = self.0.lock().expect("Proposal tracker poisoned");
        let entry = pending.entry(id.clone()).or_insert((0, Instant::now()));
        entry.0 += 1;
        ProposalGuard {
            tracker: self.clone(),
            id,
        }
    }

    fn processing_deadline(&self, id: &Digest) -> Option<Instant> {
        self.0
            .lock()
            .expect("Proposal tracker poisoned")
            .get(id)
            .map(|(_, started)| *started + MAX_PROCESSING_WAIT)
    }
}

/// Dropped on success, invalid data, queue closure, or processing cancellation.
#[derive(Debug)]
pub(crate) struct ProposalGuard {
    tracker: ProposalTracker,
    id: Digest,
}

impl Drop for ProposalGuard {
    fn drop(&mut self) {
        let mut pending = self.tracker.0.lock().expect("Proposal tracker poisoned");
        if let Some((count, _)) = pending.get_mut(&self.id) {
            *count -= 1;
            if *count == 0 {
                pending.remove(&self.id);
            }
        }
    }
}

/// Internal queue item; the network protocol still uses PrimaryMessage.
#[derive(Debug)]
pub(crate) struct PrimaryInput {
    pub message: PrimaryMessage,
    pub guard: Option<ProposalGuard>,
}

impl PrimaryInput {
    pub(crate) fn new(message: PrimaryMessage, tracker: &ProposalTracker) -> Self {
        let guard = match &message {
            PrimaryMessage::Proposal(proposal) => Some(tracker.track(proposal.header.id.clone())),
            _ => None,
        };
        Self { message, guard }
    }
}

pub(crate) struct PendingRepair {
    pub round: Round,
    pub deadline: Instant,
    id: Digest,
    peer: PublicKey,
    created: Instant,
    requested: bool,
}

impl PendingRepair {
    pub(crate) fn new(vote: &GrbcVote, now: Instant) -> Self {
        Self {
            round: vote.round,
            id: vote.id.clone(),
            peer: vote.author,
            deadline: now + REPAIR_GRACE,
            created: now,
            requested: false,
        }
    }

    /// None: defer. Some(Some(peer)): first request to the authenticated voter.
    /// Some(None): retry through the committee if that peer did not supply data.
    pub(crate) fn due(
        &mut self,
        tracker: &ProposalTracker,
        now: Instant,
    ) -> Option<Option<PublicKey>> {
        if now < self.deadline {
            return None;
        }
        if let Some(deadline) = tracker.processing_deadline(&self.id) {
            // Repeated, even invalid, arrivals cannot renew this bound forever.
            let deadline = deadline.min(self.created + MAX_PROCESSING_WAIT);
            if now < deadline {
                self.deadline = deadline;
                return None;
            }
        }
        let target = if self.requested {
            None
        } else {
            Some(self.peer)
        };
        self.requested = true;
        self.deadline = now + REPAIR_RETRY;
        Some(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{proposal, votes};
    use crate::messages::GrbcPhase;

    #[test]
    fn duplicate_arrivals_cannot_suppress_recovery_forever() {
        let vote = votes(&proposal().header, GrbcPhase::Echo).remove(1);
        let tracker = ProposalTracker::default();
        let first = tracker.track(vote.id.clone());
        let second = tracker.track(vote.id.clone());
        let now = Instant::now();
        let mut repair = PendingRepair::new(&vote, now);
        assert_eq!(repair.due(&tracker, now + REPAIR_GRACE), None);
        drop(first);
        assert!(tracker.processing_deadline(&vote.id).is_some());
        let deadline = repair.deadline;
        assert_eq!(repair.due(&tracker, deadline), Some(Some(vote.author)));
        assert_eq!(repair.due(&tracker, deadline + REPAIR_RETRY), Some(None));
        drop(second);
        assert!(tracker.processing_deadline(&vote.id).is_none());
    }

    #[test]
    fn rejected_or_dropped_proposal_releases_processing_marker() {
        let p = proposal();
        let tracker = ProposalTracker::default();
        let input = PrimaryInput::new(PrimaryMessage::Proposal(p.clone()), &tracker);
        assert!(tracker.processing_deadline(&p.header.id).is_some());
        drop(input);
        assert!(tracker.processing_deadline(&p.header.id).is_none());
    }
}
