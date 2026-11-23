//! Optional bounded scheduling at the transport boundary.
use crate::ReliableSender;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use log::info;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};
use tokio::time::{sleep_until, Instant};

pub enum Delivery {
    Send,
    Drop(String),
    Delay(Duration, String),
}

pub trait DeliveryPolicy: Send + Sync + 'static {
    fn delivery(&self, address: SocketAddr, data: &Bytes) -> Delivery;
}

#[derive(Clone)]
pub struct DelayBudget {
    limits: (usize, usize),
    used: Arc<Mutex<(usize, usize)>>,
}

impl DelayBudget {
    pub fn new(messages: usize, bytes: usize) -> Self {
        Self {
            limits: (messages, bytes),
            used: Arc::new(Mutex::new((0, 0))),
        }
    }

    fn reserve(&self, bytes: usize) -> bool {
        let mut used = self.used.lock().unwrap();
        if used.0 >= self.limits.0 || bytes > self.limits.1.saturating_sub(used.1) {
            return false;
        }
        used.0 += 1;
        used.1 += bytes;
        true
    }
    fn release(&self, bytes: usize) {
        let mut used = self.used.lock().unwrap();
        used.0 -= 1;
        used.1 -= bytes;
    }
}

pub(crate) struct Outbound {
    pub address: SocketAddr,
    pub data: Bytes,
    pub acknowledgement: Option<oneshot::Sender<Bytes>>,
}

struct Pending {
    deadline: Instant,
    outbound: Outbound,
    trace: String,
}

#[derive(Default)]
struct Queue {
    pending: VecDeque<Pending>,
    closed: bool,
}

/// One timer task per sender. The budget can be shared by all transports of an
/// authority. No timer, task, or unbounded channel is created per message.
pub(crate) struct Scheduler {
    policy: Arc<dyn DeliveryPolicy>,
    budget: DelayBudget,
    queue: Arc<Mutex<Queue>>,
    changed: Arc<Notify>,
}

impl Scheduler {
    pub fn new(policy: Arc<dyn DeliveryPolicy>, budget: DelayBudget) -> Self {
        Self::start(policy, budget, ReliableSender::new())
    }

    fn start(
        policy: Arc<dyn DeliveryPolicy>,
        budget: DelayBudget,
        mut reliable: ReliableSender,
    ) -> Self {
        let queue = Arc::new(Mutex::new(Queue::default()));
        let changed = Arc::new(Notify::new());
        let result = Self {
            policy,
            budget: budget.clone(),
            queue: queue.clone(),
            changed: changed.clone(),
        };
        tokio::spawn(async move {
            let mut acknowledgements = FuturesUnordered::<BoxFuture<'static, ()>>::new();
            loop {
                let notified = changed.notified();
                let (ready, deadline, closed) = {
                    let mut queue = queue.lock().unwrap();
                    let ready = if queue
                        .pending
                        .front()
                        .map_or(false, |p| p.deadline <= Instant::now())
                    {
                        queue.pending.pop_front()
                    } else {
                        None
                    };
                    (
                        ready,
                        queue.pending.front().map(|p| p.deadline),
                        queue.closed,
                    )
                };
                if let Some(pending) = ready {
                    let Outbound {
                        address,
                        data,
                        acknowledgement,
                    } = pending.outbound;
                    let bytes = data.len();
                    info!(
                        "BENCH_ADVERSARY {} action=release reason=deadline bytes={}",
                        pending.trace, bytes
                    );
                    // Primary handlers ACK both broadcasts and repair replies.
                    // Hold every scheduled frame (including SimpleSender repair)
                    // in the shared budget through the actual transport ACK.
                    // The independent handle also survives caller-side round GC.
                    let received = reliable.scheduled_send(address, data);
                    let budget = budget.clone();
                    acknowledgements.push(
                        async move {
                            if let Ok(ack) = received.await {
                                if let Some(acknowledgement) = acknowledgement {
                                    let _ = acknowledgement.send(ack);
                                }
                            }
                            budget.release(bytes);
                        }
                        .boxed(),
                    );
                    continue;
                }
                match deadline {
                    Some(deadline) => {
                        tokio::select! { _ = sleep_until(deadline) => {}, _ = notified => {}, Some(_) = acknowledgements.next(), if !acknowledgements.is_empty() => {} }
                    }
                    None if closed && acknowledgements.is_empty() => break,
                    None => {
                        tokio::select! { _ = notified => {}, Some(_) = acknowledgements.next(), if !acknowledgements.is_empty() => {} }
                    }
                }
            }
        });
        result
    }

    /// None means suppressed or retained; Some must go directly to transport.
    pub fn submit(&self, outbound: Outbound) -> Option<Outbound> {
        match self.policy.delivery(outbound.address, &outbound.data) {
            Delivery::Send => Some(outbound),
            Delivery::Drop(trace) => {
                info!("BENCH_ADVERSARY {} action=drop reason=fixed-silent", trace);
                None
            }
            Delivery::Delay(delay, trace) => {
                if !self.budget.reserve(outbound.data.len()) {
                    info!(
                        "BENCH_ADVERSARY {} action=release reason=budget-exhausted bytes={}",
                        trace,
                        outbound.data.len()
                    );
                    return Some(outbound);
                }
                info!(
                    "BENCH_ADVERSARY {} action=delay reason=leader-evidence delay_ms={} bytes={}",
                    trace,
                    delay.as_millis(),
                    outbound.data.len()
                );
                let pending = Pending {
                    deadline: Instant::now() + delay,
                    outbound,
                    trace,
                };
                let mut queue = self.queue.lock().unwrap();
                let position = queue
                    .pending
                    .iter()
                    .position(|p| p.deadline > pending.deadline)
                    .unwrap_or(queue.pending.len());
                queue.pending.insert(position, pending);
                drop(queue);
                self.changed.notify_one();
                None
            }
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.queue.lock().unwrap().closed = true;
        self.changed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleSender;
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    struct Policy;
    impl DeliveryPolicy for Policy {
        fn delivery(&self, _: SocketAddr, data: &Bytes) -> Delivery {
            match data[0] {
                b'd' => Delivery::Delay(Duration::from_millis(150), "test-delay".into()),
                b'x' => Delivery::Drop("test-silence".into()),
                _ => Delivery::Send,
            }
        }
    }

    #[tokio::test]
    async fn saturated_peer_does_not_block_another_peers_deadline() {
        let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled = "127.0.0.1:1".parse().unwrap();
        let (sender, _stalled_receiver) =
            crate::reliable_sender::reliable_sender_tests::saturated_sender(stalled);
        let budget = DelayBudget::new(2, 1024);
        let scheduler = Scheduler::start(Arc::new(Policy), budget.clone(), sender);
        assert!(scheduler
            .submit(Outbound {
                address: stalled,
                data: Bytes::from_static(b"delayed-stalled"),
                acknowledgement: None
            })
            .is_none());
        assert!(scheduler
            .submit(Outbound {
                address: healthy.local_addr().unwrap(),
                data: Bytes::from_static(b"delayed-healthy"),
                acknowledgement: None
            })
            .is_none());
        let (socket, _) = timeout(Duration::from_millis(500), healthy.accept())
            .await
            .expect("a saturated peer must not stall another deadline")
            .unwrap();
        let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
        assert_eq!(
            stream.next().await.unwrap().unwrap().as_ref(),
            b"delayed-healthy"
        );
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
        timeout(Duration::from_secs(1), async {
            while budget.used.lock().unwrap().0 != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            budget.used.lock().unwrap().0,
            1,
            "the stalled release remains in the shared budget"
        );
    }

    #[test]
    fn shared_budget_enforces_count_and_bytes_and_recovers_on_release() {
        let first = DelayBudget::new(2, 8);
        let second = first.clone();
        assert!(first.reserve(5));
        assert!(
            !second.reserve(4),
            "a second sender must share the byte budget"
        );
        assert!(second.reserve(3));
        assert!(
            !first.reserve(0),
            "zero-byte frames still count as messages"
        );
        first.release(5);
        assert!(second.reserve(5));
        assert!(!first.reserve(usize::MAX));
    }

    #[tokio::test]
    async fn delayed_reliable_message_releases_without_another_ingress() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut sender = ReliableSender::with_schedule(Arc::new(Policy), DelayBudget::new(2, 1024));
        let start = Instant::now();
        let ack = sender
            .send(
                listener.local_addr().unwrap(),
                Bytes::from_static(b"delayed"),
            )
            .await;
        assert!(timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err());
        let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
        assert_eq!(stream.next().await.unwrap().unwrap().as_ref(), b"delayed");
        assert!(start.elapsed() >= Duration::from_millis(150));
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
        assert_eq!(ack.await.unwrap(), Bytes::from_static(b"Ack"));
    }

    #[tokio::test]
    async fn consensus_cancellation_cannot_erase_a_delayed_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut sender = ReliableSender::with_schedule(Arc::new(Policy), DelayBudget::new(2, 1024));
        let acknowledgement = sender
            .send(
                listener.local_addr().unwrap(),
                Bytes::from_static(b"delayed-old-round"),
            )
            .await;
        // Consensus normally drops these handles when later rounds advance GC.
        drop(acknowledgement);
        let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
        assert_eq!(
            stream.next().await.unwrap().unwrap().as_ref(),
            b"delayed-old-round"
        );
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
    }

    #[tokio::test]
    async fn delayed_repair_keeps_budget_until_transport_acknowledges() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let budget = DelayBudget::new(1, 1024);
        let mut sender = SimpleSender::with_schedule(Arc::new(Policy), budget.clone());
        sender
            .send(
                listener.local_addr().unwrap(),
                Bytes::from_static(b"delayed-repair"),
            )
            .await;
        let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut stream = Framed::new(socket, LengthDelimitedCodec::new());
        assert_eq!(
            stream.next().await.unwrap().unwrap().as_ref(),
            b"delayed-repair"
        );
        assert_eq!(
            *budget.used.lock().unwrap(),
            (1, 14),
            "retained repair bytes must count until ACK"
        );
        stream.send(Bytes::from_static(b"Ack")).await.unwrap();
        timeout(Duration::from_secs(1), async {
            while budget.used.lock().unwrap().0 != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn silence_drops_simple_and_reliable_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let budget = DelayBudget::new(2, 1024);
        let mut reliable = ReliableSender::with_schedule(Arc::new(Policy), budget.clone());
        let mut simple = SimpleSender::with_schedule(Arc::new(Policy), budget);
        let ack = reliable
            .send(listener.local_addr().unwrap(), Bytes::from_static(b"x"))
            .await;
        simple
            .send(listener.local_addr().unwrap(), Bytes::from_static(b"x"))
            .await;
        assert!(timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err());
        assert!(ack.await.is_err());
    }

    #[tokio::test]
    async fn later_messages_and_budget_saturation_do_not_discard_pending_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let budget = DelayBudget::new(1, 16);
        let mut sender = SimpleSender::with_schedule(Arc::new(Policy), budget.clone());
        let address = listener.local_addr().unwrap();
        sender
            .send(address, Bytes::from_static(b"delayed-old"))
            .await;
        sender
            .send(address, Bytes::from_static(b"delayed-new"))
            .await;
        let (socket, _) = timeout(Duration::from_millis(100), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut immediate = Framed::new(socket, LengthDelimitedCodec::new());
        assert_eq!(
            immediate.next().await.unwrap().unwrap().as_ref(),
            b"delayed-new"
        );
        let (socket, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut delayed = Framed::new(socket, LengthDelimitedCodec::new());
        assert_eq!(
            delayed.next().await.unwrap().unwrap().as_ref(),
            b"delayed-old"
        );
        delayed.send(Bytes::from_static(b"Ack")).await.unwrap();
        timeout(Duration::from_secs(1), async {
            while budget.used.lock().unwrap().0 != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(*budget.used.lock().unwrap(), (0, 0));
    }
}
