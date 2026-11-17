// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use crate::scheduler::{Outbound, Scheduler};
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{info, warn};
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/reliable_sender_tests.rs"]
pub mod reliable_sender_tests;

/// Convenient alias for cancel handlers returned to the caller task.
pub type CancelHandler = oneshot::Receiver<Bytes>;

/// We keep alive one TCP connection per peer, each connection is handled by a separate task (called `Connection`).
/// We communicate with our 'connections' through a dedicated channel kept by the HashMap called `connections`.
/// This sender is 'reliable' in the sense that it keeps trying to re-transmit messages for which it didn't
/// receive an ACK back (until they succeed or are canceled).
pub struct ReliableSender {
    scheduler: Option<Scheduler>,
    /// A map holding the channels to our connections.
    connections: HashMap<SocketAddr, Sender<InnerMessage>>,
    /// Small RNG just used to shuffle nodes and randomize connections (not crypto related).
    rng: SmallRng,
    /// Optional observation of complete frames successfully written to peers.
    sent_bytes: Option<Arc<AtomicU64>>,
}

impl std::default::Default for ReliableSender {
    fn default() -> Self {
        Self::new()
    }
}

impl ReliableSender {
    pub fn with_schedule(
        policy: std::sync::Arc<dyn crate::DeliveryPolicy>,
        budget: crate::DelayBudget,
    ) -> Self {
        let mut sender = Self::new();
        sender.scheduler = Some(Scheduler::new(policy, budget));
        sender
    }

    pub fn new() -> Self {
        Self {
            scheduler: None,
            connections: HashMap::new(),
            rng: SmallRng::from_entropy(),
            sent_bytes: None,
        }
    }

    /// Count each successful transport send, including retries, in bytes. This
    /// includes the default codec's four-byte length prefix and the full payload,
    /// but excludes ACKs, TCP/IP overhead, and partial writes from failed sends.
    pub fn with_sent_byte_counter(sent_bytes: Arc<AtomicU64>) -> Self {
        Self {
            sent_bytes: Some(sent_bytes),
            ..Self::new()
        }
    }

    pub fn with_schedule_and_counter(
        policy: Arc<dyn crate::DeliveryPolicy>,
        budget: crate::DelayBudget,
        sent_bytes: Arc<AtomicU64>,
    ) -> Self {
        let mut sender = Self::with_sent_byte_counter(sent_bytes.clone());
        sender.scheduler = Some(Scheduler::start(
            policy,
            budget,
            Self::with_sent_byte_counter(sent_bytes),
        ));
        sender
    }

    /// Helper function to spawn a new connection.
    fn spawn_connection(
        address: SocketAddr,
        sent_bytes: Option<Arc<AtomicU64>>,
    ) -> Sender<InnerMessage> {
        let (tx, rx) = channel(1_000);
        Connection::spawn(address, rx, sent_bytes);
        tx
    }

    /// Reliably send a message to a specific address.
    pub async fn send(&mut self, address: SocketAddr, data: Bytes) -> CancelHandler {
        let (sender, receiver) = oneshot::channel();
        let outbound = Outbound {
            address,
            data,
            acknowledgement: Some(sender),
        };
        let outbound = match &self.scheduler {
            Some(scheduler) => scheduler.submit(outbound),
            None => Some(outbound),
        };
        if let Some(outbound) = outbound {
            self.send_inner(
                outbound.address,
                outbound.data,
                outbound.acknowledgement.unwrap(),
            )
            .await;
        }
        receiver
    }

    pub(crate) async fn send_inner(
        &mut self,
        address: SocketAddr,
        data: Bytes,
        sender: oneshot::Sender<Bytes>,
    ) {
        let sent_bytes = self.sent_bytes.clone();
        self.connections
            .entry(address)
            .or_insert_with(|| Self::spawn_connection(address, sent_bytes))
            .send(InnerMessage {
                data,
                cancel_handler: sender,
            })
            .await
            .expect("Failed to send internal message");
    }

    /// Own the bounded connection queue without borrowing the sender while
    /// waiting for capacity. The scheduler can poll all peers concurrently.
    pub(crate) fn scheduled_send(
        &mut self,
        address: SocketAddr,
        data: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, oneshot::error::RecvError>> + Send + 'static
    {
        let sent_bytes = self.sent_bytes.clone();
        let tx = self
            .connections
            .entry(address)
            .or_insert_with(|| Self::spawn_connection(address, sent_bytes))
            .clone();
        let (sender, receiver) = oneshot::channel();
        async move {
            let _ = tx
                .send(InnerMessage {
                    data,
                    cancel_handler: sender,
                })
                .await;
            receiver.await
        }
    }

    /// Broadcast the message to all specified addresses in a reliable manner. It returns a vector of
    /// cancel handlers ordered as the input `addresses` vector.
    pub async fn broadcast(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
    ) -> Vec<CancelHandler> {
        let mut handlers = Vec::new();
        for address in addresses {
            let handler = self.send(address, data.clone()).await;
            handlers.push(handler);
        }
        handlers
    }

    /// Pick a few addresses at random (specified by `nodes`) and send the message only to them.
    /// It returns a vector of cancel handlers with no specific order.
    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) -> Vec<CancelHandler> {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }
}

/// Simple message used by `ReliableSender` to communicate with its connections.
#[derive(Debug)]
struct InnerMessage {
    /// The data to transmit.
    data: Bytes,
    /// The cancel handler allowing the caller task to cancel the transmission of this message
    /// and to be notified of its successfully transmission.
    cancel_handler: oneshot::Sender<Bytes>,
}

/// A connection is responsible to reliably establish (and keep alive) a connection with a single peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<InnerMessage>,
    /// The initial delay to wait before re-attempting a connection (in ms).
    retry_delay: u64,
    /// Buffer keeping all messages that need to be re-transmitted.
    buffer: VecDeque<(Bytes, oneshot::Sender<Bytes>)>,
    /// Shared with the sender, used only for observing successful frame writes.
    sent_bytes: Option<Arc<AtomicU64>>,
}

impl Connection {
    fn spawn(
        address: SocketAddr,
        receiver: Receiver<InnerMessage>,
        sent_bytes: Option<Arc<AtomicU64>>,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                retry_delay: 200,
                buffer: VecDeque::new(),
                sent_bytes,
            }
            .run()
            .await;
        });
    }

    /// Main loop trying to connect to the peer and transmit messages.
    async fn run(&mut self) {
        let mut delay = self.retry_delay;
        let mut retry = 0;
        loop {
            match TcpStream::connect(self.address).await {
                Ok(stream) => {
                    info!("Outgoing connection established with {}", self.address);

                    // Reset the delay.
                    delay = self.retry_delay;
                    retry = 0;

                    // Try to transmit all messages in the buffer and keep transmitting incoming messages.
                    // The following function only returns if there is an error.
                    let error = self.keep_alive(stream).await;
                    warn!("{}", error);
                }
                Err(e) => {
                    warn!("{}", NetworkError::FailedToConnect(self.address, retry, e));
                    let timer = sleep(Duration::from_millis(delay));
                    tokio::pin!(timer);

                    'waiter: loop {
                        tokio::select! {
                            // Wait an increasing delay before attempting to reconnect.
                            () = &mut timer => {
                                delay = min(2*delay, 60_000);
                                retry +=1;
                                break 'waiter;
                            },

                            // Drain the channel into the buffer to not saturate the channel and block the caller task.
                            // The caller is responsible to cleanup the buffer through the cancel handlers.
                            Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                                self.buffer.push_back((data, cancel_handler));
                                self.buffer.retain(|(_, handler)| !handler.is_closed());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Transmit messages once we have established a connection.
    async fn keep_alive(&mut self, stream: TcpStream) -> NetworkError {
        // This buffer keeps all messages and handlers that we have successfully transmitted but for
        // which we are still waiting to receive an ACK.
        let mut pending_replies = VecDeque::new();

        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();
        let error = 'connection: loop {
            // Try to send all messages of the buffer.
            while let Some((data, handler)) = self.buffer.pop_front() {
                // Skip messages that have been cancelled.
                if handler.is_closed() {
                    continue;
                }

                // Try to send the message.
                match writer.send(data.clone()).await {
                    Ok(()) => {
                        if let Some(sent_bytes) = &self.sent_bytes {
                            // SinkExt::send flushes the complete default
                            // LengthDelimitedCodec frame to this peer. A later
                            // successful retry is another transmission.
                            sent_bytes.fetch_add(data.len() as u64 + 4, Ordering::Relaxed);
                        }
                        // The message has been sent, we remove it from the buffer and add it to
                        // `pending_replies` while we wait for an ACK.
                        pending_replies.push_back((data, handler));
                    }
                    Err(e) => {
                        // We failed to send the message, we put it back into the buffer.
                        self.buffer.push_front((data, handler));
                        break 'connection NetworkError::FailedToSendMessage(self.address, e);
                    }
                }
            }

            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                    // Add the message to the buffer of messages to send.
                    self.buffer.push_back((data, cancel_handler));
                },
                response = reader.next() => {
                    let (data, handler) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => break 'connection NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Some(Ok(bytes)) => {
                            // Notify the handler that the message has been successfully sent.
                            let _ = handler.send(bytes.freeze());
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            // Put the message back in the buffer, we will try to send it again.
                            pending_replies.push_front((data, handler));
                            break 'connection NetworkError::FailedToReceiveAck(self.address);
                        }
                    }
                },
            }
        };

        // If we reach this code, it means something went wrong. Put the messages for which we didn't receive an ACK
        // back into the sending buffer, we will try to send them again once we manage to establish a new connection.
        while let Some(message) = pending_replies.pop_back() {
            self.buffer.push_front(message);
        }
        error
    }
}
