// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings, Arg};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .arg(
            Arg::with_name("silence-schedule")
                .long("silence-schedule")
                .takes_value(true)
                .default_value("")
                .help("Pre-generated client silence bitmap (1 pauses, 0 sends)"),
        )
        .arg(
            Arg::with_name("silence-slot-ms")
                .long("silence-slot-ms")
                .takes_value(true)
                .default_value("0")
                .help("Duration of each pre-generated silence slot"),
        )
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;
    let silence_schedule = matches
        .value_of("silence-schedule")
        .unwrap()
        .as_bytes()
        .to_vec();
    if silence_schedule
        .iter()
        .any(|value| !matches!(value, b'0' | b'1'))
    {
        return Err(anyhow::Error::msg(
            "Silence schedule must contain only 0 and 1",
        ));
    }
    let silence_slot_ms = matches
        .value_of("silence-slot-ms")
        .unwrap()
        .parse::<u64>()
        .context("Silence slot duration must be a non-negative integer")?;
    if !silence_schedule.is_empty() && silence_slot_ms == 0 {
        return Err(anyhow::Error::msg(
            "A non-empty silence schedule requires a positive slot duration",
        ));
    }

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let client = Client {
        target,
        size,
        rate,
        nodes,
        silence_schedule,
        silence_slot_ms,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
    silence_schedule: Vec<u8>,
    silence_slot_ms: u64,
}

fn client_is_silent(schedule: &[u8], slot_ms: u64, elapsed: Duration) -> bool {
    if schedule.is_empty() || slot_ms == 0 {
        return false;
    }
    let slot = (elapsed.as_millis() / slot_ms as u128) as usize;
    schedule.get(slot) == Some(&b'1')
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");
        let schedule_start = Instant::now();

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            if client_is_silent(
                &self.silence_schedule,
                self.silence_slot_ms,
                schedule_start.elapsed(),
            ) {
                continue;
            }

            for x in 0..burst {
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", counter);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                };

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follows_pre_generated_silence_slots() {
        let schedule = b"101";
        assert!(client_is_silent(schedule, 200, Duration::from_millis(0)));
        assert!(!client_is_silent(schedule, 200, Duration::from_millis(200)));
        assert!(client_is_silent(schedule, 200, Duration::from_millis(400)));
        assert!(!client_is_silent(schedule, 200, Duration::from_millis(600)));
    }
}
