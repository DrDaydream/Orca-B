//! Explicit benchmark time is shared by every client, independent of SSH launch.
use anyhow::{ensure, Context, Result};
use log::info;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration, Instant};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartPlan {
    pub schema: u8,
    pub run_id: String,
    pub load_us: u64,
    pub start_us: u64,
    pub end_us: u64,
    pub drain_end_us: u64,
}

pub fn now_us() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64)
}

impl StartPlan {
    fn validate(&self, now: u64) -> Result<()> {
        ensure!(self.schema == 2, "Unsupported benchmark clock schema");
        ensure!(
            !self.run_id.is_empty()
                && self.run_id.len() <= 128
                && self
                    .run_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "Invalid benchmark run_id"
        );
        ensure!(
            self.load_us <= self.start_us
                && self.start_us < self.end_us
                && self.end_us <= self.drain_end_us
                && self.drain_end_us < (1u64 << 53),
            "Invalid benchmark clock interval"
        );
        ensure!(
            self.load_us > now.saturating_add(5_000_000),
            "Benchmark start plan missed the 5 second arming guard"
        );
        Ok(())
    }
}

pub async fn wait_until(target_us: u64) -> Result<()> {
    loop {
        let now = now_us()?;
        if now >= target_us {
            return Ok(());
        }
        // Recheck the wall clock so a pre-start clock correction is not hidden
        // behind one long monotonic sleep. The runner validates actual lateness.
        sleep(Duration::from_micros((target_us - now).min(100_000))).await;
    }
}

pub async fn arm_and_wait(path: &Path) -> Result<StartPlan> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let plan: StartPlan = loop {
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                break serde_json::from_slice(&bytes).context("Invalid benchmark start plan")?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ensure!(
                    Instant::now() < deadline,
                    "No benchmark start plan within 120 seconds"
                );
                sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    plan.validate(now_us()?)?;
    info!("Benchmark clock armed {}", serde_json::to_string(&plan)?);
    wait_until(plan.load_us).await?;
    let now = now_us()?;
    ensure!(
        now >= plan.load_us && now - plan.load_us <= 500_000,
        "Benchmark input start is more than 500ms late"
    );
    info!(
        "Benchmark input started at_us {} lateness_us {}",
        now,
        now - plan.load_us
    );
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> StartPlan {
        StartPlan {
            schema: 2,
            run_id: "clock-test".into(),
            load_us: 10_000_000,
            start_us: 12_000_000,
            end_us: 15_000_000,
            drain_end_us: 18_000_000,
        }
    }

    #[test]
    fn rejects_stale_invalid_or_reordered_plans() {
        assert!(plan().validate(4_999_999).is_ok());
        assert!(plan().validate(5_000_000).is_err());
        assert!(StartPlan {
            schema: 1,
            ..plan()
        }
        .validate(0)
        .is_err());
        assert!(StartPlan {
            start_us: 9_000_000,
            ..plan()
        }
        .validate(0)
        .is_err());
        assert!(StartPlan {
            end_us: 12_000_000,
            ..plan()
        }
        .validate(0)
        .is_err());
        assert!(StartPlan {
            drain_end_us: 14_000_000,
            ..plan()
        }
        .validate(0)
        .is_err());
        assert!(StartPlan {
            run_id: "\nforged-log".into(),
            ..plan()
        }
        .validate(0)
        .is_err());
    }
}
