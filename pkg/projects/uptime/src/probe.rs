// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	IdentityId,
	value::value::{datetime::DateTime, duration::Duration},
};
use tokio::{
	select,
	sync::watch,
	time::{MissedTickBehavior, interval},
};
use tracing::{debug, warn};

use crate::{checks, error::ApiError, state::AppState, store, store::JobRow};

const HEARTBEAT_INTERVAL_NANOS: u64 = 10_000_000_000;

pub async fn run(st: AppState, probe: IdentityId, name: String, mut shutdown: watch::Receiver<bool>) {
	#[allow(clippy::disallowed_types)]
	let mut tick = interval(Duration::from_seconds(1).unwrap().to_std());
	tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
	let mut last_heartbeat: u64 = 0;

	loop {
		select! {
			_ = tick.tick() => {}
			_ = shutdown.changed() => break,
		}

		let now = st.clock.now().to_nanos();
		if now.saturating_sub(last_heartbeat) >= HEARTBEAT_INTERVAL_NANOS {
			if let Err(e) = store::probe_heartbeat(&st, probe, DateTime::from_nanos(now)).await {
				warn!("probe {name} heartbeat failed: {e:?}");
			}
			last_heartbeat = now;
		}

		let monitors = match store::pending_job_monitors(&st).await {
			Ok(monitors) => monitors,
			Err(e) => {
				warn!("probe {name} failed to list pending jobs: {e:?}");
				continue;
			}
		};
		for monitor_id in monitors {
			loop {
				let job = match store::claim_job(&st, monitor_id).await {
					Ok(Some(job)) => job,
					Ok(None) => break,
					Err(e) => {
						warn!("probe {name} claim failed: {e:?}");
						break;
					}
				};
				if let Err(e) = handle_job(&st, probe, &name, &job).await {
					warn!("probe {name} failed to handle job: {e:?}");
				}
				if *shutdown.borrow() {
					return;
				}
			}
			if *shutdown.borrow() {
				return;
			}
		}
	}
}

async fn handle_job(st: &AppState, probe: IdentityId, name: &str, job: &JobRow) -> Result<(), ApiError> {
	let Some(monitor) = store::find_monitor_any_owner(st, job.monitor_id).await? else {
		return Ok(());
	};
	let outcome = checks::run_check(st, &monitor).await;
	debug!(probe = %name, monitor = %monitor.name, success = outcome.success, "check completed");
	let checked_at = st.clock.now();
	store::report_result(st, &monitor, job.region_id, probe, checked_at, outcome).await
}
