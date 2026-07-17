// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::Arc};

use reifydb::{
	runtime::sync::mutex::Mutex,
	value::value::{datetime::DateTime, duration::Duration, uuid::Uuid7},
};
use tokio::{
	select,
	sync::{Semaphore, watch},
	time::{MissedTickBehavior, interval},
};
use tracing::{debug, warn};

use crate::{checks, state::AppState, store, store::MonitorRow};

pub async fn run(st: AppState, mut shutdown: watch::Receiver<bool>) {
	#[allow(clippy::disallowed_types)]
	let mut tick = interval(Duration::from_seconds(2).unwrap().to_std());
	tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

	let semaphore = Arc::new(Semaphore::new(st.cfg.max_concurrent_checks));
	let in_flight: Arc<Mutex<HashSet<Uuid7>>> = Arc::new(Mutex::new(HashSet::new()));

	loop {
		select! {
			_ = tick.tick() => {}
			_ = shutdown.changed() => break,
		}

		let monitors = match store::enabled_monitors(&st).await {
			Ok(monitors) => monitors,
			Err(e) => {
				warn!("scheduler failed to load monitors: {e:?}");
				continue;
			}
		};

		let now_nanos = st.clock.now_nanos();
		for monitor in monitors {
			if !due(&monitor, now_nanos) {
				continue;
			}
			if !in_flight.lock().insert(monitor.id) {
				continue;
			}
			let Ok(permit) = semaphore.clone().try_acquire_owned() else {
				in_flight.lock().remove(&monitor.id);
				continue;
			};

			let st = st.clone();
			let in_flight = in_flight.clone();
			st.tokio.clone().spawn(async move {
				let outcome = checks::run_check(&st, &monitor).await;
				debug!(
					monitor = %monitor.name,
					success = outcome.success,
					"check completed"
				);
				let checked_at = DateTime::from_nanos(st.clock.now_nanos());
				let response_time =
					outcome.response_time_ms.and_then(|ms| Duration::from_milliseconds(ms).ok());
				if let Err(e) = store::report_result(
					&st,
					&monitor,
					checked_at,
					outcome.success,
					response_time,
					outcome.status_code,
					outcome.error,
				)
				.await
				{
					warn!("failed to record check result for {}: {e:?}", monitor.name);
				}
				in_flight.lock().remove(&monitor.id);
				drop(permit);
			});
		}
	}
}

fn due(monitor: &MonitorRow, now_nanos: u64) -> bool {
	let Some(last) = &monitor.last_checked_at else {
		return true;
	};
	let Ok(last_nanos) = last.timestamp_nanos() else {
		return true;
	};
	let interval_nanos = monitor.interval.as_nanos().unwrap_or(i64::MAX);
	(now_nanos as i64).saturating_sub(last_nanos) >= interval_nanos
}

#[cfg(test)]
mod tests {
	use reifydb::{
		Clock, IdentityId,
		runtime::context::rng::Rng,
		value::value::{datetime::DateTime, duration::Duration, uuid::Uuid7},
	};

	use super::due;
	use crate::store::MonitorRow;

	fn monitor(interval_seconds: i64, last_checked_nanos: Option<u64>) -> MonitorRow {
		MonitorRow {
			id: Uuid7::generate(&Clock::testing(), &Rng::seeded(42)),
			owner: IdentityId::root(),
			name: "m".to_string(),
			kind: "http".to_string(),
			target: "https://example.com".to_string(),
			interval: Duration::from_seconds(interval_seconds).unwrap(),
			timeout: Duration::from_seconds(5).unwrap(),
			http_method: None,
			expected_status: None,
			keyword: None,
			expected_ip: None,
			failure_threshold: 1,
			enabled: true,
			created_at: DateTime::from_nanos(0),
			last_checked_at: last_checked_nanos.map(DateTime::from_nanos),
			consecutive_failures: 0,
			status: "unknown".to_string(),
		}
	}

	const SECOND: u64 = 1_000_000_000;

	#[test]
	fn never_checked_monitor_is_due() {
		// A fresh monitor must be checked immediately, not after its first
		// interval elapses.
		assert!(due(&monitor(60, None), 123 * SECOND));
	}

	#[test]
	fn monitor_is_due_only_after_its_interval() {
		let m = monitor(60, Some(1_000 * SECOND));
		assert!(!due(&m, 1_030 * SECOND), "half the interval must not be due");
		assert!(!due(&m, 1_059 * SECOND), "one second early must not be due");
		assert!(due(&m, 1_060 * SECOND), "exactly the interval must be due");
		assert!(due(&m, 2_000 * SECOND), "well past the interval must be due");
	}
}
