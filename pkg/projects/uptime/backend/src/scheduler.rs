// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb::value::value::{datetime::DateTime, duration::Duration, uuid::Uuid7};
use tokio::{
	select,
	sync::watch,
	time::{MissedTickBehavior, interval},
};
use tracing::warn;

use crate::{
	state::AppState,
	store,
	store::{MonitorRegionRow, MonitorRow},
};

const MIN_INTERVAL: Duration = Duration::from_seconds_const(5);

const MIN_TIMEOUT: Duration = Duration::from_seconds_const(1);

fn bounded_interval(interval: Duration) -> Duration {
	interval.max(MIN_INTERVAL)
}

pub fn bounded_timeout(timeout: Duration, interval: Duration) -> Duration {
	timeout.clamp(MIN_TIMEOUT, bounded_interval(interval))
}

pub async fn run(st: AppState, mut shutdown: watch::Receiver<bool>) {
	#[allow(clippy::disallowed_types)]
	let mut tick = interval(Duration::from_seconds(2).unwrap().to_std());
	tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

	let mut in_flight: HashMap<(Uuid7, Uuid7), i64> = HashMap::new();

	loop {
		select! {
			_ = tick.tick() => {}
			_ = shutdown.changed() => break,
		}

		let assignments = match store::all_monitor_regions(&st).await {
			Ok(assignments) => assignments,
			Err(e) => {
				warn!("scheduler failed to load monitor regions: {e:?}");
				continue;
			}
		};
		let monitors = match store::enabled_monitors(&st).await {
			Ok(monitors) => monitors,
			Err(e) => {
				warn!("scheduler failed to load monitors: {e:?}");
				continue;
			}
		};
		let monitor_map: HashMap<Uuid7, MonitorRow> = monitors.into_iter().map(|m| (m.id, m)).collect();

		let now_nanos = st.clock.now().to_nanos();
		for (assignment, monitor) in schedule_due(&assignments, &monitor_map, &mut in_flight, now_nanos) {
			let job_id = Uuid7::generate(&st.clock, &st.rng);
			if let Err(e) =
				store::enqueue_job(&st, job_id, assignment.monitor_id, assignment.region_id).await
			{
				in_flight.remove(&(assignment.monitor_id, assignment.region_id));
				warn!("scheduler failed to enqueue job for {}: {e:?}", monitor.name);
			}
		}
	}
}

fn schedule_due<'a>(
	assignments: &'a [MonitorRegionRow],
	monitors: &'a HashMap<Uuid7, MonitorRow>,
	in_flight: &mut HashMap<(Uuid7, Uuid7), i64>,
	now_nanos: i64,
) -> Vec<(&'a MonitorRegionRow, &'a MonitorRow)> {
	let active: HashSet<(Uuid7, Uuid7)> = assignments.iter().map(|a| (a.monitor_id, a.region_id)).collect();
	in_flight.retain(|key, _| active.contains(key));

	let mut scheduled = Vec::new();
	for assignment in assignments {
		let Some(monitor) = monitors.get(&assignment.monitor_id).filter(|m| m.owner == assignment.owner) else {
			continue;
		};
		let key = (assignment.monitor_id, assignment.region_id);
		let interval = bounded_interval(monitor.interval);
		let interval_nanos = interval.as_nanos().unwrap_or(i64::MAX);

		if let Some(&enqueued_at) = in_flight.get(&key) {
			let reported = assignment
				.last_checked_at
				.as_ref()
				.map(|d| d.to_nanos())
				.is_some_and(|last| last >= enqueued_at);
			let stale = now_nanos.saturating_sub(enqueued_at) > interval_nanos;
			if reported || stale {
				in_flight.remove(&key);
			} else {
				continue;
			}
		}

		if !due(assignment.last_checked_at.as_ref(), &interval, now_nanos) {
			continue;
		}
		in_flight.insert(key, now_nanos);
		scheduled.push((assignment, monitor));
	}
	scheduled
}

fn due(last_checked_at: Option<&DateTime>, interval: &Duration, now_nanos: i64) -> bool {
	let Some(last) = last_checked_at else {
		return true;
	};
	let last_nanos = last.to_nanos();
	let interval_nanos = interval.as_nanos().unwrap_or(i64::MAX);
	now_nanos.saturating_sub(last_nanos) >= interval_nanos
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb::{
		Clock, IdentityId,
		runtime::context::rng::Rng,
		value::value::{datetime::DateTime, duration::Duration, uuid::Uuid7},
	};

	use super::{bounded_timeout, due, schedule_due};
	use crate::store::{MonitorRegionRow, MonitorRow};

	fn monitor(interval_seconds: i64, last_checked_nanos: Option<i64>) -> MonitorRow {
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
			enabled: true,
			last_checked_at: last_checked_nanos.map(DateTime::from_nanos),
			status: "unknown".to_string(),
		}
	}

	const SECOND: i64 = 1_000_000_000;

	#[test]
	fn never_checked_monitor_is_due() {
		// A fresh region assignment must be checked immediately, not after its
		// first interval elapses.
		let m = monitor(60, None);
		assert!(due(m.last_checked_at.as_ref(), &m.interval, 123 * SECOND));
	}

	#[test]
	fn monitor_is_due_only_after_its_interval() {
		let m = monitor(60, Some(1_000 * SECOND));
		let last = m.last_checked_at.as_ref();
		assert!(!due(last, &m.interval, 1_030 * SECOND), "half the interval must not be due");
		assert!(!due(last, &m.interval, 1_059 * SECOND), "one second early must not be due");
		assert!(due(last, &m.interval, 1_060 * SECOND), "exactly the interval must be due");
		assert!(due(last, &m.interval, 2_000 * SECOND), "well past the interval must be due");
	}

	fn region_row(
		monitor: &MonitorRow,
		owner: IdentityId,
		seed: u64,
		last_checked_nanos: Option<i64>,
	) -> MonitorRegionRow {
		MonitorRegionRow {
			monitor_id: monitor.id,
			owner,
			region_id: Uuid7::generate(&Clock::testing(), &Rng::seeded(seed)),
			status: "unknown".to_string(),
			last_checked_at: last_checked_nanos.map(DateTime::from_nanos),
		}
	}

	#[test]
	fn a_region_row_planted_on_another_owners_monitor_schedules_nothing() {
		// A foreign-owned region row must never be scheduled, or anyone could spend a victim's probes.
		let victim = IdentityId::generate(&Clock::testing(), &Rng::seeded(1));
		let attacker = IdentityId::generate(&Clock::testing(), &Rng::seeded(2));
		let mut m = monitor(60, None);
		m.owner = victim;
		let monitors = HashMap::from([(m.id, m.clone())]);
		let planted = region_row(&m, attacker, 3, None);
		let own = region_row(&m, victim, 4, None);
		let assignments = vec![planted.clone(), own.clone()];
		let mut in_flight = HashMap::new();

		let scheduled = schedule_due(&assignments, &monitors, &mut in_flight, 123 * SECOND);

		let regions: Vec<Uuid7> = scheduled.iter().map(|(a, _)| a.region_id).collect();
		assert_eq!(regions, vec![own.region_id], "only the owner's own region row may be scheduled");
		assert!(!in_flight.contains_key(&(m.id, planted.region_id)), "the planted row must never be in flight");
	}

	#[test]
	fn a_directly_written_one_nanosecond_interval_is_held_to_five_seconds() {
		// A direct write skips the RQL interval check; without the floor a 1ns monitor is enqueued every tick.
		let mut m = monitor(60, None);
		m.interval = Duration::from_nanoseconds(1).unwrap();
		let monitors = HashMap::from([(m.id, m.clone())]);
		let assignments = vec![region_row(&m, m.owner, 3, Some(1_000 * SECOND))];
		let mut in_flight = HashMap::new();

		assert!(
			schedule_due(&assignments, &monitors, &mut in_flight, 1_004 * SECOND).is_empty(),
			"4s after a check must not be due"
		);
		assert_eq!(
			schedule_due(&assignments, &monitors, &mut in_flight, 1_005 * SECOND).len(),
			1,
			"5s after a check must be due"
		);
		assert!(
			schedule_due(&assignments, &monitors, &mut in_flight, 1_009 * SECOND).is_empty(),
			"an unreported job must hold its slot for the bounded interval, not go stale after 1ns"
		);
	}

	#[test]
	fn a_directly_written_timeout_is_held_between_one_second_and_the_bounded_interval() {
		// A direct write skips the RQL timeout checks; without the clamp one hanging target stalls a probe.
		let secs = |n: i64| Duration::from_seconds(n).unwrap();
		let nanos = Duration::from_nanoseconds(1).unwrap();
		assert_eq!(bounded_timeout(nanos, secs(60)), secs(1), "a timeout under 1s must be raised to 1s");
		assert_eq!(
			bounded_timeout(secs(600), secs(60)),
			secs(60),
			"a timeout over the interval must drop to it"
		);
		assert_eq!(bounded_timeout(secs(600), nanos), secs(5), "the interval bound must itself be at least 5s");
		assert_eq!(bounded_timeout(secs(10), secs(60)), secs(10), "a valid timeout must be kept as is");
	}
}
