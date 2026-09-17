// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::FlowId,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::{datetime::DateTime, duration::Duration};

#[derive(Clone, Default)]
pub struct FlowHealthRegistry {
	poisoned: Arc<Mutex<BTreeMap<FlowId, String>>>,
	stalled: Arc<Mutex<BTreeSet<FlowId>>>,
	stalls: Arc<AtomicU64>,
}

impl FlowHealthRegistry {
	pub fn new() -> Self {
		Self {
			poisoned: Arc::new(Mutex::new(BTreeMap::new())),
			stalled: Arc::new(Mutex::new(BTreeSet::new())),
			stalls: Arc::new(AtomicU64::new(0)),
		}
	}

	pub fn mark_poisoned(&self, flow_id: FlowId, reason: String) {
		self.poisoned.lock().insert(flow_id, reason);
	}

	pub fn mark_stalled(&self, flow_id: FlowId) {
		if self.stalled.lock().insert(flow_id) {
			self.stalls.fetch_add(1, Ordering::Relaxed);
		}
	}

	pub fn clear_stall(&self, flow_id: FlowId) {
		self.stalled.lock().remove(&flow_id);
	}

	pub fn clear(&self, flow_id: FlowId) {
		self.poisoned.lock().remove(&flow_id);
		self.stalled.lock().remove(&flow_id);
	}

	pub fn poisoned(&self) -> Vec<(FlowId, String)> {
		self.poisoned.lock().iter().map(|(id, reason)| (*id, reason.clone())).collect()
	}

	pub fn stalled(&self) -> Vec<FlowId> {
		self.stalled.lock().iter().copied().collect()
	}

	pub fn stalled_count(&self) -> usize {
		self.stalled.lock().len()
	}

	pub fn stalls(&self) -> u64 {
		self.stalls.load(Ordering::Relaxed)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stall {
	pub cursor: CommitVersion,
	pub pending: u64,
}

pub struct StallWatch {
	flow_id: FlowId,
	timeout: Duration,
	cursor: CommitVersion,
	since: Option<DateTime>,
	reported: bool,
}

impl StallWatch {
	pub fn new(flow_id: FlowId, cursor: CommitVersion, timeout: Duration) -> Self {
		Self {
			flow_id,
			timeout,
			cursor,
			since: None,
			reported: false,
		}
	}

	pub fn observe(
		&mut self,
		health: &FlowHealthRegistry,
		cursor: CommitVersion,
		safe: CommitVersion,
		now: DateTime,
	) -> Option<Stall> {
		if safe <= cursor || health.poisoned.lock().contains_key(&self.flow_id) {
			self.clear(health);
			return None;
		}
		if cursor != self.cursor {
			self.cursor = cursor;
			self.clear(health);
		}
		let since = *self.since.get_or_insert(now);
		if self.reported || now - since < self.timeout {
			return None;
		}
		self.reported = true;
		health.mark_stalled(self.flow_id);
		Some(Stall {
			cursor,
			pending: safe.0.saturating_sub(cursor.0),
		})
	}

	fn clear(&mut self, health: &FlowHealthRegistry) {
		if self.reported {
			health.clear_stall(self.flow_id);
		}
		self.since = None;
		self.reported = false;
	}
}

impl MetricsCollector for FlowHealthRegistry {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::count("flow_health", "poisoned", self.poisoned.lock().len() as u64));
		out.push(MetricsSample::count("flow_health", "stalled", self.stalled_count() as u64));
		out.push(MetricsSample::counter("flow_health", "stalls", self.stalls()));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn poison_records_and_clear_removes_ordered_by_flow_id() {
		let health = FlowHealthRegistry::new();
		assert!(health.poisoned().is_empty());

		health.mark_poisoned(FlowId(7), "boom".to_string());
		health.mark_poisoned(FlowId(2), "kaboom".to_string());

		// Ordered by flow id, not insertion order.
		assert_eq!(health.poisoned(), vec![(FlowId(2), "kaboom".to_string()), (FlowId(7), "boom".to_string())]);

		health.clear(FlowId(2));
		assert_eq!(health.poisoned(), vec![(FlowId(7), "boom".to_string())]);

		health.clear(FlowId(7));
		assert!(health.poisoned().is_empty());
	}

	#[test]
	fn re_poison_overwrites_reason() {
		let health = FlowHealthRegistry::new();
		health.mark_poisoned(FlowId(1), "first".to_string());
		health.mark_poisoned(FlowId(1), "second".to_string());
		assert_eq!(health.poisoned(), vec![(FlowId(1), "second".to_string())]);
	}

	#[test]
	fn clones_share_the_same_map() {
		let a = FlowHealthRegistry::new();
		let b = a.clone();
		a.mark_poisoned(FlowId(3), "x".to_string());
		assert_eq!(b.poisoned().len(), 1);
	}

	const STALL_MS: u64 = 1_000;

	fn at(millis: u64) -> DateTime {
		DateTime::from_millis(millis)
	}

	fn watch() -> StallWatch {
		StallWatch::new(FlowId(1), CommitVersion(0), Duration::from_milliseconds(STALL_MS as i64).unwrap())
	}

	#[test]
	fn a_cursor_that_never_advances_while_input_is_available_is_reported_stalled() {
		let health = FlowHealthRegistry::new();
		let mut watch = watch();

		assert_eq!(watch.observe(&health, CommitVersion(0), CommitVersion(5), at(0)), None);
		assert_eq!(
			health.stalled_count(),
			0,
			"the first observation only starts the clock, or every transient pause reports a stall"
		);

		let stall = watch.observe(&health, CommitVersion(0), CommitVersion(5), at(STALL_MS + 1));

		assert_eq!(
			stall,
			Some(Stall {
				cursor: CommitVersion(0),
				pending: 5,
			}),
			"the report must name where the flow is pinned and how much input waits behind it"
		);
		assert_eq!(
			health.stalled(),
			vec![FlowId(1)],
			"a cursor pinned below the safe bound past the timeout must reach the registry, or the freeze \
			 stays invisible"
		);
		assert_eq!(health.stalls(), 1, "the stall must count once so a recovered freeze is still evidence");
	}

	#[test]
	fn a_cursor_that_advances_restarts_the_stall_clock() {
		let health = FlowHealthRegistry::new();
		let mut watch = watch();
		let safe = CommitVersion(5);

		watch.observe(&health, CommitVersion(0), safe, at(0));
		watch.observe(&health, CommitVersion(1), safe, at(STALL_MS - 1));

		watch.observe(&health, CommitVersion(1), safe, at(STALL_MS + 1));
		assert_eq!(
			health.stalled_count(),
			0,
			"progress must restart the window, otherwise a slow but living flow is reported dead"
		);

		watch.observe(&health, CommitVersion(1), safe, at(2 * STALL_MS + 2));
		assert_eq!(
			health.stalled_count(),
			1,
			"the restarted window must still expire, or one advance buys permanent immunity"
		);
	}

	#[test]
	fn a_flow_that_recovers_clears_its_stall() {
		let health = FlowHealthRegistry::new();
		let mut watch = watch();
		let safe = CommitVersion(5);

		watch.observe(&health, CommitVersion(0), safe, at(0));
		watch.observe(&health, CommitVersion(0), safe, at(STALL_MS + 1));
		assert_eq!(health.stalled_count(), 1, "precondition: the flow must be reported stalled");

		watch.observe(&health, CommitVersion(1), safe, at(STALL_MS + 2));

		assert_eq!(health.stalled_count(), 0, "a flow that moves again must leave the registry");
		assert_eq!(health.stalls(), 1, "clearing must not erase the record that it happened");
	}

	#[test]
	fn a_poisoned_flow_is_never_reported_stalled() {
		let health = FlowHealthRegistry::new();
		let mut watch = watch();
		let safe = CommitVersion(5);
		health.mark_poisoned(FlowId(1), "boom".to_string());

		watch.observe(&health, CommitVersion(0), safe, at(0));
		watch.observe(&health, CommitVersion(0), safe, at(10 * STALL_MS));

		assert_eq!(
			health.stalled_count(),
			0,
			"a poisoned flow already fails the wait with its cause, and a stall report would bury that cause"
		);
	}

	#[test]
	fn a_caught_up_flow_is_never_reported_stalled() {
		let health = FlowHealthRegistry::new();
		let mut watch = watch();
		let caught_up = CommitVersion(5);

		watch.observe(&health, caught_up, caught_up, at(0));
		watch.observe(&health, caught_up, caught_up, at(10 * STALL_MS));

		assert_eq!(
			health.stalled_count(),
			0,
			"an idle flow with nothing to read is not stalled, and reporting it would bury the real ones"
		);
	}
}
