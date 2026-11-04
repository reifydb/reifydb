// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeSet,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, FlowStatus},
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};

#[derive(Clone, Default)]
pub struct FlowHealthRegistry {
	stalled: Arc<Mutex<BTreeSet<FlowId>>>,
	stalls: Arc<AtomicU64>,
}

impl FlowHealthRegistry {
	pub fn new() -> Self {
		Self {
			stalled: Arc::new(Mutex::new(BTreeSet::new())),
			stalls: Arc::new(AtomicU64::new(0)),
		}
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
		self.stalled.lock().remove(&flow_id);
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
		if safe <= cursor {
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
		out.push(MetricsSample::count("flow_health", "stalled", self.stalled_count() as u64));
		out.push(MetricsSample::counter("flow_health", "stalls", self.stalls()));
	}
}

pub fn stored_poisoned(engine: &StandardEngine) -> Result<Vec<(FlowId, String)>> {
	let mut query = engine.begin_query(IdentityId::system())?;
	let flows = engine.catalog().list_flows_all(&mut Transaction::Query(&mut query))?;
	Ok(flows.into_iter()
		.filter_map(|flow| match flow.status {
			FlowStatus::Active => None,
			FlowStatus::Poisoned {
				reason,
			} => Some((flow.id, reason)),
		})
		.collect())
}

pub struct PoisonedFlowsCollector {
	engine: StandardEngine,
}

impl PoisonedFlowsCollector {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}
}

impl MetricsCollector for PoisonedFlowsCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let poisoned = stored_poisoned(&self.engine)
			.unwrap_or_else(|e| panic!("flow health metrics cannot list the poisoned flows: {e}"));
		out.push(MetricsSample::count("flow_health", "poisoned", poisoned.len() as u64));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const STALL_MS: i64 = 1_000;

	fn at(millis: i64) -> DateTime {
		DateTime::from_millis(millis)
	}

	fn watch() -> StallWatch {
		StallWatch::new(FlowId(1), CommitVersion(0), Duration::from_milliseconds(STALL_MS).unwrap())
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
