// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_cdc::consume::watermark::CdcConsumerWatermark;
use reifydb_core::common::CommitVersion;

use super::tracker::FlowPositionTracker;

#[derive(Clone)]
pub struct FlowMaterialization {
	inner: Arc<FlowMaterializationInner>,
}

struct FlowMaterializationInner {
	poll_frontier: CdcConsumerWatermark,
	flows: FlowPositionTracker,
	output_frontier: AtomicU64,
	latch: AtomicU64,
}

impl FlowMaterialization {
	pub fn new(poll_frontier: CdcConsumerWatermark, flows: FlowPositionTracker) -> Self {
		Self {
			inner: Arc::new(FlowMaterializationInner {
				poll_frontier,
				flows,
				output_frontier: AtomicU64::new(0),
				latch: AtomicU64::new(0),
			}),
		}
	}

	pub fn record_output(&self, version: CommitVersion) {
		self.inner.output_frontier.fetch_max(version.0, Ordering::AcqRel);
	}

	pub fn output_frontier(&self) -> CommitVersion {
		CommitVersion(self.inner.output_frontier.load(Ordering::Acquire))
	}

	pub fn caught_up(&self) -> CommitVersion {
		let poll = self.inner.poll_frontier.get();

		let Some(slowest) = self.inner.flows.all().values().min().copied() else {
			return CommitVersion(self.latch(poll.0));
		};

		if slowest.0 < self.inner.output_frontier.load(Ordering::Acquire) {
			return CommitVersion(self.inner.latch.load(Ordering::Acquire));
		}

		CommitVersion(self.latch(poll.0.min(slowest.0)))
	}

	fn latch(&self, candidate: u64) -> u64 {
		self.inner.latch.fetch_max(candidate, Ordering::AcqRel).max(candidate)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::flow::FlowId;

	use super::*;

	fn parts() -> (CdcConsumerWatermark, FlowPositionTracker, FlowMaterialization) {
		let poll = CdcConsumerWatermark::new();
		let flows = FlowPositionTracker::new();
		let materialization = FlowMaterialization::new(poll.clone(), flows.clone());
		(poll, flows, materialization)
	}

	#[test]
	fn with_no_live_flows_the_poll_frontier_is_the_watermark() {
		// With no deferred flow nothing can lag, so gating on the empty set would pin a
		// flow-less database's watermark at zero and hang every caller waiting on it.
		let (poll, _flows, materialization) = parts();
		poll.store(CommitVersion(7));

		assert_eq!(materialization.caught_up(), CommitVersion(7));
	}

	#[test]
	fn the_slowest_flow_bounds_the_watermark() {
		// Discovery running ahead of processing is normal under load, so the answer is what the
		// slowest flow reached, never what the poll consumer merely saw.
		let (poll, flows, materialization) = parts();
		poll.store(CommitVersion(20));
		flows.update(FlowId(1), CommitVersion(12));
		flows.update(FlowId(2), CommitVersion(5));

		assert_eq!(
			materialization.caught_up(),
			CommitVersion(5),
			"the watermark followed discovery or the fastest flow instead of the slowest one"
		);
	}

	#[test]
	fn a_cursor_past_the_input_does_not_count_while_flow_output_is_unconsumed() {
		// Every cursor is past 9, but a flow committed output at 14 that nobody has consumed, and
		// in a chain that output IS the effect of 9: any answer at or above 9 claims the chain is
		// materialized while its tail is still behind.
		let (poll, flows, materialization) = parts();
		poll.store(CommitVersion(14));
		flows.update(FlowId(1), CommitVersion(9));
		flows.update(FlowId(2), CommitVersion(9));
		materialization.record_output(CommitVersion(14));

		assert_eq!(
			materialization.caught_up(),
			CommitVersion(0),
			"unconsumed flow output did not hold the watermark back, so a chained view reads stale"
		);
	}

	#[test]
	fn the_watermark_advances_once_every_flow_passes_the_output_frontier() {
		// The output gate must release once every flow has consumed it, or the watermark
		// deadlocks on its own gate and never advances again.
		let (poll, flows, materialization) = parts();
		poll.store(CommitVersion(14));
		flows.update(FlowId(1), CommitVersion(9));
		flows.update(FlowId(2), CommitVersion(9));
		materialization.record_output(CommitVersion(14));
		assert_eq!(materialization.caught_up(), CommitVersion(0));

		flows.update(FlowId(1), CommitVersion(14));
		flows.update(FlowId(2), CommitVersion(14));

		assert_eq!(materialization.caught_up(), CommitVersion(14));
	}

	#[test]
	fn the_watermark_never_regresses_when_new_output_lands() {
		// A regressing watermark reads as the chain having gone backwards, and any floor derived
		// from it would be unsound.
		let (poll, flows, materialization) = parts();
		poll.store(CommitVersion(14));
		flows.update(FlowId(1), CommitVersion(14));
		materialization.record_output(CommitVersion(14));
		assert_eq!(materialization.caught_up(), CommitVersion(14));

		poll.store(CommitVersion(20));
		materialization.record_output(CommitVersion(20));

		assert_eq!(
			materialization.caught_up(),
			CommitVersion(14),
			"the watermark moved backwards while a fresh commit was in flight"
		);
	}
}
