// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_core::interface::catalog::flow::FlowNodeId;
use tracing::warn;

const WARN_STRIDE: u64 = 1_000;

pub struct SealedDrops {
	node: FlowNodeId,
	reason: &'static str,
	count: AtomicU64,
}

impl SealedDrops {
	pub fn new(node: FlowNodeId, reason: &'static str) -> Self {
		Self {
			node,
			reason,
			count: AtomicU64::new(0),
		}
	}

	pub fn note(&self, dropped: u64) {
		if dropped == 0 {
			return;
		}
		let before = self.count.fetch_add(dropped, Ordering::Relaxed);
		let after = before + dropped;
		if before == 0 || before / WARN_STRIDE != after / WARN_STRIDE {
			warn!(
				node_id = self.node.0,
				dropped,
				total = after,
				reason = self.reason,
				"diffs were dropped"
			);
		}
	}

	pub fn total(&self) -> u64 {
		self.count.load(Ordering::Relaxed)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_noted_drop_accumulates_and_a_zero_drop_does_not() {
		// The counter is the durable half of the loud-drop contract - the warn is rate
		// limited and therefore lossy, so the total is what an operator or a test can actually
		// assert on. A zero note must be a no-op rather than an entry, because every operator calls
		// note() unconditionally at the end of a batch and the overwhelmingly common case is that
		// nothing was dropped.
		let drops = SealedDrops::new(FlowNodeId(7), "test");
		assert_eq!(drops.total(), 0, "a fresh counter has dropped nothing");

		drops.note(0);
		assert_eq!(drops.total(), 0, "noting zero drops must not move the counter");

		drops.note(2);
		drops.note(1);
		assert_eq!(drops.total(), 3, "notes accumulate rather than replace");
	}

	#[test]
	fn the_warn_stride_is_crossed_exactly_once_per_thousand_however_the_drops_arrive() {
		// The rate limit is computed from the counter, not from a call count, so a node
		// dropping one diff at a time and a node dropping a whole batch at once must warn the same
		// number of times. This is what makes the warn safe on a hot path: a flow replaying a
		// backlog cannot turn it into a per-diff log storm.
		// The assertion below reproduces the stride arithmetic rather than counting log lines,
		// because the counter transition IS the rate limiter - `before / STRIDE != after / STRIDE`.
		fn warns(notes: &[u64]) -> usize {
			let drops = SealedDrops::new(FlowNodeId(1), "test");
			let mut warned = 0;
			for &n in notes {
				let before = drops.total();
				drops.note(n);
				let after = drops.total();
				if n > 0 && (before == 0 || before / WARN_STRIDE != after / WARN_STRIDE) {
					warned += 1;
				}
			}
			warned
		}

		let one_at_a_time: Vec<u64> = vec![1; 2_500];
		assert_eq!(warns(&one_at_a_time), 3, "2500 single drops cross 0, 1000 and 2000");
		assert_eq!(warns(&[2_500]), 1, "the same 2500 drops in one batch warn once");
		assert_eq!(warns(&[999, 1]), 2, "a batch that lands exactly on the stride still warns");
	}
}
