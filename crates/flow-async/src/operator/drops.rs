// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	metrics::{
		heap::DropCounts,
		operator::{
			DROPPED_CHANGES_PAST_IMMUTABLE, DROPPED_INSERTS_PAST_SEAL, DROPPED_REFUSED_SESSION_ROWS,
			DROPPED_REMOVES_OF_FORGOTTEN_DISTINCT, DROPPED_REMOVES_PAST_SEAL,
			DROPPED_ROWS_IN_SEALED_WINDOWS, DROPPED_UPDATES_PAST_SEAL,
		},
	},
};
use tracing::warn;

const WARN_STRIDE: u64 = 1_000;

pub struct DropCounter {
	operator: OperatorId,
	reason: &'static str,
	count: AtomicU64,
}

impl DropCounter {
	pub fn new(operator: OperatorId, reason: &'static str) -> Self {
		Self {
			operator,
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
				operator_id = self.operator.0,
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

pub fn seal_drops(sealed: bool, inserts: &DropCounter, updates: &DropCounter, removes: &DropCounter) -> DropCounts {
	if !sealed {
		return DropCounts::default();
	}
	DropCounts {
		inserts_past_seal: Some(inserts.total()),
		updates_past_seal: Some(updates.total()),
		removes_past_seal: Some(removes.total()),
		..DropCounts::default()
	}
}

pub fn merge_drops(operator: OperatorId, host: DropCounts, inner: DropCounts) -> DropCounts {
	let one = |reason: &'static str, host: Option<u64>, inner: Option<u64>| match (host, inner) {
		(Some(_), Some(_)) => {
			panic!("operator {} reports {reason} from both the host and the operator", operator.0)
		}
		(host, inner) => host.or(inner),
	};
	DropCounts {
		inserts_past_seal: one(DROPPED_INSERTS_PAST_SEAL, host.inserts_past_seal, inner.inserts_past_seal),
		updates_past_seal: one(DROPPED_UPDATES_PAST_SEAL, host.updates_past_seal, inner.updates_past_seal),
		removes_past_seal: one(DROPPED_REMOVES_PAST_SEAL, host.removes_past_seal, inner.removes_past_seal),
		rows_in_sealed_windows: one(
			DROPPED_ROWS_IN_SEALED_WINDOWS,
			host.rows_in_sealed_windows,
			inner.rows_in_sealed_windows,
		),
		changes_past_immutable: one(
			DROPPED_CHANGES_PAST_IMMUTABLE,
			host.changes_past_immutable,
			inner.changes_past_immutable,
		),
		refused_session_rows: one(
			DROPPED_REFUSED_SESSION_ROWS,
			host.refused_session_rows,
			inner.refused_session_rows,
		),
		removes_of_forgotten_distinct: one(
			DROPPED_REMOVES_OF_FORGOTTEN_DISTINCT,
			host.removes_of_forgotten_distinct,
			inner.removes_of_forgotten_distinct,
		),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_noted_drop_accumulates_and_a_zero_drop_does_not() {
		// The warn is rate limited and therefore lossy, so the counter is the only thing that can
		// be asserted on. Every operator notes unconditionally at the end of a batch, so a zero
		// note has to be a no-op.
		let drops = DropCounter::new(OperatorId(7), "test");
		assert_eq!(drops.total(), 0, "a fresh counter has dropped nothing");

		drops.note(0);
		assert_eq!(drops.total(), 0, "noting zero drops must not move the counter");

		drops.note(2);
		drops.note(1);
		assert_eq!(drops.total(), 3, "notes accumulate rather than replace");
	}

	#[test]
	fn the_warn_stride_is_crossed_exactly_once_per_thousand_however_the_drops_arrive() {
		// The rate limit reads the counter, not a call count, so one diff at a time and a whole
		// batch at once must warn equally often - which is what stops a flow replaying a backlog
		// from turning the warn into a per-diff log storm.
		fn warns(notes: &[u64]) -> usize {
			let drops = DropCounter::new(OperatorId(1), "test");
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
