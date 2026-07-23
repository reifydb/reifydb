// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Per-class retention accounting.
//!
//! Zero work done means one of three very different things: nothing was eligible, the floor never advanced, or the
//! class is broken. Without separating them a stalled reclaimer is indistinguishable from an idle one, which is how
//! the original leak stayed invisible while every actor kept ticking. Each counter here exists to tell those cases
//! apart: `work_done` for progress, `stuck_slices` for a floor that will not move, `budget_exhausted_slices` plus
//! `backlog_hint` for a class that is working but losing ground, and `gated_slices` for one deliberately held back
//! at startup.

use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{common::CommitVersion, lifecycle::class::RetentionClass};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ClassSnapshot {
	pub floor_version: u64,

	pub work_done: u64,

	pub backlog_hint: u64,

	pub slices: u64,

	pub stuck_slices: u64,

	pub budget_exhausted_slices: u64,

	pub gated_slices: u64,
}

#[derive(Default)]
struct ClassCounters {
	floor_version: AtomicU64,
	work_done: AtomicU64,
	backlog_hint: AtomicU64,
	slices: AtomicU64,
	stuck_slices: AtomicU64,
	budget_exhausted_slices: AtomicU64,
	gated_slices: AtomicU64,
}

impl ClassCounters {
	fn snapshot(&self) -> ClassSnapshot {
		ClassSnapshot {
			floor_version: self.floor_version.load(Ordering::Relaxed),
			work_done: self.work_done.load(Ordering::Relaxed),
			backlog_hint: self.backlog_hint.load(Ordering::Relaxed),
			slices: self.slices.load(Ordering::Relaxed),
			stuck_slices: self.stuck_slices.load(Ordering::Relaxed),
			budget_exhausted_slices: self.budget_exhausted_slices.load(Ordering::Relaxed),
			gated_slices: self.gated_slices.load(Ordering::Relaxed),
		}
	}
}

#[derive(Clone, Default)]
pub struct RetentionMetrics {
	classes: Arc<BTreeMap<RetentionClass, ClassCounters>>,
}

impl RetentionMetrics {
	pub fn new() -> Self {
		let classes = RetentionClass::all().iter().map(|class| (*class, ClassCounters::default())).collect();
		Self {
			classes: Arc::new(classes),
		}
	}

	fn counters(&self, class: RetentionClass) -> Option<&ClassCounters> {
		self.classes.get(&class)
	}

	pub fn record_slice(&self, class: RetentionClass, floor: Option<CommitVersion>, work_done: u64, backlog_hint: u64) {
		let Some(counters) = self.counters(class) else {
			return;
		};
		counters.slices.fetch_add(1, Ordering::Relaxed);
		counters.work_done.fetch_add(work_done, Ordering::Relaxed);
		counters.backlog_hint.store(backlog_hint, Ordering::Relaxed);

		match floor {
			Some(version) => {
				let previous = counters.floor_version.swap(version.0, Ordering::Relaxed);
				if version.0 <= previous && work_done == 0 {
					counters.stuck_slices.fetch_add(1, Ordering::Relaxed);
				}
			}
			None => {
				counters.stuck_slices.fetch_add(1, Ordering::Relaxed);
			}
		}
	}

	pub fn record_budget_exhausted(&self, class: RetentionClass) {
		if let Some(counters) = self.counters(class) {
			counters.budget_exhausted_slices.fetch_add(1, Ordering::Relaxed);
		}
	}

	pub fn record_gated(&self, class: RetentionClass) {
		if let Some(counters) = self.counters(class) {
			counters.gated_slices.fetch_add(1, Ordering::Relaxed);
		}
	}

	pub fn snapshot(&self, class: RetentionClass) -> ClassSnapshot {
		self.counters(class).map(ClassCounters::snapshot).unwrap_or_default()
	}

	pub fn report(&self) -> Vec<(RetentionClass, ClassSnapshot)> {
		self.classes.iter().map(|(class, counters)| (*class, counters.snapshot())).collect()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, lifecycle::class::RetentionClass};

	use super::RetentionMetrics;

	#[test]
	fn every_class_is_present_from_construction() {
		// A class that only appears in the report once it has done work is invisible exactly when it matters:
		// the report is how an operator checks whether a class is running at all.
		let metrics = RetentionMetrics::new();

		assert_eq!(
			metrics.report().len(),
			RetentionClass::all().len(),
			"the report must enumerate every class before any of them has run"
		);
	}

	#[test]
	fn a_slice_that_reclaims_nothing_on_a_frozen_floor_counts_as_stuck() {
		// The signature of the failure this whole plan targets: the class keeps ticking, reports success, and
		// reclaims nothing because its floor never moves. It has to be counted, or it reads as idle.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_slice(class, Some(CommitVersion(100)), 0, 0);
		metrics.record_slice(class, Some(CommitVersion(100)), 0, 0);

		assert_eq!(
			metrics.snapshot(class).stuck_slices,
			1,
			"a repeated floor with no work done is a stuck class, not an idle one"
		);
	}

	#[test]
	fn an_unresolvable_floor_counts_as_stuck_rather_than_silently_passing() {
		// None means the epoch could not resolve a cutoff - the exact state in which TTLs silently stopped
		// firing. It must surface, because the executor's own behaviour (delete nothing) is indistinguishable
		// from having nothing to do.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_slice(class, None, 0, 0);

		assert_eq!(metrics.snapshot(class).stuck_slices, 1, "an unresolvable cutoff must be reported as stuck");
	}

	#[test]
	fn an_advancing_floor_with_work_is_never_stuck() {
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_slice(class, Some(CommitVersion(100)), 10, 0);
		metrics.record_slice(class, Some(CommitVersion(200)), 10, 0);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.stuck_slices, 0, "a class making progress must not be flagged");
		assert_eq!(snapshot.work_done, 20, "work is cumulative across slices");
		assert_eq!(snapshot.floor_version, 200, "the floor reported is the most recent one");
	}

	#[test]
	fn classes_account_independently() {
		// Per-class accounting is the point of decision B3: one wedged class must be visible as one wedged
		// class, not smeared across the others.
		let metrics = RetentionMetrics::new();

		metrics.record_slice(RetentionClass::CdcTruncate, None, 0, 0);
		metrics.record_slice(RetentionClass::RowTtlDrop, Some(CommitVersion(5)), 3, 0);

		assert_eq!(metrics.snapshot(RetentionClass::CdcTruncate).stuck_slices, 1);
		assert_eq!(metrics.snapshot(RetentionClass::RowTtlDrop).stuck_slices, 0);
		assert_eq!(metrics.snapshot(RetentionClass::CdcTruncate).work_done, 0);
		assert_eq!(metrics.snapshot(RetentionClass::RowTtlDrop).work_done, 3);
	}

	#[test]
	fn a_backlog_that_survives_a_full_budget_is_visible() {
		// Draining under budget is healthy; draining under budget while the backlog stays high is losing
		// ground. Only both counters together distinguish them.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_slice(class, Some(CommitVersion(10)), 1024, 50_000);
		metrics.record_budget_exhausted(class);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.budget_exhausted_slices, 1);
		assert_eq!(snapshot.backlog_hint, 50_000, "the latest backlog estimate must be readable, not accumulated");
	}
}
