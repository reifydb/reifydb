// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{
	common::CommitVersion,
	lifecycle::class::{FloorTerm, RetentionClass},
};
use tracing::warn;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ClassSnapshot {
	pub floor_version: u64,

	pub binding: Option<FloorTerm>,

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
	binding: AtomicU64,
	stuck_streak: AtomicU64,
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
			binding: decode_binding(self.binding.load(Ordering::Relaxed)),
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

	pub fn record_liveness(&self, class: RetentionClass) {
		if let Some(counters) = self.counters(class) {
			counters.slices.fetch_add(1, Ordering::Relaxed);
		}
	}

	pub fn record_reclamation(
		&self,
		class: RetentionClass,
		floor: Option<(CommitVersion, FloorTerm)>,
		work_done: u64,
		backlog_hint: u64,
	) {
		let Some(counters) = self.counters(class) else {
			return;
		};
		counters.work_done.fetch_add(work_done, Ordering::Relaxed);
		counters.backlog_hint.store(backlog_hint, Ordering::Relaxed);
		counters.binding.store(encode_binding(floor.map(|(_, binding)| binding)), Ordering::Relaxed);

		let stuck = match floor {
			Some((version, _)) => {
				let previous = counters.floor_version.swap(version.0, Ordering::Relaxed);
				version.0 <= previous && work_done == 0
			}
			None => true,
		};

		if !stuck {
			counters.stuck_streak.store(0, Ordering::Relaxed);
			return;
		}

		counters.stuck_slices.fetch_add(1, Ordering::Relaxed);
		if counters.stuck_streak.fetch_add(1, Ordering::Relaxed) > 0 {
			return;
		}
		match floor {
			Some(_) if backlog_hint == 0 => {}
			Some((version, binding)) => warn!(
				class = class.name(),
				floor = version.0,
				binding = %binding,
				protects = binding.protects(),
				backlog = backlog_hint,
				"retention class has eligible work but its floor will not advance"
			),
			None => warn!(
				class = class.name(),
				"retention class has no resolvable floor; it can reclaim nothing"
			),
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

fn encode_binding(term: Option<FloorTerm>) -> u64 {
	term.map(|term| term.index() as u64 + 1).unwrap_or(0)
}

fn decode_binding(encoded: u64) -> Option<FloorTerm> {
	FloorTerm::from_index(encoded.checked_sub(1)? as usize)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		lifecycle::class::{FloorTerm, RetentionClass},
	};

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

		metrics.record_reclamation(class, Some((CommitVersion(100), FloorTerm::QueryDoneUntil)), 0, 0);
		metrics.record_reclamation(class, Some((CommitVersion(100), FloorTerm::QueryDoneUntil)), 0, 0);

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

		metrics.record_reclamation(class, None, 0, 0);

		assert_eq!(metrics.snapshot(class).stuck_slices, 1, "an unresolvable cutoff must be reported as stuck");
	}

	#[test]
	fn an_advancing_floor_with_work_is_never_stuck() {
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_reclamation(class, Some((CommitVersion(100), FloorTerm::QueryDoneUntil)), 10, 0);
		metrics.record_reclamation(class, Some((CommitVersion(200), FloorTerm::QueryDoneUntil)), 10, 0);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.stuck_slices, 0, "a class making progress must not be flagged");
		assert_eq!(snapshot.work_done, 20, "work is cumulative across slices");
		assert_eq!(snapshot.floor_version, 200, "the floor reported is the most recent one");
	}

	#[test]
	fn the_binding_term_is_reported_so_a_stuck_class_names_what_holds_it() {
		// "buffer-historical-gc is stuck" is not actionable; "stuck on subscription-snapshot" points at a
		// lagging subscription, while the same class stuck on lease-min points at a leaked operator lease.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::BufferHistoricalGc;

		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::SubscriptionSnapshot)), 0, 0);

		assert_eq!(
			metrics.snapshot(class).binding,
			Some(FloorTerm::SubscriptionSnapshot),
			"the term that produced the cutoff must survive into the report"
		);
	}

	#[test]
	fn an_unresolvable_floor_reports_no_binding_term() {
		// No cutoff means no term bound it. Reporting a stale binding would send an operator after a reader
		// that is not the problem.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;
		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::LeaseMin)), 1, 0);

		metrics.record_reclamation(class, None, 0, 0);

		assert_eq!(metrics.snapshot(class).binding, None, "an unresolved floor must clear the binding term");
	}

	#[test]
	fn progress_clears_the_stuck_streak_so_the_alarm_can_fire_again() {
		// The alarm fires on the transition into stuck. Without clearing on progress a class that recovers and
		// wedges a second time would stay silent for the rest of the process lifetime.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::RowExpiry)), 0, 0);
		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::RowExpiry)), 0, 0);
		metrics.record_reclamation(class, Some((CommitVersion(20), FloorTerm::RowExpiry)), 5, 0);
		metrics.record_reclamation(class, Some((CommitVersion(20), FloorTerm::RowExpiry)), 0, 0);

		assert_eq!(
			metrics.snapshot(class).stuck_slices,
			2,
			"stuck_slices stays cumulative across separate wedges"
		);
	}

	#[test]
	fn a_frozen_floor_with_no_eligible_work_is_stuck_but_not_an_alarm() {
		// An idle database freezes its floor and reclaims nothing every tick. That is counted, because a
		// human reading the report should see it, but it must not be an alarm or the alarm becomes noise on
		// every quiet system.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::RowExpiry)), 0, 0);
		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::RowExpiry)), 0, 0);

		assert_eq!(metrics.snapshot(class).stuck_slices, 1, "an idle frozen floor is still counted as stuck");
		assert_eq!(metrics.snapshot(class).backlog_hint, 0, "and reports nothing eligible");
	}

	#[test]
	fn a_frozen_floor_with_work_waiting_reports_the_backlog() {
		// The alarm condition: rows are eligible, the floor will not move, nothing is being reclaimed. Only
		// the backlog distinguishes this from the idle case above.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtlDrop;

		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::LeaseMin)), 0, 3);
		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::LeaseMin)), 0, 3);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.stuck_slices, 1);
		assert_eq!(snapshot.backlog_hint, 3, "the eligible-work estimate must reach the report");
		assert_eq!(snapshot.binding, Some(FloorTerm::LeaseMin), "and name what is holding the floor down");
	}

	#[test]
	fn classes_account_independently() {
		// Per-class accounting is the point of decision B3: one wedged class must be visible as one wedged
		// class, not smeared across the others.
		let metrics = RetentionMetrics::new();

		metrics.record_reclamation(RetentionClass::CdcTruncate, None, 0, 0);
		metrics.record_reclamation(
			RetentionClass::RowTtlDrop,
			Some((CommitVersion(5), FloorTerm::QueryDoneUntil)),
			3,
			0,
		);

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

		metrics.record_reclamation(class, Some((CommitVersion(10), FloorTerm::QueryDoneUntil)), 1024, 50_000);
		metrics.record_budget_exhausted(class);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.budget_exhausted_slices, 1);
		assert_eq!(
			snapshot.backlog_hint, 50_000,
			"the latest backlog estimate must be readable, not accumulated"
		);
	}
}
