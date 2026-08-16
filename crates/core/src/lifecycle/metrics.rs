// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use crate::lifecycle::class::{Floor, FloorTerm, RetentionClass};
#[derive(Debug, Default)]
pub struct GcMetrics {
	pub objects_scanned: u64,
	pub versions_dropped: u64,
}

const STARVATION_WINDOW_NANOS: u64 = 5 * 60 * 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StuckOnset {
	Quiet,
	FloorPinned {
		floor: Floor,
		binding: FloorTerm,
		backlog_hint: u64,
	},
	Starved {
		binding: FloorTerm,
		backlog_hint: u64,
	},
	FloorUnresolvable,
}

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
	starved_since: AtomicU64,
	work_done: AtomicU64,
	backlog_hint: AtomicU64,
	slices: AtomicU64,
	stuck_slices: AtomicU64,
	budget_exhausted_slices: AtomicU64,
	gated_slices: AtomicU64,
}

impl ClassCounters {
	fn record_starvation(
		&self,
		floor_key: u64,
		binding: FloorTerm,
		work_done: u64,
		backlog_hint: u64,
	) -> StuckOnset {
		if work_done > 0 || backlog_hint == 0 {
			self.stuck_streak.store(0, Ordering::Relaxed);
			return StuckOnset::Quiet;
		}

		self.stuck_slices.fetch_add(1, Ordering::Relaxed);
		if self.stuck_streak.fetch_add(1, Ordering::Relaxed) == 0 {
			self.starved_since.store(floor_key, Ordering::Relaxed);
			return StuckOnset::Quiet;
		}

		let since = self.starved_since.load(Ordering::Relaxed);
		if floor_key < since {
			self.starved_since.store(floor_key, Ordering::Relaxed);
			return StuckOnset::Quiet;
		}
		if floor_key - since < STARVATION_WINDOW_NANOS {
			return StuckOnset::Quiet;
		}

		self.starved_since.store(floor_key, Ordering::Relaxed);
		StuckOnset::Starved {
			binding,
			backlog_hint,
		}
	}

	fn record_pinning(&self, floor: Option<(Floor, FloorTerm)>, work_done: u64, backlog_hint: u64) -> StuckOnset {
		let stuck = match floor {
			Some((floor, _)) => {
				let key = floor.monotonic_key();
				let previous = self.floor_version.swap(key, Ordering::Relaxed);
				key <= previous && work_done == 0
			}
			None => true,
		};

		if !stuck {
			self.stuck_streak.store(0, Ordering::Relaxed);
			return StuckOnset::Quiet;
		}

		self.stuck_slices.fetch_add(1, Ordering::Relaxed);
		if self.stuck_streak.fetch_add(1, Ordering::Relaxed) > 0 {
			return StuckOnset::Quiet;
		}
		if backlog_hint == 0 {
			return StuckOnset::Quiet;
		}

		match floor {
			Some((floor, binding)) => StuckOnset::FloorPinned {
				floor,
				binding,
				backlog_hint,
			},
			None => StuckOnset::FloorUnresolvable,
		}
	}

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
		floor: Option<(Floor, FloorTerm)>,
		work_done: u64,
		backlog_hint: u64,
	) -> StuckOnset {
		let Some(counters) = self.counters(class) else {
			return StuckOnset::Quiet;
		};
		counters.work_done.fetch_add(work_done, Ordering::Relaxed);
		counters.backlog_hint.store(backlog_hint, Ordering::Relaxed);
		counters.binding.store(encode_binding(floor.map(|(_, binding)| binding)), Ordering::Relaxed);

		match floor {
			Some((floor, binding)) if binding.is_clock_driven() => {
				let key = floor.monotonic_key();
				counters.floor_version.store(key, Ordering::Relaxed);
				counters.record_starvation(key, binding, work_done, backlog_hint)
			}
			_ => counters.record_pinning(floor, work_done, backlog_hint),
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
	use reifydb_value::value::datetime::DateTime;

	use super::{RetentionMetrics, STARVATION_WINDOW_NANOS, StuckOnset};
	use crate::{
		common::CommitVersion,
		lifecycle::class::{Floor, FloorTerm, RetentionClass},
	};

	const HOUR_NANOS: u64 = 3_600 * 1_000_000_000;

	const BASE: u64 = 10 * HOUR_NANOS;

	fn expiry_floor(nanos: u64) -> Option<(Floor, FloorTerm)> {
		// Every clock-driven floor in the ledger is `now - ttl` rendered as an instant, so a test
		// exercising that path has to hand the same shape over: a version floor would take the pinned
		// branch and prove nothing about the branch under test.
		Some((Floor::Instant(DateTime::from_nanos(nanos)), FloorTerm::RowExpiry))
	}

	#[test]
	fn a_clock_driven_binding_is_never_reported_as_a_pinned_floor() {
		// A row-expiry floor is `now - ttl`: only the clock feeds it, so no reader, lease or consumer
		// can hold it down and "the floor will not advance" cannot be a true diagnosis for it. The
		// sequence below is what a budgeted sweep across a ttl ladder produces - the reported cutoff
		// swinging between a short-lived and a long-lived object - which the pinned predicate reads as
		// a floor moving backwards and alarms on.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		for step in 0..20u64 {
			let floor = match step % 2 {
				0 => BASE,
				_ => BASE - HOUR_NANOS,
			};
			let onset = metrics.record_reclamation(class, expiry_floor(floor), 0, 2);

			assert!(
				!matches!(onset, StuckOnset::FloorPinned { .. }),
				"slice {step} reported a floor that nothing but the clock can move as pinned"
			);
		}
	}

	#[test]
	fn a_catch_up_sweep_shorter_than_the_window_never_alarms() {
		// A sweep that cannot finish inside one budget leaves a backlog and reclaims nothing on the
		// slices that only scan, and the lane re-runs it every few milliseconds. That is a healthy
		// catch-up, not starvation, and alarming on it buries the real signal under dozens of lines a
		// second.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;
		let step = STARVATION_WINDOW_NANOS / 100;

		for slice in 0..50u64 {
			let onset = metrics.record_reclamation(class, expiry_floor(BASE + slice * step), 0, 2);

			assert_eq!(onset, StuckOnset::Quiet, "slice {slice} alarmed inside the starvation window");
		}
	}

	#[test]
	fn a_backlog_that_outlives_the_window_alarms_once_per_window() {
		// The condition worth waking someone for is a class that has held a backlog and reclaimed
		// nothing for long enough that it cannot be a sweep in progress. It must still speak once per
		// window rather than once per slice, because the lane re-slices at its catch-up cadence.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		assert_eq!(metrics.record_reclamation(class, expiry_floor(BASE), 0, 7), StuckOnset::Quiet);
		assert_eq!(
			metrics.record_reclamation(class, expiry_floor(BASE + STARVATION_WINDOW_NANOS), 0, 7),
			StuckOnset::Starved {
				binding: FloorTerm::RowExpiry,
				backlog_hint: 7,
			},
			"a backlog held across the whole window with nothing reclaimed must alarm"
		);
		assert_eq!(
			metrics.record_reclamation(class, expiry_floor(BASE + STARVATION_WINDOW_NANOS + 1), 0, 7),
			StuckOnset::Quiet,
			"the very next slice must not alarm again, or the window has bought nothing"
		);
	}

	#[test]
	fn reclaiming_anything_restarts_the_starvation_window() {
		// A class that drains some of its backlog is making progress, and the clock it is measured
		// against keeps running. Without restarting the window, the accumulated age of an old stall
		// would alarm on a class that has since recovered.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(class, expiry_floor(BASE), 0, 4);
		metrics.record_reclamation(class, expiry_floor(BASE + STARVATION_WINDOW_NANOS), 12, 4);

		assert_eq!(
			metrics.record_reclamation(class, expiry_floor(BASE + STARVATION_WINDOW_NANOS + 1), 0, 4),
			StuckOnset::Quiet,
			"the slice after progress opens a fresh window rather than inheriting the old one"
		);
		assert_eq!(
			metrics.record_reclamation(class, expiry_floor(BASE + 2 * STARVATION_WINDOW_NANOS), 0, 4),
			StuckOnset::Quiet,
			"and the fresh window must run its full length from the slice that opened it"
		);
		assert_eq!(
			metrics.record_reclamation(class, expiry_floor(BASE + 2 * STARVATION_WINDOW_NANOS + 2), 0, 4),
			StuckOnset::Starved {
				binding: FloorTerm::RowExpiry,
				backlog_hint: 4,
			},
			"once the fresh window elapses the class is starving again and must say so"
		);
	}

	#[test]
	fn an_externally_pinned_floor_is_still_reported_as_pinned() {
		// Splitting the alarm by binding must not retire the case it exists for: a lease, a query or a
		// consumer holding the floor down is a real pin, diagnosed by naming the party responsible.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::BufferHistoricalGc;
		let floor = Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin));

		assert_eq!(metrics.record_reclamation(class, floor, 0, 3), StuckOnset::Quiet);

		assert_eq!(
			metrics.record_reclamation(class, floor, 0, 3),
			StuckOnset::FloorPinned {
				floor: Floor::Version(CommitVersion(10)),
				binding: FloorTerm::LeaseMin,
				backlog_hint: 3,
			},
			"a version floor that did not move while work waited must still name what holds it"
		);
	}

	#[test]
	fn an_unresolvable_floor_alarms_only_when_something_is_waiting_on_it() {
		// "No floor" and "nothing eligible" are different states. A database that declares no ttl at
		// all resolves no cutoff and has nothing to reclaim, which is not a fault; the same missing
		// cutoff with rows waiting behind it means the class can reclaim nothing and must be heard.
		let idle = RetentionMetrics::new();
		assert_eq!(
			idle.record_reclamation(RetentionClass::RowTtl, None, 0, 0),
			StuckOnset::Quiet,
			"an unresolvable floor with nothing eligible is not a fault to alarm on"
		);

		let waiting = RetentionMetrics::new();
		assert_eq!(
			waiting.record_reclamation(RetentionClass::RowTtl, None, 0, 3),
			StuckOnset::FloorUnresolvable,
			"the same missing cutoff with work waiting behind it must still be reported"
		);
	}

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
		// A class that keeps ticking and reports success while its floor never moves reclaims nothing;
		// it has to be counted, or it reads as idle.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(
			class,
			Some((Floor::Version(CommitVersion(100)), FloorTerm::QueryDoneUntil)),
			0,
			0,
		);
		metrics.record_reclamation(
			class,
			Some((Floor::Version(CommitVersion(100)), FloorTerm::QueryDoneUntil)),
			0,
			0,
		);

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
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(class, None, 0, 0);

		assert_eq!(metrics.snapshot(class).stuck_slices, 1, "an unresolvable cutoff must be reported as stuck");
	}

	#[test]
	fn an_advancing_floor_with_work_is_never_stuck() {
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(
			class,
			Some((Floor::Version(CommitVersion(100)), FloorTerm::QueryDoneUntil)),
			10,
			0,
		);
		metrics.record_reclamation(
			class,
			Some((Floor::Version(CommitVersion(200)), FloorTerm::QueryDoneUntil)),
			10,
			0,
		);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.stuck_slices, 0, "a class making progress must not be flagged");
		assert_eq!(snapshot.work_done, 20, "work is cumulative across slices");
		assert_eq!(snapshot.floor_version, 200, "the floor reported is the most recent one");
	}

	#[test]
	fn the_binding_term_is_reported_so_a_stuck_class_names_what_holds_it() {
		// "buffer-historical-gc is stuck" is not actionable; "stuck on lease-min" points at a leaked
		// or long-held lease, while the same class stuck on query-done-until points at a wedged query.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::BufferHistoricalGc;

		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 0, 0);

		assert_eq!(
			metrics.snapshot(class).binding,
			Some(FloorTerm::LeaseMin),
			"the term that produced the cutoff must survive into the report"
		);
	}

	#[test]
	fn an_unresolvable_floor_reports_no_binding_term() {
		// No cutoff means no term bound it. Reporting a stale binding would send an operator after a reader
		// that is not the problem.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;
		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 1, 0);

		metrics.record_reclamation(class, None, 0, 0);

		assert_eq!(metrics.snapshot(class).binding, None, "an unresolved floor must clear the binding term");
	}

	#[test]
	fn progress_clears_the_stuck_streak_so_the_alarm_can_fire_again() {
		// The alarm fires on the transition into stuck. Without clearing on progress a class that recovers and
		// wedges a second time would stay silent for the rest of the process lifetime.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 0, 0);
		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 0, 0);
		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(20)), FloorTerm::LeaseMin)), 5, 0);
		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(20)), FloorTerm::LeaseMin)), 0, 0);

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
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 0, 0);
		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 0, 0);

		assert_eq!(metrics.snapshot(class).stuck_slices, 1, "an idle frozen floor is still counted as stuck");
		assert_eq!(metrics.snapshot(class).backlog_hint, 0, "and reports nothing eligible");
	}

	#[test]
	fn a_frozen_floor_with_work_waiting_reports_the_backlog() {
		// The alarm condition: rows are eligible, the floor will not move, nothing is being reclaimed. Only
		// the backlog distinguishes this from the idle case above.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 0, 3);
		metrics.record_reclamation(class, Some((Floor::Version(CommitVersion(10)), FloorTerm::LeaseMin)), 0, 3);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.stuck_slices, 1);
		assert_eq!(snapshot.backlog_hint, 3, "the eligible-work estimate must reach the report");
		assert_eq!(snapshot.binding, Some(FloorTerm::LeaseMin), "and name what is holding the floor down");
	}

	#[test]
	fn classes_account_independently() {
		// Accounting is per class so one wedged class is visible as one wedged class, not smeared
		// across the others.
		let metrics = RetentionMetrics::new();

		metrics.record_reclamation(RetentionClass::CdcTruncate, None, 0, 0);
		metrics.record_reclamation(
			RetentionClass::RowTtl,
			Some((Floor::Version(CommitVersion(5)), FloorTerm::QueryDoneUntil)),
			3,
			0,
		);

		assert_eq!(metrics.snapshot(RetentionClass::CdcTruncate).stuck_slices, 1);
		assert_eq!(metrics.snapshot(RetentionClass::RowTtl).stuck_slices, 0);
		assert_eq!(metrics.snapshot(RetentionClass::CdcTruncate).work_done, 0);
		assert_eq!(metrics.snapshot(RetentionClass::RowTtl).work_done, 3);
	}

	#[test]
	fn a_backlog_that_survives_a_full_budget_is_visible() {
		// Draining under budget is healthy; draining under budget while the backlog stays high is losing
		// ground. Only both counters together distinguish them.
		let metrics = RetentionMetrics::new();
		let class = RetentionClass::RowTtl;

		metrics.record_reclamation(
			class,
			Some((Floor::Version(CommitVersion(10)), FloorTerm::QueryDoneUntil)),
			1024,
			50_000,
		);
		metrics.record_budget_exhausted(class);

		let snapshot = metrics.snapshot(class);
		assert_eq!(snapshot.budget_exhausted_slices, 1);
		assert_eq!(
			snapshot.backlog_hint, 50_000,
			"the latest backlog estimate must be readable, not accumulated"
		);
	}
}
