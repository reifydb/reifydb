// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BinaryHeap, HashMap, HashSet},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_runtime::sync::waiter::WaiterHandle;
use reifydb_value::reifydb_assertions;

use super::{MAX_PENDING, MAX_WAITERS, OLD_VERSION_THRESHOLD, PENDING_CLEANUP_THRESHOLD};

const MAX_ORPHANED: usize = 10000;

const ORPHAN_CLEANUP_THRESHOLD: u64 = 1000;

#[repr(align(64))]
pub struct WatermarkShared {
	pub done_until: AtomicU64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdvanceBudget {
	Unlimited,
	Capped(usize),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdvanceOutcome {
	Complete,
	MoreWork,
}

#[derive(Default)]
pub struct WatermarkState {
	indices: BinaryHeap<Reverse<u64>>,
	pending: HashMap<u64, i64>,
	begun: HashSet<u64>,
	orphaned_done: HashSet<u64>,
	waiters: HashMap<u64, Vec<Arc<WaiterHandle>>>,
}

impl WatermarkState {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn process_begin(
		&mut self,
		version: u64,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
		budget: AdvanceBudget,
	) -> AdvanceOutcome {
		if budget == AdvanceBudget::Unlimited {
			self.cleanup_if_needed(done_until, out);
		}

		let first_begin = self.begun.insert(version);

		if self.orphaned_done.remove(&version) {
			self.pending.insert(version, 0);
		} else {
			self.pending.entry(version).and_modify(|v| *v += 1).or_insert(1);
		}

		if first_begin {
			self.indices.push(Reverse(version));
		}

		self.advance_with_deferred_cleanup(done_until, out, budget)
	}

	pub fn process_done(
		&mut self,
		version: u64,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
		budget: AdvanceBudget,
	) -> AdvanceOutcome {
		if budget == AdvanceBudget::Unlimited {
			self.cleanup_if_needed(done_until, out);
		}

		if self.begun.contains(&version) {
			self.pending.entry(version).and_modify(|v| *v -= 1).or_insert(-1);
		} else {
			self.orphaned_done.insert(version);
			return AdvanceOutcome::Complete;
		}

		self.advance_with_deferred_cleanup(done_until, out, budget)
	}

	pub fn advance_chunk(
		&mut self,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
		chunk: usize,
	) -> AdvanceOutcome {
		self.cleanup_if_needed(done_until, out);
		self.try_advance(done_until, out, AdvanceBudget::Capped(chunk))
	}

	pub fn drain_remaining(&mut self, done_until: &AtomicU64, out: &mut Vec<Arc<WaiterHandle>>) {
		self.cleanup_if_needed(done_until, out);
		self.try_advance(done_until, out, AdvanceBudget::Unlimited);
	}

	fn advance_with_deferred_cleanup(
		&mut self,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
		budget: AdvanceBudget,
	) -> AdvanceOutcome {
		let outcome = self.try_advance(done_until, out, budget);
		if matches!(budget, AdvanceBudget::Capped(_)) && self.needs_cleanup() {
			return AdvanceOutcome::MoreWork;
		}
		outcome
	}

	fn needs_cleanup(&self) -> bool {
		self.pending.len() > MAX_PENDING
			|| self.waiters.len() > MAX_WAITERS
			|| self.orphaned_done.len() > MAX_ORPHANED
	}

	pub fn register_waiter(
		&mut self,
		version: u64,
		waiter: Arc<WaiterHandle>,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
	) {
		let current = done_until.load(Ordering::SeqCst);
		if current >= version || version < current.saturating_sub(OLD_VERSION_THRESHOLD) {
			out.push(waiter);
		} else {
			self.waiters.entry(version).or_default().push(waiter);
		}
	}

	fn try_advance(
		&mut self,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
		budget: AdvanceBudget,
	) -> AdvanceOutcome {
		let old_done_until = done_until.load(Ordering::SeqCst);
		let mut until = old_done_until;
		let mut pops: usize = 0;
		let mut capped_out = false;

		while let Some(Reverse(min)) = self.indices.peek().copied() {
			if let AdvanceBudget::Capped(cap) = budget
				&& pops >= cap
			{
				capped_out = true;
				break;
			}

			if !self.begun.contains(&min) {
				if min <= done_until.load(Ordering::SeqCst) {
					self.indices.pop();
					self.pending.remove(&min);
					pops += 1;
					continue;
				}
				break;
			}

			if let Some(done) = self.pending.get(&min)
				&& done.gt(&0)
			{
				break;
			}

			reifydb_assertions! {
				assert_eq!(
					self.pending.get(&min).copied().unwrap_or(0),
					0,
					"version {min} is being popped from the watermark with a negative refcount, \
					 meaning mark_finished ran more often than register_in_flight for it; the \
					 frontier may have advanced past a version that a later register believed \
					 was still protected"
				);
			}

			self.indices.pop();
			self.pending.remove(&min);
			self.begun.remove(&min);
			pops += 1;
			until = min;
		}

		if until != old_done_until {
			done_until.fetch_max(until, Ordering::SeqCst);

			reifydb_assertions! {
				assert!(
					done_until.load(Ordering::SeqCst) >= old_done_until,
					"done_until moved backwards across try_advance; the GC cutoff regressing \
					 re-exposes evicted history to readers that already observed the higher frontier"
				);
			}

			self.notify_waiters(old_done_until, until, out);
		} else {
			let current = done_until.load(Ordering::SeqCst);
			self.waiters.retain(|&idx, waiters_list| {
				if idx <= current {
					out.append(waiters_list);
					false
				} else {
					true
				}
			});
		}

		if capped_out {
			AdvanceOutcome::MoreWork
		} else {
			AdvanceOutcome::Complete
		}
	}

	fn notify_waiters(&mut self, from: u64, to: u64, out: &mut Vec<Arc<WaiterHandle>>) {
		(from + 1..=to).for_each(|idx| {
			if let Some(mut waiters_list) = self.waiters.remove(&idx) {
				out.append(&mut waiters_list);
			}
		});
	}

	fn cleanup_if_needed(&mut self, done_until: &AtomicU64, out: &mut Vec<Arc<WaiterHandle>>) {
		if self.pending.len() > MAX_PENDING {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(PENDING_CLEANUP_THRESHOLD);
			self.pending.retain(|&k, _| k > cutoff);
			self.begun.retain(|&k| k > cutoff);
		}

		if self.waiters.len() > MAX_WAITERS {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(OLD_VERSION_THRESHOLD);
			self.waiters.retain(|&k, waiters_list| {
				if k <= cutoff {
					out.append(waiters_list);
					false
				} else {
					true
				}
			});
		}

		if self.orphaned_done.len() > MAX_ORPHANED {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(ORPHAN_CLEANUP_THRESHOLD);
			self.orphaned_done.retain(|&v| v > cutoff);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn duplicate_begin_pushes_the_heap_exactly_once() {
		// Two register_in_flight calls for the same version (two snapshots sharing it) must
		// refcount in `pending`, not duplicate the heap entry. A duplicate heap entry would
		// survive the first pop and permanently block try_advance at that version, because
		// the second pop finds the version no longer in `begun` and stops the frontier.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);

		assert_eq!(state.indices.len(), 1, "second begin of the same version must not push a duplicate");
		assert_eq!(state.pending.get(&1), Some(&2), "both begins must be refcounted");

		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), 0, "one of two holders finishing must not advance");

		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), 1, "the last holder finishing advances the frontier");
		assert!(state.indices.is_empty(), "the popped version must leave the heap");
	}

	#[test]
	fn stale_index_entry_left_by_cleanup_does_not_wedge_the_frontier() {
		// cleanup_if_needed retains `pending` and `begun` above a cutoff but never rebuilds
		// `indices`, so after an overflow cleanup the heap can hold versions that no longer
		// exist in `begun`. try_advance must skip such stale entries when they are already
		// covered by done_until instead of treating them as a not-yet-begun version and
		// freezing the frontier below them forever.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(3, &done_until, &mut out, AdvanceBudget::Unlimited);

		// Mimic exactly what cleanup_if_needed does to version 1: forget it from the maps
		// while its heap entry stays behind, with done_until already covering it.
		state.begun.remove(&1);
		state.pending.remove(&1);
		done_until.store(1, Ordering::SeqCst);

		state.process_done(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_done(3, &done_until, &mut out, AdvanceBudget::Unlimited);

		assert_eq!(
			done_until.load(Ordering::SeqCst),
			3,
			"the stale heap entry for version 1 must be skipped, not block the frontier"
		);
	}

	#[test]
	fn capped_advance_stops_at_budget_and_reports_more_work() {
		// The hot path advances at most its pop budget per operation; the surplus must be
		// reported as MoreWork so the caller kicks the advancer. Losing that signal would
		// leave done_until stuck below the true frontier until an unrelated later operation
		// happens to advance it, stalling every waiter in between.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		for version in 1..=10 {
			state.process_begin(version, &done_until, &mut out, AdvanceBudget::Unlimited);
		}
		for version in 2..=10 {
			let outcome = state.process_done(version, &done_until, &mut out, AdvanceBudget::Unlimited);
			assert_eq!(outcome, AdvanceOutcome::Complete, "a blocked frontier pops nothing");
		}

		let outcome = state.process_done(1, &done_until, &mut out, AdvanceBudget::Capped(4));
		assert_eq!(outcome, AdvanceOutcome::MoreWork, "hitting the pop budget must request deferral");
		assert_eq!(done_until.load(Ordering::SeqCst), 4, "exactly the budgeted pops must have advanced");

		let outcome = state.advance_chunk(&done_until, &mut out, 4);
		assert_eq!(outcome, AdvanceOutcome::MoreWork, "a chunk that fills its budget requests another");
		assert_eq!(done_until.load(Ordering::SeqCst), 8);

		state.drain_remaining(&done_until, &mut out);
		assert_eq!(done_until.load(Ordering::SeqCst), 10, "drain_remaining must reach the true frontier");

		let outcome = state.advance_chunk(&done_until, &mut out, 4);
		assert_eq!(outcome, AdvanceOutcome::Complete, "a caught-up chunk must not request another kick");
	}

	#[test]
	fn capped_advance_reports_more_work_when_waiter_cleanup_threshold_exceeded() {
		// In Capped mode the hot path never runs the O(n) cleanup retains; when a map
		// exceeds its threshold it must report MoreWork even with zero pops available, so
		// the advancer performs the cleanup. Without this trigger a stuck frontier (pops
		// never reach the budget) would let the maps grow without bound.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(2, &done_until, &mut out, AdvanceBudget::Unlimited);

		for version in 0..(MAX_WAITERS as u64 + 1) {
			state.register_waiter(
				1_000_000 + version,
				Arc::new(WaiterHandle::new()),
				&done_until,
				&mut out,
			);
		}

		let outcome = state.process_done(2, &done_until, &mut out, AdvanceBudget::Capped(4));
		assert_eq!(
			outcome,
			AdvanceOutcome::MoreWork,
			"an over-threshold waiter map must request the advancer even though no pop happened"
		);

		let outcome = state.process_done(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(
			outcome,
			AdvanceOutcome::Complete,
			"Unlimited mode runs cleanup inline and must not ask for deferral"
		);
	}

	#[test]
	fn unlimited_advance_never_reports_more_work() {
		// Unlimited is the no-advancer fallback (single-threaded builds, post-shutdown);
		// it must always finish the job itself, because there is nobody to kick.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		for version in 1..=1_000 {
			state.process_begin(version, &done_until, &mut out, AdvanceBudget::Unlimited);
		}
		for version in (2..=1_000).rev() {
			let outcome = state.process_done(version, &done_until, &mut out, AdvanceBudget::Unlimited);
			assert_eq!(outcome, AdvanceOutcome::Complete);
		}
		let outcome = state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(outcome, AdvanceOutcome::Complete, "a thousand-pop burst still completes inline");
		assert_eq!(done_until.load(Ordering::SeqCst), 1_000);
	}
}
