// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_runtime::sync::waiter::WaiterHandle;
use reifydb_value::reifydb_assertions;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use super::{MAX_WAITERS, OLD_VERSION_THRESHOLD};

type WaiterList = SmallVec<[Arc<WaiterHandle>; 1]>;

const MAX_ORPHANED: usize = 10000;

const ORPHAN_CLEANUP_THRESHOLD: u64 = 1000;

const INITIAL_SLOTS: usize = 1024;

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

#[derive(Clone, Copy, Default)]
struct Slot {
	refcount: i32,
	begun: bool,
}

impl Slot {
	fn vacant(&self) -> bool {
		!self.begun && self.refcount == 0
	}
}

pub struct WatermarkState {
	ring: Box<[Slot]>,
	tail: u64,
	head: u64,
	orphaned_done: FxHashSet<u64>,
	waiters: FxHashMap<u64, WaiterList>,
	last_swept: u64,
}

impl Default for WatermarkState {
	fn default() -> Self {
		Self {
			ring: vec![Slot::default(); INITIAL_SLOTS].into_boxed_slice(),
			tail: 1,
			head: 0,
			orphaned_done: FxHashSet::default(),
			waiters: FxHashMap::default(),
			last_swept: 0,
		}
	}
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

		let cancelled = !self.orphaned_done.is_empty() && self.orphaned_done.remove(&version);

		self.admit(version);
		let slot = self.slot(version);
		if cancelled {
			slot.refcount = 0;
		} else {
			slot.refcount += 1;
		}
		slot.begun = true;

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

		if !self.is_begun(version) {
			self.orphaned_done.insert(version);
			return AdvanceOutcome::Complete;
		}

		self.slot(version).refcount -= 1;

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
		self.waiters.len() > MAX_WAITERS || self.orphaned_done.len() > MAX_ORPHANED
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

	fn empty(&self) -> bool {
		self.head < self.tail
	}

	#[cfg(reifydb_assertions)]
	pub(crate) fn min_live_in_flight(&self) -> Option<u64> {
		if self.empty() {
			return None;
		}
		(self.tail..=self.head).find(|&version| {
			let slot = self.ring[(version & self.mask()) as usize];
			slot.begun && slot.refcount > 0
		})
	}

	fn mask(&self) -> u64 {
		(self.ring.len() - 1) as u64
	}

	fn slot(&mut self, version: u64) -> &mut Slot {
		let index = (version & self.mask()) as usize;
		&mut self.ring[index]
	}

	fn is_begun(&self, version: u64) -> bool {
		!self.empty()
			&& version >= self.tail
			&& version <= self.head
			&& self.ring[(version & self.mask()) as usize].begun
	}

	fn admit(&mut self, version: u64) {
		if self.empty() {
			self.tail = version;
			self.head = version;
			return;
		}

		if version >= self.tail && version <= self.head {
			return;
		}

		let tail = self.tail.min(version);
		let head = self.head.max(version);
		if head - tail >= self.ring.len() as u64 {
			self.grow(head - tail + 1);
		}
		self.tail = tail;
		self.head = head;

		reifydb_assertions! {
			assert!(
				self.head - self.tail < self.ring.len() as u64,
				"the watermark ring window {}..={} exceeds its {} slots; two distinct versions \
				 would alias onto one slot and merge their refcounts, letting the frontier pass \
				 a version that still has a live holder",
				self.tail,
				self.head,
				self.ring.len()
			);
		}
	}

	fn grow(&mut self, span: u64) {
		let mut slots = self.ring.len();
		while (slots as u64) < span {
			slots = slots.checked_mul(2).expect("watermark ring capacity overflowed usize");
		}

		let old_mask = self.mask();
		let new_mask = (slots - 1) as u64;
		let mut grown = vec![Slot::default(); slots].into_boxed_slice();
		for version in self.tail..=self.head {
			let slot = self.ring[(version & old_mask) as usize];
			if !slot.vacant() {
				grown[(version & new_mask) as usize] = slot;
			}
		}
		self.ring = grown;
	}

	fn try_advance(
		&mut self,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
		budget: AdvanceBudget,
	) -> AdvanceOutcome {
		let old_done_until = done_until.load(Ordering::SeqCst);
		let mut until = old_done_until;
		let mut visits: usize = 0;
		let mut capped_out = false;

		while !self.empty() {
			if let AdvanceBudget::Capped(cap) = budget
				&& visits >= cap
			{
				capped_out = true;
				break;
			}

			let version = self.tail;
			let index = (version & self.mask()) as usize;
			let slot = self.ring[index];

			if slot.begun && slot.refcount > 0 {
				break;
			}

			reifydb_assertions! {
				assert_eq!(
					slot.refcount,
					0,
					"version {version} is being consumed by the watermark with a negative refcount, \
					 meaning mark_finished ran more often than register_in_flight for it; the \
					 frontier may have advanced past a version that a later register believed \
					 was still protected"
				);
			}

			self.ring[index] = Slot::default();
			self.tail += 1;
			visits += 1;
			if slot.begun {
				until = version;
			}
		}

		if self.empty() && self.ring.len() > INITIAL_SLOTS {
			self.ring = vec![Slot::default(); INITIAL_SLOTS].into_boxed_slice();
		}

		if until > old_done_until {
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
			if current != self.last_swept && !self.waiters.is_empty() {
				self.last_swept = current;
				self.waiters.retain(|&idx, waiters_list| {
					if idx <= current {
						out.extend(waiters_list.drain(..));
						false
					} else {
						true
					}
				});
			}
		}

		if capped_out {
			AdvanceOutcome::MoreWork
		} else {
			AdvanceOutcome::Complete
		}
	}

	fn notify_waiters(&mut self, from: u64, to: u64, out: &mut Vec<Arc<WaiterHandle>>) {
		if self.waiters.is_empty() {
			return;
		}

		if (self.waiters.len() as u64) < to - from {
			self.waiters.retain(|&idx, waiters_list| {
				if idx > from && idx <= to {
					out.extend(waiters_list.drain(..));
					false
				} else {
					true
				}
			});
			return;
		}

		(from + 1..=to).for_each(|idx| {
			if let Some(mut waiters_list) = self.waiters.remove(&idx) {
				out.extend(waiters_list.drain(..));
			}
		});
	}

	fn cleanup_if_needed(&mut self, done_until: &AtomicU64, out: &mut Vec<Arc<WaiterHandle>>) {
		if self.waiters.len() > MAX_WAITERS {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(OLD_VERSION_THRESHOLD);
			self.waiters.retain(|&k, waiters_list| {
				if k <= cutoff {
					out.extend(waiters_list.drain(..));
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

	#[cfg(test)]
	fn refcount(&self, version: u64) -> i32 {
		self.ring[(version & self.mask()) as usize].refcount
	}

	#[cfg(test)]
	fn live_range(&self) -> Option<(u64, u64)> {
		if self.empty() {
			None
		} else {
			Some((self.tail, self.head))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn duplicate_begin_refcounts_one_slot_instead_of_admitting_twice() {
		// Two register_in_flight calls for the same version (two snapshots sharing it) must
		// refcount a single slot. Losing the second holder's count would let the frontier pass
		// the version while that holder is still reading at it, exposing its snapshot to
		// eviction; widening the window would leave a slot that nothing ever clears.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);

		assert_eq!(state.refcount(1), 2, "both begins must be refcounted onto one slot");
		assert_eq!(state.live_range(), Some((1, 1)), "a repeated version must not widen the window");

		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), 0, "one of two holders finishing must not advance");

		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), 1, "the last holder finishing advances the frontier");
		assert_eq!(state.live_range(), None, "the consumed version must leave the window");
	}

	#[test]
	fn a_version_never_registered_here_does_not_wedge_the_frontier() {
		// Each watermark sees its own subset of the version space, so the frontier must jump
		// over version numbers that were allocated but never registered on it. Treating such a
		// gap as a not-yet-begun version would freeze done_until below the true frontier
		// forever, stalling every waiter above it and pinning GC at a version nobody reads.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(4, &done_until, &mut out, AdvanceBudget::Unlimited);

		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_done(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), 2, "the frontier stops below the live version 4");

		state.process_done(4, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(
			done_until.load(Ordering::SeqCst),
			4,
			"the gap at version 3 must be skipped, not block the frontier"
		);
	}

	#[test]
	fn a_snapshot_at_the_frontier_pins_it_until_the_reader_finishes() {
		// A read transaction registers the version it is reading at, which is routinely the
		// version done_until already sits on. That registration is what stops GC evicting the
		// snapshot out from under it, so the frontier must refuse to move past it even though
		// the version is not above done_until.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), 1);

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_done(2, &done_until, &mut out, AdvanceBudget::Unlimited);

		assert_eq!(
			done_until.load(Ordering::SeqCst),
			1,
			"version 2 finishing must not advance past the reader still holding version 1"
		);

		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), 2, "releasing the reader lets the frontier catch up");
	}

	#[test]
	fn a_window_wider_than_the_ring_grows_instead_of_aliasing() {
		// The ring maps versions onto slots by masking, so two versions more than a full
		// capacity apart land on the same slot. Holding a low version open while the head runs
		// past tail + capacity must grow the ring; without growth the newer version would
		// silently inherit the older one's refcount and the frontier would skip a live holder.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		let span = INITIAL_SLOTS as u64 + 64;
		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		for version in 2..=span {
			state.process_begin(version, &done_until, &mut out, AdvanceBudget::Unlimited);
			state.process_done(version, &done_until, &mut out, AdvanceBudget::Unlimited);
		}

		assert_eq!(done_until.load(Ordering::SeqCst), 0, "version 1 is still held, so nothing may advance");
		assert_eq!(state.refcount(1), 1, "the held version must survive the regrow intact");
		assert_eq!(state.live_range(), Some((1, span)), "the window must span the whole live range");

		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		assert_eq!(done_until.load(Ordering::SeqCst), span, "releasing version 1 drains the whole window");
		assert_eq!(state.live_range(), None);
	}

	#[test]
	fn a_done_arriving_before_its_begin_cancels_that_begin() {
		// mark_finished can reach the watermark for a version that register_in_flight has not
		// recorded yet. The done must be remembered and cancel the matching begin, otherwise
		// that begin would wait forever for a completion that already happened and pin the
		// frontier below it permanently.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.process_begin(1, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_done(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_begin(2, &done_until, &mut out, AdvanceBudget::Unlimited);
		state.process_done(1, &done_until, &mut out, AdvanceBudget::Unlimited);

		assert_eq!(
			done_until.load(Ordering::SeqCst),
			2,
			"the early done must satisfy version 2 rather than leave it pending"
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

	#[test]
	fn frontier_moved_by_another_path_still_releases_a_passed_over_waiter() {
		// done_until can be raised by a path that notifies nobody, leaving a waiter enqueued for
		// a version the frontier has already passed. The no-progress sweep in try_advance is the
		// only thing that releases it. That sweep is skipped when the frontier has not moved
		// since the previous sweep, so this pins the case where it HAS moved: skipping it here
		// would park that caller forever.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(0);
		let mut out = Vec::new();

		state.register_waiter(5, Arc::new(WaiterHandle::new()), &done_until, &mut out);
		assert!(out.is_empty(), "a waiter ahead of the frontier must be enqueued, not released");

		done_until.store(10, Ordering::SeqCst);
		state.advance_chunk(&done_until, &mut out, 4);

		assert_eq!(out.len(), 1, "a waiter the frontier passed must be released by the sweep");
		assert!(state.waiters.is_empty(), "the released waiter must leave the map");
	}

	#[test]
	fn repeated_sweeps_at_an_unchanged_frontier_do_not_drop_a_live_waiter() {
		// The sweep is guarded on the frontier having moved, so it runs at most once per
		// frontier value. A waiter registered ahead of an unchanged frontier must survive every
		// subsequent no-progress call rather than being released early, which would wake a
		// reader before its snapshot version was actually durable.
		let mut state = WatermarkState::new();
		let done_until = AtomicU64::new(7);
		let mut out = Vec::new();

		state.register_waiter(9, Arc::new(WaiterHandle::new()), &done_until, &mut out);
		for _ in 0..3 {
			state.advance_chunk(&done_until, &mut out, 4);
		}

		assert!(out.is_empty(), "a waiter above the frontier must not be released");
		assert_eq!(state.waiters.len(), 1, "it must still be enqueued");

		done_until.store(9, Ordering::SeqCst);
		state.advance_chunk(&done_until, &mut out, 4);
		assert_eq!(out.len(), 1, "reaching its version releases it");
	}
}
