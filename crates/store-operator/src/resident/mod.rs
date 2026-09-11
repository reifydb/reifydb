// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batch;
pub mod bucket;

mod census;
mod checkpoint;
pub mod slot;
mod state;

#[cfg(test)]
mod tests;

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt, mem,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use dashmap::DashMap;
use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	default,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
	util::{bloom::hash_item, budget::MemoryBudget},
};
use reifydb_filter::adaptive::{AdaptiveKeyFilter, FilterMetrics};
use reifydb_runtime::sync::{
	condvar::Condvar,
	mutex::{Mutex, MutexGuard},
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, value::duration::Duration};
use tracing::instrument;

use crate::{
	actor::{Waker, resident_evict::EvictMessage, resident_flush::FlushMessage},
	error::Result,
	persistent::{Apply, Enumerate, Fetch, Persistent, PersistentTier},
	range::{OperatorRangeTier, RangeSink},
	resident::{
		bucket::write::Staged,
		slot::{Slot, SlotInner},
	},
	types::{Applied, DropMarker, FlushBatch, OperatorWrite, StagedWrite},
};

const CLOCK_PASSES: usize = 2;

pub const FLUSH_BUDGET_BYTES: ByteSize = if default::TESTING {
	default::store::OPERATOR_RESIDENT_BUDGET_TESTING
} else {
	default::store::OPERATOR_RESIDENT_BUDGET
};

pub const FLUSH_ENTRY_LIMIT: u64 = if default::TESTING {
	default::store::OPERATOR_RESIDENT_ENTRIES_TESTING
} else {
	default::store::OPERATOR_RESIDENT_ENTRIES
};

pub const DIRTY_BUDGET_BYTES: ByteSize = if default::TESTING {
	default::store::OPERATOR_DIRTY_BUDGET_TESTING
} else {
	default::store::OPERATOR_DIRTY_BUDGET
};

pub const FLUSH_INTERVAL: Duration = if default::TESTING {
	default::store::OPERATOR_FLUSH_INTERVAL_TESTING
} else {
	default::store::OPERATOR_FLUSH_INTERVAL
};

pub const FILTER_KEYS: u64 = if default::TESTING {
	default::store::OPERATOR_FILTER_KEYS_TESTING
} else {
	default::store::OPERATOR_FILTER_KEYS
};

pub const SLICE_BYTES: ByteSize = if default::TESTING {
	default::store::OPERATOR_FLUSH_SLICE_TESTING
} else {
	default::store::OPERATOR_FLUSH_SLICE
};

#[derive(Debug, Clone, Copy)]
pub struct ResidentLimits {
	pub budget: ByteSize,
	pub entries: u64,
	pub dirty_budget: ByteSize,
	pub slice: ByteSize,
}

impl Default for ResidentLimits {
	fn default() -> Self {
		Self {
			budget: FLUSH_BUDGET_BYTES,
			entries: FLUSH_ENTRY_LIMIT,
			dirty_budget: DIRTY_BUDGET_BYTES,
			slice: SLICE_BYTES,
		}
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorResidentStateMetrics {
	pub wakes: u64,
	pub tick_wakes: u64,
	pub slices: u64,
	pub persisted: u64,
	pub reclaimed: u64,
	pub evicted: u64,
	pub budget_exhausted: u64,
	pub released: ByteSize,
	pub backlog: ByteSize,
}

#[derive(Debug, Default)]
struct GlobalInner {
	checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	in_flight_checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	drops: Vec<DropMarker>,
	in_flight_drops: Vec<DropMarker>,
	flushing: bool,
	in_flight_operators: Vec<OperatorId>,
}

impl GlobalInner {
	fn any_drop(&self, predicate: impl Fn(&DropMarker) -> bool) -> bool {
		self.drops.iter().any(&predicate) || self.in_flight_drops.iter().any(&predicate)
	}
}

struct PendingGroup {
	flow: Option<FlowId>,
	seq: u64,
	operators: Vec<OperatorId>,
}

struct OperatorSinks {
	persistent: PersistentTier,
	range: OperatorRangeTier,
}

#[cfg(test)]
pub(crate) type PersistInterlock = Box<dyn Fn() + Send + Sync>;

pub struct Shared {
	slots: DashMap<OperatorId, Arc<Slot>>,
	global: Mutex<GlobalInner>,
	drop_epoch: AtomicU64,
	write_seq: AtomicU64,
	idle: Condvar,
	drain: Mutex<()>,
	flusher: Mutex<()>,
	accounting: Mutex<()>,
	sinks: OnceLock<OperatorSinks>,
	budget: Arc<MemoryBudget>,
	entries: AtomicU64,
	dirty: AtomicU64,
	dirty_bytes: AtomicU64,
	entry_limit: u64,
	slice: ByteSize,
	dirty_budget: ByteSize,
	waker: Mutex<Option<Waker<FlushMessage>>>,
	evictor: Mutex<Option<Waker<EvictMessage>>>,
	metrics: Mutex<OperatorResidentStateMetrics>,
	triggered: AtomicBool,
	filter: Arc<AdaptiveKeyFilter>,
	filter_armed: AtomicBool,
	sweep_cursor: AtomicU64,
	#[cfg(test)]
	persist_interlock: Mutex<Option<PersistInterlock>>,
}

impl Shared {
	fn new(limits: ResidentLimits) -> Self {
		Self {
			slots: DashMap::new(),
			global: Mutex::new(GlobalInner::default()),
			drop_epoch: AtomicU64::new(0),
			write_seq: AtomicU64::new(0),
			idle: Condvar::new(),
			drain: Mutex::new(()),
			flusher: Mutex::new(()),
			accounting: Mutex::new(()),
			sinks: OnceLock::new(),
			budget: Arc::new(MemoryBudget::new(limits.budget)),
			entries: AtomicU64::new(0),
			dirty: AtomicU64::new(0),
			dirty_bytes: AtomicU64::new(0),
			entry_limit: limits.entries,
			slice: limits.slice.min(limits.budget),
			dirty_budget: limits.dirty_budget,
			waker: Mutex::new(None),
			evictor: Mutex::new(None),
			metrics: Mutex::new(OperatorResidentStateMetrics::default()),
			triggered: AtomicBool::new(false),
			filter: Arc::new(AdaptiveKeyFilter::new()),
			filter_armed: AtomicBool::new(false),
			sweep_cursor: AtomicU64::new(0),
			#[cfg(test)]
			persist_interlock: Mutex::new(None),
		}
	}

	fn charge_entries(&self, count: usize) {
		self.entries.fetch_add(count as u64, Ordering::Relaxed);
	}

	fn release_entries(&self, count: usize) {
		let amount = count as u64;
		let mut current = self.entries.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_sub(amount);
			match self.entries.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
				Ok(_) => return,
				Err(observed) => current = observed,
			}
		}
	}

	fn over_entry_limit(&self) -> bool {
		self.entries.load(Ordering::Relaxed) > self.entry_limit
	}

	fn charge_dirty(&self, count: usize) {
		self.dirty.fetch_add(count as u64, Ordering::Relaxed);
	}

	fn release_dirty(&self, count: usize) {
		let amount = count as u64;
		let mut current = self.dirty.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_sub(amount);
			match self.dirty.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
				Ok(_) => return,
				Err(observed) => current = observed,
			}
		}
	}

	fn charge_dirty_bytes(&self, amount: ByteSize) {
		self.dirty_bytes.fetch_add(amount.as_bytes(), Ordering::Relaxed);
	}

	fn release_dirty_bytes(&self, amount: ByteSize) {
		let amount = amount.as_bytes();
		let mut current = self.dirty_bytes.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_sub(amount);
			match self.dirty_bytes.compare_exchange_weak(
				current,
				next,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return,
				Err(observed) => current = observed,
			}
		}
	}

	fn dirty_footprint(&self) -> ByteSize {
		ByteSize::from_bytes(self.dirty_bytes.load(Ordering::Relaxed))
	}

	fn over_dirty_budget(&self) -> bool {
		self.dirty_footprint() > self.dirty_budget || self.dirty.load(Ordering::Relaxed) > self.entry_limit
	}

	pub(crate) fn slot(&self, operator: OperatorId) -> Option<Arc<Slot>> {
		self.slots.get(&operator).map(|slot| Arc::clone(slot.value()))
	}

	pub(crate) fn slot_or_create(&self, operator: OperatorId) -> Arc<Slot> {
		if let Some(slot) = self.slot(operator) {
			return slot;
		}
		Arc::clone(self.slots.entry(operator).or_insert_with(|| Arc::new(Slot::new(operator))).value())
	}

	pub(crate) fn operators(&self) -> Vec<OperatorId> {
		let mut operators: Vec<OperatorId> = self.slots.iter().map(|slot| *slot.key()).collect();
		operators.sort_unstable();
		operators
	}

	pub(crate) fn dropped(&self, predicate: impl Fn(&DropMarker) -> bool) -> bool {
		if self.drop_epoch.load(Ordering::Acquire) == 0 {
			return false;
		}
		self.global.lock().any_drop(predicate)
	}
}

#[derive(Clone)]
pub struct Resident {
	shared: Arc<Shared>,
}

impl fmt::Debug for Resident {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Resident").field("budget", &self.budget()).finish()
	}
}

impl Default for Resident {
	fn default() -> Self {
		Self::with_limits(ResidentLimits::default())
	}
}

impl Resident {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_budget(budget: ByteSize) -> Self {
		Self::with_limits(ResidentLimits {
			budget,
			dirty_budget: budget,
			..ResidentLimits::default()
		})
	}

	pub fn note_tick(&self) {
		self.shared.metrics.lock().tick_wakes += 1;
	}

	pub fn with_limits(limits: ResidentLimits) -> Self {
		Self {
			shared: Arc::new(Shared::new(limits)),
		}
	}

	#[cfg(test)]
	pub(crate) fn set_persist_interlock(&self, interlock: PersistInterlock) {
		*self.shared.persist_interlock.lock() = Some(interlock);
	}

	pub(crate) fn shared(&self) -> &Shared {
		&self.shared
	}

	pub fn attach_flusher(&self, flusher: Waker<FlushMessage>) {
		*self.shared.waker.lock() = Some(flusher);
	}

	pub fn attach_evictor(&self, evictor: Waker<EvictMessage>) {
		*self.shared.evictor.lock() = Some(evictor);
	}

	pub fn attach_sinks(&self, persistent: PersistentTier, range: OperatorRangeTier) {
		let _ = self.shared.sinks.set(OperatorSinks {
			persistent,
			range,
		});
	}

	pub fn budget(&self) -> ByteSize {
		self.shared.budget.limit()
	}

	pub fn slice(&self) -> ByteSize {
		self.shared.slice
	}

	pub fn metrics(&self) -> OperatorResidentStateMetrics {
		let mut metrics = *self.shared.metrics.lock();
		metrics.backlog = self.resident_bytes();
		metrics
	}

	pub fn resident_bytes(&self) -> ByteSize {
		let mut total = ByteSize::ZERO;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			total = total.saturating_add(slot.inner.lock().resident_bytes());
		}
		total
	}

	#[cfg(test)]
	pub(crate) fn flushing_entries(&self) -> usize {
		let mut total = 0usize;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let inner = slot.inner.lock();
			inner.live.state.for_each_entry(operator, |_, _, _, entry| {
				if matches!(entry.staged, Staged::Flushing) {
					total += 1;
				}
			});
		}
		total
	}

	#[cfg(any(test, reifydb_assertions))]
	pub(crate) fn dirty_entries(&self) -> usize {
		let mut total = 0usize;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			total = total.saturating_add(slot.inner.lock().dirty_entries());
		}
		total
	}

	#[cfg(any(test, reifydb_assertions))]
	pub(crate) fn dirty_bytes(&self) -> ByteSize {
		let mut total = ByteSize::ZERO;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			total = total.saturating_add(slot.inner.lock().dirty_bytes());
		}
		total
	}

	pub fn resident_entries(&self) -> usize {
		let mut total = 0usize;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			total = total.saturating_add(slot.inner.lock().resident_entries());
		}
		total
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		self.record_writes(writes, None);
		self.observe_write();
	}

	pub fn apply_batch_with_checkpoints(
		&self,
		writes: &[OperatorWrite],
		checkpoints: &[(FlowId, CommitVersion)],
		checkpoint_deletes: &[FlowId],
	) {
		if writes.is_empty() && checkpoints.is_empty() && checkpoint_deletes.is_empty() {
			return;
		}
		let flow = checkpoints.first().map(|(flow, _)| *flow).or_else(|| checkpoint_deletes.first().copied());
		self.record_writes(writes, flow);
		if !checkpoints.is_empty() || !checkpoint_deletes.is_empty() {
			{
				let mut global = self.shared.global.lock();
				for (flow, version) in checkpoints {
					global.checkpoints.insert(*flow, Some(*version));
				}
				for flow in checkpoint_deletes {
					global.checkpoints.insert(*flow, None);
				}
			}
			for (flow, version) in checkpoints {
				self.wake_checkpoint(*flow, *version);
			}
		}
		self.observe_write();
	}

	#[instrument(name = "store::operator::resident::record_writes", level = "trace", skip(self, writes), fields(write_count = writes.len()))]
	fn record_writes(&self, writes: &[OperatorWrite], flow: Option<FlowId>) {
		let mut grouped: BTreeMap<OperatorId, Vec<&OperatorWrite>> = BTreeMap::new();
		for write in writes {
			grouped.entry(write_operator(write)).or_default().push(write);
		}
		for (operator, group) in grouped {
			let slot = self.shared.slot_or_create(operator);
			let mut inner = slot.inner.lock();
			let before = inner.live.bytes;
			let before_entries = inner.live.entry_count();
			let before_dirty = inner.live.dirty_count();
			let before_dirty_bytes = inner.live.dirty_bytes();
			for write in group {
				self.apply_write(&mut inner, write);
			}
			let after = inner.live.bytes;
			let after_entries = inner.live.entry_count();
			let after_dirty = inner.live.dirty_count();
			let after_dirty_bytes = inner.live.dirty_bytes();
			self.shared.budget.charge(after.saturating_sub(before));
			self.shared.budget.release(before.saturating_sub(after));
			self.shared.charge_entries(after_entries.saturating_sub(before_entries));
			self.shared.release_entries(before_entries.saturating_sub(after_entries));
			self.shared.charge_dirty(after_dirty.saturating_sub(before_dirty));
			self.shared.release_dirty(before_dirty.saturating_sub(after_dirty));
			self.shared.charge_dirty_bytes(after_dirty_bytes.saturating_sub(before_dirty_bytes));
			self.shared.release_dirty_bytes(before_dirty_bytes.saturating_sub(after_dirty_bytes));
			if flow.is_some() {
				inner.flow = flow;
			}
			self.mark_pending(&mut inner);
		}
	}

	fn mark_pending(&self, inner: &mut SlotInner) {
		if !inner.live.has_dirty() {
			inner.pending_seq = None;
			return;
		}
		if inner.pending_seq.is_none() {
			inner.pending_seq = Some(self.shared.write_seq.fetch_add(1, Ordering::Relaxed));
		}
	}

	pub(crate) fn write_slot<R>(&self, operator: OperatorId, mutate: impl FnOnce(&mut SlotInner) -> R) -> R {
		let slot = self.shared.slot_or_create(operator);
		let out = {
			let mut inner = slot.inner.lock();
			let before = inner.live.bytes;
			let before_entries = inner.live.entry_count();
			let before_dirty = inner.live.dirty_count();
			let before_dirty_bytes = inner.live.dirty_bytes();
			let out = mutate(&mut inner);
			let after = inner.live.bytes;
			let after_entries = inner.live.entry_count();
			let after_dirty = inner.live.dirty_count();
			let after_dirty_bytes = inner.live.dirty_bytes();
			self.shared.budget.charge(after.saturating_sub(before));
			self.shared.budget.release(before.saturating_sub(after));
			self.shared.charge_entries(after_entries.saturating_sub(before_entries));
			self.shared.release_entries(before_entries.saturating_sub(after_entries));
			self.shared.charge_dirty(after_dirty.saturating_sub(before_dirty));
			self.shared.release_dirty(before_dirty.saturating_sub(after_dirty));
			self.shared.charge_dirty_bytes(after_dirty_bytes.saturating_sub(before_dirty_bytes));
			self.shared.release_dirty_bytes(before_dirty_bytes.saturating_sub(after_dirty_bytes));
			self.mark_pending(&mut inner);
			out
		};
		self.observe_write();
		out
	}

	pub fn record_drop(&self, marker: DropMarker) {
		let operator = drop_operator(&marker);
		{
			let mut global = self.shared.global.lock();
			global.drops.push(marker);
			self.shared.drop_epoch.fetch_add(1, Ordering::Release);
		}
		let Some(slot) = self.shared.slot(operator) else {
			return;
		};
		let mut inner = slot.inner.lock();
		let before = inner.live.bytes;
		let before_entries = inner.live.entry_count();
		let before_dirty = inner.live.dirty_count();
		let before_dirty_bytes = inner.live.dirty_bytes();
		clear_drop(&mut inner, marker);
		let after = inner.live.bytes;
		let after_entries = inner.live.entry_count();
		let after_dirty = inner.live.dirty_count();
		let after_dirty_bytes = inner.live.dirty_bytes();
		self.shared.budget.release(before.saturating_sub(after));
		self.shared.release_entries(before_entries.saturating_sub(after_entries));
		self.shared.release_dirty(before_dirty.saturating_sub(after_dirty));
		self.shared.release_dirty_bytes(before_dirty_bytes.saturating_sub(after_dirty_bytes));
		self.mark_pending(&mut inner);
	}

	pub fn evict_to_capacity(&self) -> (usize, ByteSize) {
		let mut evicted = 0usize;
		let mut freed = ByteSize::ZERO;
		for _ in 0..CLOCK_PASSES {
			let (mut bytes, mut entries) = self.overshoot();
			if bytes.as_bytes() == 0 && entries == 0 {
				break;
			}
			let (count, released) = self.sweep(&mut bytes, &mut entries);
			evicted += count;
			freed = freed.saturating_add(released);
		}
		if evicted > 0 {
			let mut metrics = self.shared.metrics.lock();
			metrics.evicted += evicted as u64;
			metrics.reclaimed = metrics.reclaimed.saturating_add(freed.as_bytes());
		}
		(evicted, freed)
	}

	fn overshoot(&self) -> (ByteSize, usize) {
		let bytes = self.shared.budget.used().saturating_sub(self.shared.budget.limit());
		let entries = self.shared.entries.load(Ordering::Relaxed).saturating_sub(self.shared.entry_limit);
		(bytes, entries as usize)
	}

	#[instrument(name = "store::operator::resident::sweep", level = "trace", skip(self, bytes, entries))]
	fn sweep(&self, bytes: &mut ByteSize, entries: &mut usize) -> (usize, ByteSize) {
		let operators = self.shared.operators();
		if operators.is_empty() {
			return (0, ByteSize::ZERO);
		}
		let _accounting = self.shared.accounting.lock();
		let start = self.shared.sweep_cursor.fetch_add(1, Ordering::Relaxed) as usize % operators.len();
		let mut evicted = 0usize;
		let mut freed = ByteSize::ZERO;
		for offset in 0..operators.len() {
			if bytes.as_bytes() == 0 && *entries == 0 {
				break;
			}
			let operator = operators[(start + offset) % operators.len()];
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let mut inner = slot.inner.lock();
			let (count, released) = inner.live.evict_clean(bytes, entries);
			self.shared.budget.release(released);
			self.shared.release_entries(count);
			evicted += count;
			freed = freed.saturating_add(released);
		}
		(evicted, freed)
	}

	#[instrument(name = "store::operator::resident::flush_acquire", level = "debug", skip_all)]
	pub fn flush_guard(&self) -> MutexGuard<'_, ()> {
		self.shared.drain.lock()
	}

	pub fn take_for_flush(&self) -> Option<Arc<FlushBatch>> {
		self.take_drain_slice(self.shared.slice)
	}

	pub fn complete_flush(&self) {
		let batch = {
			let global = self.shared.global.lock();
			if global.in_flight_operators.is_empty() && global.in_flight_checkpoints.is_empty() {
				return;
			}
			self.rebuild_in_flight(&global)
		};
		self.settle(Arc::new(batch));
	}

	#[instrument(name = "store::operator::resident::drain", level = "debug", skip(self))]
	pub fn drain(&self, slice: ByteSize) -> Result<Applied> {
		let _flusher = self.shared.flusher.lock();
		if self.device_absent() {
			return Ok(Applied::default());
		}
		let batch = {
			let _staging = self.flush_guard();
			match self.take_drain_slice(slice) {
				Some(batch) => batch,
				None => return Ok(Applied::default()),
			}
		};
		match self.persist_applied(&batch) {
			Ok(applied) => {
				let _staging = self.flush_guard();
				self.settle(batch);
				Ok(applied)
			}
			Err(error) => {
				let _staging = self.flush_guard();
				self.revert_batch(&batch);
				Err(error)
			}
		}
	}

	#[instrument(name = "store::operator::resident::pending", level = "trace", skip(self))]
	pub fn pending(&self) -> bool {
		let staged = {
			let global = self.shared.global.lock();
			!global.checkpoints.is_empty() || !global.drops.is_empty()
		};
		staged || !self.pending_groups().is_empty()
	}

	pub fn flush_all(&self) {
		let _flusher = self.shared.flusher.lock();
		if self.device_absent() {
			return;
		}
		loop {
			let batch = {
				let _staging = self.flush_guard();
				match self.take_drain_slice(self.shared.slice) {
					Some(batch) => batch,
					None => return,
				}
			};
			self.persist(&batch);
			let _staging = self.flush_guard();
			self.settle(batch);
		}
	}

	#[instrument(name = "store::operator::resident::rebuild_in_flight", level = "trace", skip(self, global))]
	fn rebuild_in_flight(&self, global: &GlobalInner) -> FlushBatch {
		let mut batch = FlushBatch::default();
		for operator in &global.in_flight_operators {
			let Some(slot) = self.shared.slot(*operator) else {
				continue;
			};
			let inner = slot.inner.lock();
			inner.live.state.for_each_entry(*operator, |keyspace, group, suffix, entry| {
				if !matches!(entry.staged, Staged::Flushing) {
					return;
				}
				batch.writes.push((
					*operator,
					GroupStateKey::new(group, keyspace, suffix),
					staged_write(entry.post.clone()),
				));
			});
		}
		batch.checkpoints = global.in_flight_checkpoints.clone();
		batch.drops = global.in_flight_drops.clone();
		batch
	}

	#[instrument(name = "store::operator::resident::pending_groups", level = "trace", skip(self))]
	fn pending_groups(&self) -> Vec<PendingGroup> {
		let mut by_flow: BTreeMap<Option<FlowId>, PendingGroup> = BTreeMap::new();
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let inner = slot.inner.lock();
			if !inner.live.has_dirty() {
				continue;
			}
			let flow = inner.flow;
			let seq = inner.pending_seq.unwrap_or(u64::MAX);
			drop(inner);
			let group = by_flow.entry(flow).or_insert(PendingGroup {
				flow,
				seq: u64::MAX,
				operators: Vec::new(),
			});
			group.seq = group.seq.min(seq);
			group.operators.push(operator);
		}
		let mut groups: Vec<PendingGroup> = by_flow.into_values().collect();
		groups.sort_by_key(|group| (group.seq, group.flow.map_or(u64::MAX, |flow| flow.0)));
		groups
	}

	/// Arming publishes an empty filter as the authoritative one, which is only sound over a device
	/// that holds nothing: from there every persisted key passes through `add` and the filter sees
	/// all of them. Over a device that already holds rows it is a filter that rejects keys the store
	/// still has, and a rejection is final, so a read answers absent for a durable row. Anything short
	/// of a positive answer that the device is empty therefore has to leave the filter alone: an
	/// unarmed filter admits every key and costs a device read, which is the direction that stays
	/// correct.
	fn arm_filter(&self) {
		let Some(sinks) = self.shared.sinks.get() else {
			return;
		};
		let Ok(census) = sinks.persistent.census() else {
			return;
		};
		if !census.is_empty() {
			return;
		}
		if self.shared.filter_armed.swap(true, Ordering::AcqRel) {
			return;
		}
		let handle = self.shared.filter.begin_rebuild(FILTER_KEYS);
		self.shared.filter.commit_rebuild(handle);
	}

	#[instrument(name = "store::operator::resident::take_drain_slice", level = "debug", skip_all)]
	fn take_drain_slice(&self, slice: ByteSize) -> Option<Arc<FlushBatch>> {
		self.arm_filter();
		let mut global = self.shared.global.lock();
		self.clear_in_flight(&mut global);

		let mut batch = FlushBatch::default();
		let mut touched: Vec<OperatorId> = Vec::new();
		let mut consumed = ByteSize::ZERO;
		let mut staged = 0usize;
		let mut staged_bytes = ByteSize::ZERO;
		let mut exhausted = false;

		for group in self.pending_groups() {
			if consumed >= slice {
				exhausted = true;
				break;
			}
			for operator in group.operators {
				let Some(slot) = self.shared.slot(operator) else {
					continue;
				};
				let mut inner = slot.inner.lock();
				if !inner.live.has_dirty() {
					continue;
				}
				let before_dirty_bytes = inner.live.dirty_bytes();
				let carried =
					inner.live.state.stage_dirty(operator, |keyspace, group, suffix, entry| {
						batch.writes.push((
							operator,
							GroupStateKey::new(group, keyspace, suffix),
							staged_write(entry.post.clone()),
						));
						staged += 1;
						self.shared.filter.add(state_hash(operator, keyspace, group, suffix));
					});
				consumed = consumed.saturating_add(carried);
				staged_bytes = staged_bytes
					.saturating_add(before_dirty_bytes.saturating_sub(inner.live.dirty_bytes()));
				self.mark_pending(&mut inner);
				touched.push(operator);
			}
		}
		self.shared.release_dirty(staged);
		self.shared.release_dirty_bytes(staged_bytes);
		batch.bytes = consumed;

		let blocked = self.flows_with_dirty();
		let mut ready: Vec<FlowId> = Vec::new();
		for flow in global.checkpoints.keys() {
			if !blocked.contains(flow) {
				ready.push(*flow);
			}
		}
		for flow in ready {
			let entry = global.checkpoints.remove(&flow).expect("the flow was listed from this map");
			batch.checkpoints.insert(flow, entry);
		}

		if batch.writes.is_empty() && batch.checkpoints.is_empty() && global.drops.is_empty() {
			return None;
		}

		if exhausted {
			self.shared.metrics.lock().budget_exhausted += 1;
		}

		batch.drops = mem::take(&mut global.drops);
		global.in_flight_checkpoints = batch.checkpoints.clone();
		global.in_flight_drops = batch.drops.clone();
		global.in_flight_operators = touched;
		global.flushing = true;
		Some(Arc::new(batch))
	}

	fn flows_with_dirty(&self) -> BTreeSet<FlowId> {
		let mut blocked = BTreeSet::new();
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let inner = slot.inner.lock();
			if !inner.live.has_dirty() {
				continue;
			}
			if let Some(flow) = inner.flow {
				blocked.insert(flow);
			}
		}
		blocked
	}

	fn clear_in_flight(&self, global: &mut GlobalInner) {
		let mut rearmed = 0usize;
		let mut rearmed_bytes = ByteSize::ZERO;
		for operator in &global.in_flight_operators {
			let Some(slot) = self.shared.slot(*operator) else {
				continue;
			};
			let mut inner = slot.inner.lock();
			let before_dirty_bytes = inner.live.dirty_bytes();
			rearmed += inner.live.revert_flushing();
			rearmed_bytes = rearmed_bytes
				.saturating_add(inner.live.dirty_bytes().saturating_sub(before_dirty_bytes));
		}
		self.shared.charge_dirty(rearmed);
		self.shared.charge_dirty_bytes(rearmed_bytes);
		global.in_flight_operators.clear();
		global.in_flight_checkpoints.clear();
		global.in_flight_drops.clear();
	}

	#[instrument(name = "store::operator::resident::persist_applied", level = "debug", skip_all)]
	fn device_absent(&self) -> bool {
		self.shared.sinks.get().is_some_and(|sinks| sinks.persistent.is_absent())
	}

	fn persist_applied(&self, batch: &Arc<FlushBatch>) -> Result<Applied> {
		#[cfg(test)]
		{
			let interlock = self.shared.persist_interlock.lock().take();
			if let Some(interlock) = interlock {
				interlock();
			}
		}
		let sinks = self
			.shared
			.sinks
			.get()
			.expect("the operator resident state flushed before its sinks were attached");
		let applied = Apply::apply(&sinks.persistent, batch)?;
		invalidate_flushed(&sinks.range, batch);
		Ok(applied)
	}

	#[instrument(name = "store::operator::resident::revert_batch", level = "debug", skip_all)]
	fn revert_batch(&self, batch: &Arc<FlushBatch>) {
		{
			let mut global = self.shared.global.lock();
			for (flow, version) in &batch.checkpoints {
				global.checkpoints.entry(*flow).or_insert(*version);
			}
			let mut restored = batch.drops.clone();
			restored.append(&mut global.drops);
			global.drops = restored;
			self.clear_in_flight(&mut global);
			global.flushing = false;
		}
		self.shared.idle.notify_all();
	}

	#[instrument(name = "store::operator::resident::persist", level = "debug", skip_all)]
	fn persist(&self, batch: &Arc<FlushBatch>) {
		#[cfg(test)]
		{
			let interlock = self.shared.persist_interlock.lock().take();
			if let Some(interlock) = interlock {
				interlock();
			}
		}
		let sinks = self
			.shared
			.sinks
			.get()
			.expect("the operator resident state flushed before its sinks were attached");
		sinks.persistent.flush_batch(batch);
		invalidate_flushed(&sinks.range, batch);
	}

	#[instrument(name = "store::operator::resident::settle", level = "debug", skip_all)]
	fn settle(&self, batch: Arc<FlushBatch>) {
		{
			let mut global = self.shared.global.lock();
			for operator in global.in_flight_operators.clone() {
				let Some(slot) = self.shared.slot(operator) else {
					continue;
				};
				let mut inner = slot.inner.lock();
				inner.live.settle_flushing();
			}
			self.clear_in_flight(&mut global);
		}
		self.shared.triggered.store(false, Ordering::Release);

		{
			let mut metrics = self.shared.metrics.lock();
			metrics.slices += 1;
			metrics.persisted += batch.writes.len() as u64;
			metrics.released = metrics.released.saturating_add(batch.bytes);
		}

		reifydb_assertions! {
			let _accounting = self.shared.accounting.lock();
			let counted = self.shared.budget.used();
			let walked = self.resident_bytes();
			assert_eq!(
				counted, walked,
				"store::operator::resident resident state byte counter drifted: the budget carries {}, the resident set walks to {}",
				counted, walked
			);
			let counted = self.shared.entries.load(Ordering::Relaxed) as usize;
			let walked = self.resident_entries();
			assert_eq!(
				counted, walked,
				"store::operator::resident resident state entry counter drifted: the budget carries {}, the resident set walks to {}",
				counted, walked
			);
			let counted = self.shared.dirty.load(Ordering::Relaxed) as usize;
			let walked = self.dirty_entries();
			assert_eq!(
				counted, walked,
				"store::operator::resident dirty entry counter drifted: the counter carries {}, the resident set walks to {}",
				counted, walked
			);
			let counted = self.shared.dirty_footprint();
			let walked = self.dirty_bytes();
			assert_eq!(
				counted, walked,
				"store::operator::resident dirty byte counter drifted: the counter carries {}, the resident set walks to {}",
				counted, walked
			);
		}

		self.shared.global.lock().flushing = false;
		self.shared.idle.notify_all();
		self.wake_evictor();
	}

	fn wake_evictor(&self) {
		if !self.shared.budget.over_budget() && !self.shared.over_entry_limit() {
			return;
		}
		let evictor = self.shared.evictor.lock().clone();
		match evictor {
			Some(evictor) => evictor.wake(EvictMessage::Pressure),
			None => {
				self.evict_to_capacity();
			}
		}
	}

	fn wake_checkpoint(&self, flow: FlowId, version: CommitVersion) {
		let waker = self.shared.waker.lock().clone();
		if let Some(waker) = waker {
			waker.wake(FlushMessage::Checkpoint {
				flow,
				version,
			});
		}
	}

	fn observe_write(&self) {
		self.wake_evictor();
		if !self.shared.over_dirty_budget() {
			return;
		}
		if self.shared.triggered.swap(true, Ordering::AcqRel) {
			return;
		}
		self.shared.metrics.lock().wakes += 1;
		let waker = self.shared.waker.lock().clone();
		if let Some(waker) = waker {
			waker.wake(FlushMessage::Pressure);
		}
	}

	fn apply_write(&self, inner: &mut SlotInner, write: &OperatorWrite) {
		match write {
			OperatorWrite::Insert {
				key,
				post,
				..
			} => inner.live.insert_state(key.as_encoded().clone(), Some(post.clone())),
			OperatorWrite::Replace {
				key,
				post,
				..
			} => record_state(inner, key.as_encoded().clone(), Some(post.clone())),
			OperatorWrite::Remove {
				key,
				..
			} => {
				if inner.live.erase_state(key.as_encoded()) {
					#[cfg(reifydb_assertions)]
					self.assert_erasable(inner, key.as_encoded());
					return;
				}
				if self.never_persisted(inner.live.operator, key.as_encoded()) {
					#[cfg(reifydb_assertions)]
					self.assert_erasable(inner, key.as_encoded());
					return;
				}
				record_state(inner, key.as_encoded().clone(), None)
			}
		}
	}

	#[cfg_attr(not(all(feature = "sqlite", not(target_arch = "wasm32"))), allow(dead_code))]
	pub(crate) fn filter(&self) -> Arc<AdaptiveKeyFilter> {
		self.shared.filter.clone()
	}

	pub fn filter_metrics(&self) -> FilterMetrics {
		self.shared.filter.metrics()
	}

	pub(crate) fn never_persisted(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return false;
		};
		!self.shared.filter.may_contain(state_hash(operator, keyspace, group, suffix))
	}

	#[cfg(reifydb_assertions)]
	fn assert_erasable(&self, inner: &SlotInner, key: &EncodedKey) {
		reifydb_assertions! {
			let operator = inner.live.operator;
			let durable = self
				.shared
				.sinks
				.get()
				.and_then(|sinks| {
					sinks.persistent
						.get(operator, &GroupStateKey::bound_unchecked(key.clone()))
						.ok()
						.flatten()
				})
				.is_some();
			let dropped = self.shared.dropped(|marker| match marker {
				DropMarker::OperatorState(candidate) => *candidate == operator,
			});
			assert!(
				!durable || dropped,
				"store::operator::resident collapsed a remove on operator {} over a key that is already durable",
				operator.0
			);
		}
	}
}

pub(crate) fn state_hash(operator: OperatorId, keyspace: KeyspaceId, group: GroupId, suffix: &[u8]) -> u64 {
	hash_item(&(operator.0, keyspace.0, group.as_bytes(), suffix))
}

fn write_operator(write: &OperatorWrite) -> OperatorId {
	match write {
		OperatorWrite::Insert {
			operator,
			..
		}
		| OperatorWrite::Replace {
			operator,
			..
		}
		| OperatorWrite::Remove {
			operator,
			..
		} => *operator,
	}
}

fn drop_operator(marker: &DropMarker) -> OperatorId {
	match marker {
		DropMarker::OperatorState(operator) => *operator,
	}
}

pub(crate) fn record_state(inner: &mut SlotInner, key: EncodedKey, post: Option<EncodedPodRow>) {
	inner.live.record_state(key, post);
}

fn clear_drop(inner: &mut SlotInner, marker: DropMarker) {
	match marker {
		DropMarker::OperatorState(_) => {
			inner.live.clear_state();
		}
	}
}

fn staged_write(post: Option<EncodedPodRow>) -> StagedWrite {
	match post {
		Some(row) => StagedWrite::Set(row),
		None => StagedWrite::Remove,
	}
}

fn invalidate_flushed(range: &OperatorRangeTier, batch: &FlushBatch) {
	for marker in &batch.drops {
		match marker {
			DropMarker::OperatorState(operator) => {
				range.invalidate_operator(*operator);
			}
		}
	}
	for (operator, key, write) in &batch.writes {
		match write {
			StagedWrite::Set(row) => {
				range.insert(*operator, key.as_encoded(), row.clone());
			}
			StagedWrite::Remove => {
				range.retract(*operator, key.as_encoded());
			}
		}
	}
}
