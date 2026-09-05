// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batch;
pub mod evict;
pub mod flush;

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
	key::operator::state::{GroupId, KeyspaceId, OperatorStateKey},
	util::{bloom::hash_item, budget::MemoryBudget},
};
use reifydb_filter::adaptive::AdaptiveKeyFilter;
use reifydb_runtime::{
	actor::mailbox::ActorRef,
	sync::{
		condvar::Condvar,
		mutex::{Mutex, MutexGuard},
	},
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, value::duration::Duration};
use tracing::instrument;

use crate::{
	tier::{
		bucket::write::Staged,
		persistent::OperatorPersistentTier,
		range::tiers::RangeTiers,
		resident::{
			batch::{DropMarker, FlushBatch},
			evict::actor::EvictMessage,
			flush::actor::FlushMessage,
			slot::{Slot, SlotInner},
		},
	},
	types::OperatorWrite,
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
	persistent: OperatorPersistentTier,
	range: Option<RangeTiers>,
}

pub struct Shared {
	slots: DashMap<OperatorId, Arc<Slot>>,
	global: Mutex<GlobalInner>,
	drop_epoch: AtomicU64,
	write_seq: AtomicU64,
	idle: Condvar,
	drain: Mutex<()>,
	accounting: Mutex<()>,
	sinks: OnceLock<OperatorSinks>,
	budget: Arc<MemoryBudget>,
	entries: AtomicU64,
	dirty: AtomicU64,
	dirty_bytes: AtomicU64,
	entry_limit: u64,
	slice: ByteSize,
	dirty_budget: ByteSize,
	waker: Mutex<Option<ActorRef<FlushMessage>>>,
	evictor: Mutex<Option<ActorRef<EvictMessage>>>,
	metrics: Mutex<OperatorResidentStateMetrics>,
	triggered: AtomicBool,
	filter: AdaptiveKeyFilter,
	filter_armed: AtomicBool,
	sweep_cursor: AtomicU64,
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
			filter: AdaptiveKeyFilter::new(),
			filter_armed: AtomicBool::new(false),
			sweep_cursor: AtomicU64::new(0),
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
pub struct OperatorResidentState {
	shared: Arc<Shared>,
}

impl fmt::Debug for OperatorResidentState {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("OperatorResidentState").field("budget", &self.budget()).finish()
	}
}

impl Default for OperatorResidentState {
	fn default() -> Self {
		Self::with_limits(ResidentLimits::default())
	}
}

impl OperatorResidentState {
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

	pub(crate) fn shared(&self) -> &Shared {
		&self.shared
	}

	pub fn attach_flusher(&self, flusher: ActorRef<FlushMessage>) {
		*self.shared.waker.lock() = Some(flusher);
	}

	pub fn attach_evictor(&self, evictor: ActorRef<EvictMessage>) {
		*self.shared.evictor.lock() = Some(evictor);
	}

	pub fn attach_sinks(&self, persistent: OperatorPersistentTier, range: Option<RangeTiers>) {
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

	pub(crate) fn resident_bytes(&self) -> ByteSize {
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

	#[cfg(any(test, reifydb_assertions))]
	pub(crate) fn resident_entries(&self) -> usize {
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
			let mut global = self.shared.global.lock();
			for (flow, version) in checkpoints {
				global.checkpoints.insert(*flow, Some(*version));
			}
			for flow in checkpoint_deletes {
				global.checkpoints.insert(*flow, None);
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
			while global.flushing {
				self.shared.idle.wait(&mut global);
			}
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

	pub fn flush_guard(&self) -> MutexGuard<'_, ()> {
		self.shared.drain.lock()
	}

	pub fn take_for_flush(&self) -> Option<Arc<FlushBatch>> {
		self.take_drain_slice()
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

	pub fn flush_all(&self) {
		let _guard = self.flush_guard();
		while let Some(batch) = self.take_drain_slice() {
			self.persist(&batch);
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
				batch.state.record_bytes(*operator, keyspace, group, suffix, entry.post.clone());
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

	fn arm_filter(&self) {
		if self.shared.filter_armed.swap(true, Ordering::AcqRel) {
			return;
		}
		let empty = self.shared.sinks.get().is_none_or(|sinks| sinks.persistent.census().is_empty());
		if !empty {
			return;
		}
		let handle = self.shared.filter.begin_rebuild(FILTER_KEYS);
		self.shared.filter.commit_rebuild(handle);
	}

	fn take_drain_slice(&self) -> Option<Arc<FlushBatch>> {
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
			if consumed >= self.shared.slice {
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
						batch.state.record_bytes(
							operator,
							keyspace,
							group,
							suffix,
							entry.post.clone(),
						);
						staged += 1;
						if self.shared.filter.is_enabled() {
							self.shared
								.filter
								.add(state_hash(operator, keyspace, group, suffix));
						}
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

		if batch.state.is_empty() && batch.checkpoints.is_empty() && global.drops.is_empty() {
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

	fn persist(&self, batch: &Arc<FlushBatch>) {
		let sinks = self
			.shared
			.sinks
			.get()
			.expect("the operator resident state flushed before its sinks were attached");
		sinks.persistent.flush_batch(batch);
		invalidate_flushed(sinks.range.as_ref(), batch);
	}

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
			metrics.persisted += batch.state.len() as u64;
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
			Some(evictor) => {
				let _ = evictor.send(EvictMessage::Pressure);
			}
			None => {
				self.evict_to_capacity();
			}
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
			let _ = waker.send(FlushMessage::Pressure);
		}
	}

	fn apply_write(&self, inner: &mut SlotInner, write: &OperatorWrite) {
		match write {
			OperatorWrite::Insert {
				key,
				post,
				..
			} => inner.live.insert_state(key.clone(), Some(post.clone())),
			OperatorWrite::Replace {
				key,
				post,
				..
			} => record_state(inner, key.clone(), Some(post.clone())),
			OperatorWrite::Remove {
				key,
				..
			} => {
				if inner.live.erase_state(key) {
					#[cfg(reifydb_assertions)]
					self.assert_erasable(inner, key);
					return;
				}
				if self.never_staged(inner.live.operator, key) {
					#[cfg(reifydb_assertions)]
					self.assert_erasable(inner, key);
					return;
				}
				record_state(inner, key.clone(), None)
			}
		}
	}

	fn never_staged(&self, operator: OperatorId, key: &EncodedKey) -> bool {
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
				.and_then(|sinks| sinks.persistent.get(operator, key))
				.is_some();
			assert!(
				!durable,
				"store::operator::resident collapsed a remove on operator {} over a key that is already durable",
				operator.0
			);
		}
	}
}

fn state_hash(operator: OperatorId, keyspace: KeyspaceId, group: GroupId, suffix: &[u8]) -> u64 {
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

fn invalidate_flushed(range: Option<&RangeTiers>, batch: &FlushBatch) {
	for marker in &batch.drops {
		match marker {
			DropMarker::OperatorState(operator) => {
				if let Some(range) = range {
					range.invalidate_operator(*operator);
				}
			}
		}
	}
	for operator in batch.state.operators() {
		for (key, entry) in batch.state.encoded_entries(operator) {
			match &entry.post {
				Some(row) => {
					if let Some(range) = range {
						range.insert(operator, &key, row.clone());
					}
				}
				None => {
					if let Some(range) = range {
						range.retract(operator, &key);
					}
				}
			}
		}
	}
}
