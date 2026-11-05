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
	collections::BTreeMap,
	fmt, mem,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
	},
};

use dashmap::DashMap;
use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	default,
	interface::{
		catalog::flow::{FlowId, OperatorId},
		operator::FlowCheckpoint,
		store::WalFloorCheckpoint,
	},
	key::operator::{
		keyspace::expiry::{RETIRED_AT_FLUSH, ReapQueueKey},
		state::{GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, OperatorStateKey},
	},
	state::typed::SuffixBytes,
	util::{bloom::hash_item, budget::MemoryBudget},
};
use reifydb_filter::adaptive::{AdaptiveKeyFilter, FilterMetrics};
use reifydb_runtime::sync::{
	mutex::{Mutex, MutexGuard},
	rwlock::RwLock,
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, value::duration::Duration};
use reifydb_wal::lsn::Lsn;
use tracing::{Span, field::Empty, instrument};

#[cfg(reifydb_assertions)]
use crate::persistent::Fetch;
use crate::{
	actor::{Waker, resident_evict::EvictMessage, resident_flush::FlushMessage},
	error::Result,
	persistent::{Apply, Enumerate, Persistent, PersistentTier},
	range::{OperatorRangeTier, RangeSink},
	resident::slot::{Slot, SlotInner},
	store::census::OperatorCensus,
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

pub const TOMBSTONE_LIMIT: u64 = if default::TESTING {
	default::store::OPERATOR_RESIDENT_TOMBSTONES_TESTING
} else {
	default::store::OPERATOR_RESIDENT_TOMBSTONES
};

const OPERATOR_LOCAL_FRACTION: u64 = 16;

const SWEEP_STARVATION_LIMIT: u32 = 10;

pub const DIRTY_BUDGET_BYTES: ByteSize = if default::TESTING {
	default::store::OPERATOR_DIRTY_BUDGET_TESTING
} else {
	default::store::OPERATOR_DIRTY_BUDGET
};

pub const FLUSH_INTERVAL: Duration = if default::TESTING {
	default::store::OPERATOR_RESIDENT_FLUSH_EVERY_TESTING
} else {
	default::store::OPERATOR_RESIDENT_FLUSH_EVERY
};

pub const FILTER_KEYS: u64 = if default::TESTING {
	default::store::OPERATOR_FILTER_KEYS_TESTING
} else {
	default::store::OPERATOR_FILTER_KEYS
};

#[derive(Debug, Clone, Copy)]
pub struct ResidentLimits {
	pub budget: ByteSize,
	pub entries: u64,
	pub tombstones: u64,
	pub dirty_budget: ByteSize,
}

impl Default for ResidentLimits {
	fn default() -> Self {
		Self {
			budget: FLUSH_BUDGET_BYTES,
			entries: FLUSH_ENTRY_LIMIT,
			tombstones: TOMBSTONE_LIMIT,
			dirty_budget: DIRTY_BUDGET_BYTES,
		}
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorResidentStateMetrics {
	pub wakes: u64,
	pub tick_wakes: u64,
	pub evict_wakes: u64,
	pub slices: u64,
	pub persisted: u64,
	pub reclaimed: u64,
	pub evicted: u64,
	pub released: ByteSize,
	pub backlog: ByteSize,
}

#[derive(Debug, Default)]
struct GlobalInner {
	checkpoints: BTreeMap<FlowId, Option<FlowCheckpoint>>,
	in_flight_checkpoints: BTreeMap<FlowId, Option<FlowCheckpoint>>,
	first_unflushed: BTreeMap<FlowId, Lsn>,
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

const RETIRE_CHUNK_GROUPS: usize = 64;

struct FrozenSet {
	rows: Vec<(OperatorId, KeyspaceId, Box<dyn bucket::FrozenRows>)>,
	checkpoints: BTreeMap<FlowId, Option<FlowCheckpoint>>,
	drops: Vec<DropMarker>,
	bytes: ByteSize,
	through: Option<Lsn>,
}

struct OperatorSinks {
	persistent: PersistentTier,
	range: OperatorRangeTier,
	checkpoint: Option<Arc<dyn WalFloorCheckpoint>>,
}

pub struct OperatorCommitGuard {
	shared: Arc<Shared>,
	id: u64,
}

impl Drop for OperatorCommitGuard {
	fn drop(&mut self) {
		self.shared.live_heads.lock().remove(&self.id);
		self.shared.in_flight.fetch_sub(1, Ordering::SeqCst);
	}
}

#[cfg(test)]
pub(crate) type PersistInterlock = Box<dyn Fn() + Send + Sync>;

pub struct Shared {
	slots: DashMap<OperatorId, Arc<Slot>>,
	global: Mutex<GlobalInner>,
	drops_outstanding: AtomicBool,
	drain: Mutex<()>,
	flusher: Mutex<()>,
	accounting: RwLock<()>,
	sinks: OnceLock<OperatorSinks>,
	census: OnceLock<Arc<OperatorCensus>>,
	budget: Arc<MemoryBudget>,
	entries: AtomicU64,
	tombstones: AtomicU64,
	dirty: AtomicU64,
	dirty_bytes: AtomicU64,
	entry_limit: u64,
	tombstone_limit: u64,
	dirty_budget: ByteSize,
	waker: Mutex<Option<Waker<FlushMessage>>>,
	evictor: Mutex<Option<Waker<EvictMessage>>>,
	metrics: Mutex<OperatorResidentStateMetrics>,
	triggered: AtomicBool,
	evict_pending: AtomicBool,
	evict_parked: AtomicBool,
	operator_over_local_cap: AtomicBool,
	filter: Arc<AdaptiveKeyFilter>,
	filter_decided: AtomicBool,
	sweep_cursor: AtomicU64,
	in_flight: AtomicUsize,
	next_guard: AtomicU64,
	live_heads: Mutex<BTreeMap<u64, Option<Lsn>>>,
	applied: Mutex<Option<Lsn>>,
	last_saved: Mutex<Option<Lsn>>,
	#[cfg(test)]
	persist_interlock: Mutex<Option<PersistInterlock>>,
	#[cfg(test)]
	stage_interlock: Mutex<Option<PersistInterlock>>,
}

impl Shared {
	fn new(limits: ResidentLimits) -> Self {
		Self {
			slots: DashMap::new(),
			global: Mutex::new(GlobalInner::default()),
			drops_outstanding: AtomicBool::new(false),
			drain: Mutex::new(()),
			flusher: Mutex::new(()),
			accounting: RwLock::new(()),
			sinks: OnceLock::new(),
			census: OnceLock::new(),
			budget: Arc::new(MemoryBudget::new(limits.budget)),
			entries: AtomicU64::new(0),
			tombstones: AtomicU64::new(0),
			dirty: AtomicU64::new(0),
			dirty_bytes: AtomicU64::new(0),
			entry_limit: limits.entries,
			tombstone_limit: limits.tombstones,
			dirty_budget: limits.dirty_budget,
			waker: Mutex::new(None),
			evictor: Mutex::new(None),
			metrics: Mutex::new(OperatorResidentStateMetrics::default()),
			triggered: AtomicBool::new(false),
			evict_pending: AtomicBool::new(false),
			evict_parked: AtomicBool::new(false),
			operator_over_local_cap: AtomicBool::new(false),
			filter: Arc::new(AdaptiveKeyFilter::new()),
			filter_decided: AtomicBool::new(false),
			sweep_cursor: AtomicU64::new(0),
			in_flight: AtomicUsize::new(0),
			next_guard: AtomicU64::new(0),
			live_heads: Mutex::new(BTreeMap::new()),
			applied: Mutex::new(None),
			last_saved: Mutex::new(None),
			#[cfg(test)]
			persist_interlock: Mutex::new(None),
			#[cfg(test)]
			stage_interlock: Mutex::new(None),
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

	fn charge_tombstones(&self, count: usize) {
		self.tombstones.fetch_add(count as u64, Ordering::Relaxed);
	}

	fn release_tombstones(&self, count: usize) {
		let amount = count as u64;
		let mut current = self.tombstones.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_sub(amount);
			match self.tombstones.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
			{
				Ok(_) => return,
				Err(observed) => current = observed,
			}
		}
	}

	fn over_tombstone_limit(&self) -> bool {
		self.tombstones.load(Ordering::Relaxed) > self.tombstone_limit
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

	fn local_caps(&self, live_operators: u64) -> (ByteSize, u64) {
		let share = live_operators.clamp(1, OPERATOR_LOCAL_FRACTION);
		(ByteSize::from_bytes(self.budget.limit().as_bytes() / share), self.tombstone_limit / share)
	}

	pub(crate) fn dropped(&self, predicate: impl Fn(&DropMarker) -> bool) -> bool {
		if !self.drops_outstanding.load(Ordering::Acquire) {
			return false;
		}
		self.global.lock().any_drop(predicate)
	}

	fn publish_drops(&self, global: &GlobalInner) {
		let outstanding = !global.drops.is_empty() || !global.in_flight_drops.is_empty();
		self.drops_outstanding.store(outstanding, Ordering::Release);
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

	#[cfg(test)]
	pub(crate) fn set_stage_interlock(&self, interlock: PersistInterlock) {
		*self.shared.stage_interlock.lock() = Some(interlock);
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

	pub(crate) fn attach_census(&self, census: Arc<OperatorCensus>) {
		let _ = self.shared.census.set(census);
	}

	pub fn attach_sinks(
		&self,
		persistent: PersistentTier,
		range: OperatorRangeTier,
		checkpoint: Option<Arc<dyn WalFloorCheckpoint>>,
	) {
		let _ = self.shared.sinks.set(OperatorSinks {
			persistent,
			range,
			checkpoint,
		});
	}

	fn checkpoint(&self) -> Option<&Arc<dyn WalFloorCheckpoint>> {
		self.shared.sinks.get().and_then(|sinks| sinks.checkpoint.as_ref())
	}

	pub fn begin_commit(&self) -> OperatorCommitGuard {
		self.shared.in_flight.fetch_add(1, Ordering::SeqCst);
		let head = self.checkpoint().and_then(|checkpoint| {
			checkpoint.appended_head().unwrap_or_else(|err| {
				panic!("operator commit could not read the commit log appended head: {err}")
			})
		});
		let id = self.shared.next_guard.fetch_add(1, Ordering::Relaxed);
		self.shared.live_heads.lock().insert(id, head);
		OperatorCommitGuard {
			shared: Arc::clone(&self.shared),
			id,
		}
	}

	fn marker_through(&self) -> Option<Lsn> {
		let applied = *self.shared.applied.lock();
		let lowest_live = self.shared.live_heads.lock().values().min().copied();
		match lowest_live {
			Some(head) => applied.min(head),
			None => applied,
		}
	}

	pub fn budget(&self) -> ByteSize {
		self.shared.budget.limit()
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
			total = total.saturating_add(slot.inner.lock().buckets.footprint());
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
			total += inner.buckets.flushing_count();
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
			total = total.saturating_add(slot.inner.lock().buckets.dirty_count());
		}
		total
	}

	#[cfg(reifydb_assertions)]
	pub(crate) fn dirty_bytes(&self) -> ByteSize {
		let mut total = ByteSize::ZERO;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			total = total.saturating_add(slot.inner.lock().buckets.dirty_footprint());
		}
		total
	}

	pub fn resident_entries(&self) -> usize {
		let mut total = 0usize;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			total = total.saturating_add(slot.inner.lock().buckets.entry_count());
		}
		total
	}

	pub fn resident_tombstones(&self) -> usize {
		let mut total = 0usize;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			total = total.saturating_add(slot.inner.lock().buckets.tombstone_count());
		}
		total
	}

	pub fn apply_batch_with_checkpoints(
		&self,
		writes: &[OperatorWrite],
		checkpoints: &[(FlowId, FlowCheckpoint)],
		checkpoint_deletes: &[FlowId],
		lsn: Option<Lsn>,
	) {
		if let Some(lsn) = lsn {
			let mut applied = self.shared.applied.lock();
			*applied = (*applied).max(Some(lsn));
		}
		if writes.is_empty() && checkpoints.is_empty() && checkpoint_deletes.is_empty() {
			return;
		}
		self.record_writes(writes);
		if !checkpoints.is_empty() || !checkpoint_deletes.is_empty() {
			{
				let mut global = self.shared.global.lock();
				for (flow, checkpoint) in checkpoints {
					global.checkpoints.insert(*flow, Some(*checkpoint));
					global.first_unflushed.entry(*flow).or_insert(checkpoint.lsn);
				}
				for flow in checkpoint_deletes {
					global.checkpoints.insert(*flow, None);
					global.first_unflushed.remove(flow);
				}
			}
		}
		self.observe_write();
	}

	#[instrument(name = "store::operator::resident::record_writes", level = "trace", skip(self, writes), fields(write_count = writes.len()))]
	fn record_writes(&self, writes: &[OperatorWrite]) {
		let mut grouped: BTreeMap<OperatorId, Vec<&OperatorWrite>> = BTreeMap::new();
		for write in writes {
			grouped.entry(write_operator(write)).or_default().push(write);
		}
		let (local_budget, local_tombstone_limit) = self.shared.local_caps(self.shared.slots.len() as u64);
		for (operator, group) in grouped {
			let slot = self.shared.slot_or_create(operator);
			let _accounting = self.shared.accounting.read();
			let mut inner = slot.inner.lock();
			let before = inner.buckets.totals();
			for write in group {
				self.apply_write(&mut inner, write);
			}
			let after = inner.buckets.totals();
			self.shared.budget.charge(after.footprint.saturating_sub(before.footprint));
			self.shared.budget.release(before.footprint.saturating_sub(after.footprint));
			self.shared.charge_entries(after.entries.saturating_sub(before.entries));
			self.shared.release_entries(before.entries.saturating_sub(after.entries));
			self.shared.charge_tombstones(after.tombstones.saturating_sub(before.tombstones));
			self.shared.release_tombstones(before.tombstones.saturating_sub(after.tombstones));
			self.shared.charge_dirty(after.dirty.saturating_sub(before.dirty));
			self.shared.release_dirty(before.dirty.saturating_sub(after.dirty));
			self.shared.charge_dirty_bytes(after.dirty_footprint.saturating_sub(before.dirty_footprint));
			self.shared.release_dirty_bytes(before.dirty_footprint.saturating_sub(after.dirty_footprint));
			if after.footprint.as_bytes() > local_budget.as_bytes()
				|| after.tombstones as u64 > local_tombstone_limit
			{
				self.shared.operator_over_local_cap.store(true, Ordering::Release);
			}
		}
	}

	pub fn record_drop(&self, marker: DropMarker) {
		let _staging = self.flush_guard();
		let operator = drop_operator(&marker);
		{
			let mut global = self.shared.global.lock();
			global.drops.push(marker);
			self.shared.publish_drops(&global);
		}
		let Some(slot) = self.shared.slot(operator) else {
			return;
		};
		let _accounting = self.shared.accounting.read();
		let mut inner = slot.inner.lock();
		let before = inner.buckets.footprint();
		let before_entries = inner.buckets.entry_count();
		let before_tombstones = inner.buckets.tombstone_count();
		let before_dirty = inner.buckets.dirty_count();
		let before_dirty_bytes = inner.buckets.dirty_footprint();
		clear_drop(&mut inner, marker);
		let after = inner.buckets.footprint();
		let after_entries = inner.buckets.entry_count();
		let after_tombstones = inner.buckets.tombstone_count();
		let after_dirty = inner.buckets.dirty_count();
		let after_dirty_bytes = inner.buckets.dirty_footprint();
		self.shared.budget.release(before.saturating_sub(after));
		self.shared.release_entries(before_entries.saturating_sub(after_entries));
		self.shared.release_tombstones(before_tombstones.saturating_sub(after_tombstones));
		self.shared.release_dirty(before_dirty.saturating_sub(after_dirty));
		self.shared.release_dirty_bytes(before_dirty_bytes.saturating_sub(after_dirty_bytes));
	}

	#[instrument(name = "store::operator::resident::evict_to_capacity", level = "debug", skip_all, fields(evicted = Empty, freed = Empty))]
	pub fn evict_to_capacity(&self) -> (usize, ByteSize) {
		self.shared.evict_pending.store(false, Ordering::Release);
		let mut evicted = 0usize;
		let mut freed = ByteSize::ZERO;
		for _ in 0..CLOCK_PASSES {
			let (mut bytes, mut entries) = self.overshoot();
			if bytes.as_bytes() == 0
				&& entries == 0 && !self.shared.operator_over_local_cap.load(Ordering::Acquire)
			{
				break;
			}
			let (count, released) = self.sweep(&mut bytes, &mut entries);
			evicted += count;
			freed = freed.saturating_add(released);
		}
		let (bytes, tombstones) = self.overshoot();
		self.shared.evict_parked.store(bytes.as_bytes() > 0 || tombstones > 0, Ordering::Release);
		if evicted > 0 {
			let mut metrics = self.shared.metrics.lock();
			metrics.evicted += evicted as u64;
			metrics.reclaimed = metrics.reclaimed.saturating_add(freed.as_bytes());
		}
		let span = Span::current();
		span.record("evicted", evicted);
		span.record("freed", freed.as_bytes());
		(evicted, freed)
	}

	fn overshoot(&self) -> (ByteSize, usize) {
		let bytes = self.shared.budget.used().saturating_sub(self.shared.budget.limit());
		let tombstones =
			self.shared.tombstones.load(Ordering::Relaxed).saturating_sub(self.shared.tombstone_limit);
		(bytes, tombstones as usize)
	}

	#[instrument(name = "store::operator::resident::sweep", level = "trace", skip(self, bytes, tombstones))]
	fn sweep(&self, bytes: &mut ByteSize, tombstones: &mut usize) -> (usize, ByteSize) {
		let operators = self.shared.operators();
		if operators.is_empty() {
			return (0, ByteSize::ZERO);
		}
		let _accounting = self.shared.accounting.read();
		let start = self.shared.sweep_cursor.fetch_add(1, Ordering::Relaxed) as usize % operators.len();
		let (local_budget, local_tombstone_limit) = self.shared.local_caps(operators.len() as u64);
		let mut evicted = 0usize;
		let mut freed = ByteSize::ZERO;
		let mut any_over_local_cap = false;
		for offset in 0..operators.len() {
			let operator = operators[(start + offset) % operators.len()];
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let mut inner = slot.inner.lock();
			let before_tombstones = inner.buckets.tombstone_count();
			let local_byte_overage =
				inner.buckets.footprint().as_bytes().saturating_sub(local_budget.as_bytes());
			let local_tombstone_overage = before_tombstones.saturating_sub(local_tombstone_limit as usize);
			let global_deficit_remains = bytes.as_bytes() > 0 || *tombstones > 0;
			let starved = inner.sweeps_since_swept >= SWEEP_STARVATION_LIMIT;
			if !global_deficit_remains
				&& local_byte_overage == 0 && local_tombstone_overage == 0
				&& !starved
			{
				inner.sweeps_since_swept += 1;
				continue;
			}
			inner.sweeps_since_swept = 0;
			let (count, released) = if global_deficit_remains {
				inner.buckets.evict_clean(bytes, tombstones)
			} else if local_byte_overage > 0 || local_tombstone_overage > 0 {
				let mut local_bytes = ByteSize::from_bytes(local_byte_overage);
				let mut local_tombstones = local_tombstone_overage;
				inner.buckets.evict_clean(&mut local_bytes, &mut local_tombstones)
			} else {
				let mut unlimited_bytes = ByteSize::from_bytes(u64::MAX);
				let mut unlimited_tombstones = usize::MAX;
				inner.buckets.evict_clean(&mut unlimited_bytes, &mut unlimited_tombstones)
			};
			self.shared.budget.release(released);
			self.shared.release_entries(count);
			self.shared
				.release_tombstones(before_tombstones.saturating_sub(inner.buckets.tombstone_count()));
			evicted += count;
			freed = freed.saturating_add(released);
			if inner.buckets.footprint().as_bytes() > local_budget.as_bytes()
				|| inner.buckets.tombstone_count() > local_tombstone_limit as usize
			{
				any_over_local_cap = true;
			}
			let evicted_rows = inner.buckets.take_evicted();
			drop(inner);
			drop(evicted_rows);
		}
		self.shared.operator_over_local_cap.store(any_over_local_cap, Ordering::Release);
		(evicted, freed)
	}

	#[instrument(name = "store::operator::resident::flush_acquire", level = "debug", skip_all)]
	pub fn flush_guard(&self) -> MutexGuard<'_, ()> {
		self.shared.drain.lock()
	}

	#[cfg(any(test, feature = "testing"))]
	pub fn take_for_flush(&self) -> Option<Arc<FlushBatch>> {
		let frozen = self.swap_pending()?;
		Some(self.frozen_batch(frozen))
	}

	#[cfg(any(test, feature = "testing"))]
	pub fn settle_flush(&self, batch: Arc<FlushBatch>) {
		self.settle(&batch);
	}

	#[instrument(name = "store::operator::resident::pending", level = "trace", skip(self))]
	pub fn pending(&self) -> bool {
		let staged = {
			let global = self.shared.global.lock();
			!global.checkpoints.is_empty() || !global.drops.is_empty()
		};
		staged || self.shared.dirty.load(Ordering::Relaxed) > 0
	}

	#[instrument(name = "store::operator::resident::flush_all", level = "debug", skip_all)]
	pub fn flush_all(&self) {
		let _flusher = self.shared.flusher.lock();
		if self.device_absent() {
			return;
		}
		let mut flushed = false;
		while self.flush_pass() {
			flushed = true;
		}
		if !flushed {
			self.advance_idle();
		}
	}

	#[instrument(name = "store::operator::resident::flush_once", level = "debug", skip_all)]
	pub fn flush_once(&self) {
		let _flusher = self.shared.flusher.lock();
		if self.device_absent() {
			return;
		}
		if !self.flush_pass() {
			self.advance_idle();
		}
	}

	fn flush_pass(&self) -> bool {
		let taken = {
			let _staging = self.flush_guard();
			self.swap_pending()
		};
		let Some(frozen) = taken else {
			return false;
		};
		let batch = self.frozen_batch(frozen);
		let applied = self
			.persist(&batch)
			.expect("operator state flush must persist; a dropped batch loses buffered rows");
		if let Some(census) = self.shared.census.get() {
			census.retire(&applied.retired);
		}
		{
			let _staging = self.flush_guard();
			self.settle(&batch);
		}
		self.retire_resident(&batch);
		true
	}

	fn retire_resident(&self, batch: &FlushBatch) {
		if batch.retired.is_empty() {
			return;
		}
		let mut by_operator: BTreeMap<OperatorId, Vec<GroupId>> = BTreeMap::new();
		for (operator, group) in &batch.retired {
			by_operator.entry(*operator).or_default().push(*group);
		}
		for (operator, groups) in by_operator {
			if let Some(sinks) = self.shared.sinks.get() {
				for group in &groups {
					sinks.range.invalidate_group(operator, *group, KeyspaceMask::data());
				}
			}
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			for chunk in groups.chunks(RETIRE_CHUNK_GROUPS) {
				let _accounting = self.shared.accounting.read();
				let mut inner = slot.inner.lock();
				let (entries, tombstones, freed) = inner.buckets.retire_groups(operator, chunk);
				drop(inner);
				self.shared.budget.release(freed);
				self.shared.release_entries(entries);
				self.shared.release_tombstones(tombstones);
			}
		}
	}

	fn advance_idle(&self) {
		let Some(checkpoint) = self.checkpoint() else {
			return;
		};
		let head = checkpoint.durable_head().unwrap_or_else(|err| {
			panic!("operator idle flush could not read the commit log durable head: {err}")
		});
		{
			let _staging = self.flush_guard();
			if self.shared.in_flight.load(Ordering::SeqCst) != 0 || self.pending() {
				return;
			}
		}
		let Some(head) = head else {
			return;
		};
		if self.shared.last_saved.lock().is_some_and(|last| head <= last) {
			return;
		}
		let marker = Arc::new(FlushBatch {
			through: Some(head),
			..FlushBatch::default()
		});
		self.persist(&marker).expect("operator idle flush must persist its saved lsn marker");
	}

	/// Arming publishes an empty filter as the authoritative one, which is only sound over a device
	/// that holds nothing: from there every persisted key passes through `add` and the filter sees
	/// all of them. Over a device that already holds rows it is a filter that rejects keys the store
	/// still has, and a rejection is final, so a read answers absent for a durable row. Anything short
	/// of a positive answer that the device is empty therefore has to leave the filter alone: an
	/// unarmed filter admits every key and costs a device read, which is the direction that stays
	/// correct.
	fn arm_filter(&self) {
		if self.shared.filter_decided.load(Ordering::Acquire) {
			return;
		}
		let Some(sinks) = self.shared.sinks.get() else {
			return;
		};
		let Ok(census) = sinks.persistent.census() else {
			return;
		};
		if self.shared.filter_decided.swap(true, Ordering::AcqRel) || !census.is_empty() {
			return;
		}
		let handle = self.shared.filter.begin_rebuild(FILTER_KEYS);
		self.shared.filter.commit_rebuild(handle);
	}

	#[instrument(name = "store::operator::resident::swap_pending", level = "debug", skip_all)]
	fn swap_pending(&self) -> Option<FrozenSet> {
		self.arm_filter();
		let mut rows: Vec<(OperatorId, KeyspaceId, Box<dyn bucket::FrozenRows>)> = Vec::new();
		let mut touched: Vec<OperatorId> = Vec::new();
		let mut staged = 0usize;
		let mut bytes = ByteSize::ZERO;
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let frozen = slot.inner.lock().buckets.freeze();
			if frozen.rows.is_empty() {
				continue;
			}
			staged += frozen.count;
			bytes = bytes.saturating_add(frozen.bytes);
			touched.push(operator);
			rows.extend(frozen.rows.into_iter().map(|(keyspace, frozen)| (operator, keyspace, frozen)));
		}
		#[cfg(test)]
		{
			let interlock = self.shared.stage_interlock.lock().take();
			if let Some(interlock) = interlock {
				interlock();
			}
		}
		self.shared.release_dirty(staged);
		self.shared.release_dirty_bytes(bytes);

		let mut global = self.shared.global.lock();
		if rows.is_empty() && global.checkpoints.is_empty() && global.drops.is_empty() {
			return None;
		}
		let through = match self.checkpoint().is_some() {
			true => self.marker_through(),
			false => None,
		};
		let checkpoints = mem::take(&mut global.checkpoints);
		let drops = mem::take(&mut global.drops);
		global.in_flight_checkpoints = checkpoints.clone();
		global.in_flight_drops = drops.clone();
		self.shared.publish_drops(&global);
		global.in_flight_operators = touched;
		global.flushing = true;
		Some(FrozenSet {
			rows,
			checkpoints,
			drops,
			bytes,
			through,
		})
	}

	#[instrument(name = "store::operator::resident::frozen_batch", level = "debug", skip_all)]
	fn frozen_batch(&self, frozen: FrozenSet) -> Arc<FlushBatch> {
		let FrozenSet {
			rows,
			checkpoints,
			drops,
			bytes,
			through,
		} = frozen;
		let mut writes = Vec::new();
		let mut retired = Vec::new();
		let mut hashes: Vec<u64> = Vec::new();
		for (operator, keyspace, frozen) in &rows {
			frozen.for_each(&mut |group, suffix, post| {
				if *keyspace == KeyspaceId::REAP_QUEUE
					&& post.as_ref().is_some_and(|row| row.body() == RETIRED_AT_FLUSH)
				{
					let key = <ReapQueueKey as SuffixBytes>::from_suffix_bytes(suffix)
						.expect("a reap queue row carries a reap queue key");
					retired.push((*operator, key.group.0));
					return;
				}
				writes.push((
					*operator,
					GroupStateKey::new(group, *keyspace, suffix),
					staged_write(post.clone()),
				));
				hashes.push(state_hash(*operator, *keyspace, group, suffix));
			});
		}
		self.shared.filter.add_many(&hashes);
		Arc::new(FlushBatch {
			writes,
			checkpoints,
			drops,
			retired,
			bytes,
			through,
		})
	}

	fn device_absent(&self) -> bool {
		self.shared.sinks.get().is_some_and(|sinks| sinks.persistent.is_absent())
	}

	#[instrument(name = "store::operator::resident::persist", level = "debug", skip_all)]
	fn persist(&self, batch: &Arc<FlushBatch>) -> Result<Applied> {
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
		let durable = match batch.through {
			Some(through) => Some(through),
			None if !batch.writes.is_empty() && self.checkpoint().is_some() => *self.shared.applied.lock(),
			None => None,
		};
		if let Some(through) = durable {
			self.checkpoint()
				.expect(
					"a flush batch carries a saved lsn only when a commit log checkpoint is attached",
				)
				.make_durable(through)
				.unwrap_or_else(|err| {
					panic!(
						"operator flush could not make the commit log durable through lsn {}: {err}",
						through.as_u64()
					)
				});
		}
		let applied = Apply::apply(&sinks.persistent, batch)?;
		if let Some(through) = batch.through {
			self.checkpoint()
				.expect(
					"a flush batch carries a saved lsn only when a commit log checkpoint is attached",
				)
				.saved_through(through)
				.unwrap_or_else(|err| {
					panic!(
						"operator flush saved through lsn {} but could not move its commit log floor: {err}",
						through.as_u64()
					)
				});
			*self.shared.last_saved.lock() = Some(through);
		}
		self.shared.filter.add_many(
			&batch.writes.iter().map(|(operator, key, _)| staged_hash(*operator, key)).collect::<Vec<_>>(),
		);
		invalidate_flushed(&sinks.range, batch);
		Ok(applied)
	}

	#[instrument(name = "store::operator::resident::settle", level = "debug", skip_all)]
	fn settle(&self, batch: &FlushBatch) {
		{
			let mut global = self.shared.global.lock();
			for operator in global.in_flight_operators.clone() {
				let Some(slot) = self.shared.slot(operator) else {
					continue;
				};
				let mut inner = slot.inner.lock();
				let before_tombstones = inner.buckets.tombstone_count();
				inner.buckets.settle_flushing();
				self.shared.charge_tombstones(
					inner.buckets.tombstone_count().saturating_sub(before_tombstones),
				);
			}
			global.in_flight_operators.clear();
			for flow in batch.checkpoints.keys() {
				global.first_unflushed.remove(flow);
			}
			global.in_flight_checkpoints.clear();
			global.in_flight_drops.clear();
			self.shared.publish_drops(&global);
		}
		self.shared.triggered.store(false, Ordering::Release);

		{
			let mut metrics = self.shared.metrics.lock();
			metrics.slices += 1;
			metrics.persisted += batch.writes.len() as u64;
			metrics.released = metrics.released.saturating_add(batch.bytes);
		}

		reifydb_assertions! {
			let _accounting = self.shared.accounting.write();
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
			let counted = self.shared.tombstones.load(Ordering::Relaxed) as usize;
			let walked = self.resident_tombstones();
			assert_eq!(
				counted, walked,
				"store::operator::resident tombstone counter drifted: the budget carries {}, the resident set walks to {}",
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
		self.shared.evict_parked.store(false, Ordering::Release);
		self.wake_evictor();
	}

	fn wake_evictor(&self) {
		if !self.shared.budget.over_budget()
			&& !self.shared.over_tombstone_limit()
			&& !self.shared.operator_over_local_cap.load(Ordering::Acquire)
		{
			return;
		}
		if self.shared.evict_parked.load(Ordering::Acquire) {
			return;
		}
		if self.shared.evict_pending.swap(true, Ordering::AcqRel) {
			return;
		}
		self.shared.metrics.lock().evict_wakes += 1;
		let evictor = self.shared.evictor.lock().clone();
		match evictor {
			Some(evictor) => evictor.wake(EvictMessage::Pressure),
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
			waker.wake(FlushMessage::Pressure);
		}
	}

	fn apply_write(&self, inner: &mut SlotInner, write: &OperatorWrite) {
		match write {
			OperatorWrite::Insert {
				key,
				post,
				..
			} => inner.insert_state(key.as_encoded().clone(), Some(post.clone())),
			OperatorWrite::Replace {
				key,
				post,
				..
			} => record_state(inner, key.as_encoded().clone(), Some(post.clone())),
			OperatorWrite::Remove {
				key,
				..
			} => {
				if inner.erase_state(key.as_encoded()) {
					#[cfg(reifydb_assertions)]
					self.assert_erasable(inner, key.as_encoded());
					return;
				}
				if inner.lookup(key.as_encoded()).is_none()
					&& self.never_persisted(inner.operator, key.as_encoded())
				{
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
			let operator = inner.operator;
			let durable = self
				.shared
				.sinks
				.get()
				.and_then(|sinks| {
					sinks.persistent
						.get(operator, &[GroupStateKey::bound_unchecked(key.clone())])
						.ok()
						.and_then(|mut rows| rows.pop())
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

fn staged_hash(operator: OperatorId, key: &GroupStateKey) -> u64 {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
		.expect("a staged operator state key decodes into group, keyspace and suffix");
	state_hash(operator, keyspace, group, suffix)
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
	inner.record_state(key, post);
}

fn clear_drop(inner: &mut SlotInner, marker: DropMarker) {
	match marker {
		DropMarker::OperatorState(_) => {
			inner.clear_state();
		}
	}
}

fn staged_write(post: Option<EncodedPodRow>) -> StagedWrite {
	match post {
		Some(row) => StagedWrite::Set(row),
		None => StagedWrite::Remove,
	}
}

fn invalidate_flushed(range: &impl RangeSink, batch: &FlushBatch) {
	for marker in &batch.drops {
		match marker {
			DropMarker::OperatorState(operator) => {
				range.invalidate_operator(*operator);
			}
		}
	}
	let mut run: Vec<&EncodedKey> = Vec::new();
	let mut run_operator: Option<OperatorId> = None;
	for (operator, key, write) in &batch.writes {
		if (!matches!(write, StagedWrite::Remove) || run_operator != Some(*operator))
			&& let Some(owner) = run_operator.take()
		{
			range.retract_run(owner, &run);
			run.clear();
		}
		match write {
			StagedWrite::Set(row) => {
				range.insert(*operator, key.as_encoded(), row.clone());
			}
			StagedWrite::Remove => {
				run_operator = Some(*operator);
				run.push(key.as_encoded());
			}
		}
	}
	if let Some(owner) = run_operator {
		range.retract_run(owner, &run);
	}
}
