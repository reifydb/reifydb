// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batch;
pub mod flush;

mod census;
mod checkpoint;
mod join_expiry;
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
	util::budget::MemoryBudget,
};
use reifydb_runtime::{
	actor::mailbox::ActorRef,
	sync::{
		condvar::Condvar,
		mutex::{Mutex, MutexGuard},
	},
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions};

use crate::{
	tier::{
		persistent::OperatorPersistentTier,
		point::tiers::PointTiers,
		range::tiers::RangeTiers,
		resident::{
			batch::{DropMarker, FlushBatch},
			flush::actor::FlushMessage,
			slot::{OperatorLive, Slot, SlotInner, SlotJoinKey},
		},
	},
	types::{DurablePre, OperatorWrite},
};

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

pub const SLICE_BYTES: ByteSize = if default::TESTING {
	default::store::OPERATOR_FLUSH_SLICE_TESTING
} else {
	default::store::OPERATOR_FLUSH_SLICE
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorResidentStateMetrics {
	pub wakes: u64,
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
	point: Option<PointTiers>,
	range: Option<RangeTiers>,
}

pub struct Shared {
	slots: DashMap<OperatorId, Arc<Slot>>,
	global: Mutex<GlobalInner>,
	drop_epoch: AtomicU64,
	write_seq: AtomicU64,
	idle: Condvar,
	drain: Mutex<()>,
	sinks: OnceLock<OperatorSinks>,
	budget: Arc<MemoryBudget>,
	entries: AtomicU64,
	entry_limit: u64,
	slice: ByteSize,
	waker: Mutex<Option<ActorRef<FlushMessage>>>,
	metrics: Mutex<OperatorResidentStateMetrics>,
	triggered: AtomicBool,
}

impl Shared {
	fn new(cap: ByteSize, entry_limit: u64) -> Self {
		Self {
			slots: DashMap::new(),
			global: Mutex::new(GlobalInner::default()),
			drop_epoch: AtomicU64::new(0),
			write_seq: AtomicU64::new(0),
			idle: Condvar::new(),
			drain: Mutex::new(()),
			sinks: OnceLock::new(),
			budget: Arc::new(MemoryBudget::new(cap)),
			entries: AtomicU64::new(0),
			entry_limit,
			slice: cap.min(SLICE_BYTES),
			waker: Mutex::new(None),
			metrics: Mutex::new(OperatorResidentStateMetrics::default()),
			triggered: AtomicBool::new(false),
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
		Self::with_limits(FLUSH_BUDGET_BYTES, FLUSH_ENTRY_LIMIT)
	}
}

impl OperatorResidentState {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_budget(budget: ByteSize) -> Self {
		Self::with_limits(budget, FLUSH_ENTRY_LIMIT)
	}

	pub fn with_limits(budget: ByteSize, entries: u64) -> Self {
		Self {
			shared: Arc::new(Shared::new(budget, entries)),
		}
	}

	pub(crate) fn shared(&self) -> &Shared {
		&self.shared
	}

	pub fn attach_flusher(&self, flusher: ActorRef<FlushMessage>) {
		*self.shared.waker.lock() = Some(flusher);
	}

	pub fn attach_sinks(
		&self,
		persistent: OperatorPersistentTier,
		point: Option<PointTiers>,
		range: Option<RangeTiers>,
	) {
		let _ = self.shared.sinks.set(OperatorSinks {
			persistent,
			point,
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
			for write in group {
				apply_write(&mut inner, write);
			}
			let after = inner.live.bytes;
			let after_entries = inner.live.entry_count();
			self.shared.budget.charge(after.saturating_sub(before));
			self.shared.budget.release(before.saturating_sub(after));
			self.shared.charge_entries(after_entries.saturating_sub(before_entries));
			self.shared.release_entries(before_entries.saturating_sub(after_entries));
			if flow.is_some() {
				inner.flow = flow;
			}
			self.mark_pending(&mut inner);
		}
	}

	fn mark_pending(&self, inner: &mut SlotInner) {
		if inner.live.is_empty() {
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
			let out = mutate(&mut inner);
			let after = inner.live.bytes;
			let after_entries = inner.live.entry_count();
			self.shared.budget.charge(after.saturating_sub(before));
			self.shared.budget.release(before.saturating_sub(after));
			self.shared.charge_entries(after_entries.saturating_sub(before_entries));
			self.shared.release_entries(before_entries.saturating_sub(after_entries));
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
		clear_drop(&mut inner, marker);
		let after = inner.live.bytes;
		let after_entries = inner.live.entry_count();
		self.shared.budget.release(before.saturating_sub(after));
		self.shared.release_entries(before_entries.saturating_sub(after_entries));
		self.mark_pending(&mut inner);
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

	fn rebuild_in_flight(&self, global: &GlobalInner) -> FlushBatch {
		let mut batch = FlushBatch::default();
		for operator in &global.in_flight_operators {
			let Some(slot) = self.shared.slot(*operator) else {
				continue;
			};
			let inner = slot.inner.lock();
			let Some(pending) = inner.in_flight.as_ref() else {
				continue;
			};
			merge_into_batch(&mut batch, *operator, pending);
		}
		batch.checkpoints = global.in_flight_checkpoints.clone();
		batch.drops = global.in_flight_drops.clone();
		batch
	}

	fn pending_groups(&self) -> Vec<PendingGroup> {
		let mut by_flow: BTreeMap<Option<FlowId>, PendingGroup> = BTreeMap::new();
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let inner = slot.inner.lock();
			if inner.live.is_empty() {
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

	fn take_drain_slice(&self) -> Option<Arc<FlushBatch>> {
		let mut global = self.shared.global.lock();
		self.release_in_flight(&mut global);

		let mut batch = FlushBatch::default();
		let mut touched: Vec<OperatorId> = Vec::new();
		let mut exhausted = false;

		for group in self.pending_groups() {
			if batch.bytes >= self.shared.slice {
				exhausted = true;
				break;
			}
			for operator in group.operators {
				let Some(slot) = self.shared.slot(operator) else {
					continue;
				};
				let mut inner = slot.inner.lock();
				if inner.live.is_empty() {
					continue;
				}
				let taken = take_all(&mut inner);
				merge_into_batch(&mut batch, operator, &taken);
				inner.in_flight = Some(Arc::new(taken));
				inner.pending_seq = None;
				touched.push(operator);
			}
		}

		let blocked = self.flows_with_pending_live();
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

		if batch.state.is_empty()
			&& batch.join_expiries.is_empty()
			&& batch.checkpoints.is_empty()
			&& global.drops.is_empty()
		{
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

	fn flows_with_pending_live(&self) -> BTreeSet<FlowId> {
		let mut blocked = BTreeSet::new();
		for operator in self.shared.operators() {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			let inner = slot.inner.lock();
			if inner.live.is_empty() {
				continue;
			}
			if let Some(flow) = inner.flow {
				blocked.insert(flow);
			}
		}
		blocked
	}

	fn release_in_flight(&self, global: &mut GlobalInner) {
		let operators = mem::take(&mut global.in_flight_operators);
		for operator in operators {
			let Some(slot) = self.shared.slot(operator) else {
				continue;
			};
			slot.inner.lock().in_flight.take();
		}
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
		invalidate_flushed(sinks.point.as_ref(), sinks.range.as_ref(), batch);
	}

	fn settle(&self, batch: Arc<FlushBatch>) {
		{
			let mut global = self.shared.global.lock();
			for operator in global.in_flight_operators.clone() {
				let Some(slot) = self.shared.slot(operator) else {
					continue;
				};
				let mut inner = slot.inner.lock();
				let durable =
					inner.flow.and_then(|flow| batch.checkpoints.get(&flow).copied()).flatten();
				if let Some(version) = durable {
					inner.durable_position = Some(version);
				}
			}
			self.release_in_flight(&mut global);
			global.flushing = false;
		}
		self.shared.idle.notify_all();
		let entries = (batch.state.len() + batch.join_expiries.len()) as u64;
		self.shared.budget.release(batch.bytes);
		self.shared.release_entries(entries as usize);
		self.shared.triggered.store(false, Ordering::Release);

		{
			let mut metrics = self.shared.metrics.lock();
			metrics.slices += 1;
			metrics.persisted += entries;
			metrics.reclaimed += entries;
			metrics.evicted += batch.state.len() as u64;
			metrics.released = metrics.released.saturating_add(batch.bytes);
		}

		reifydb_assertions! {
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
		}
	}

	fn observe_write(&self) {
		if !self.shared.budget.over_budget() && !self.shared.over_entry_limit() {
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
		}
		| OperatorWrite::JoinExpiryInsert {
			operator,
			..
		}
		| OperatorWrite::JoinExpiryReplace {
			operator,
			..
		}
		| OperatorWrite::JoinExpiryRemove {
			operator,
			..
		} => *operator,
	}
}

fn drop_operator(marker: &DropMarker) -> OperatorId {
	match marker {
		DropMarker::OperatorState(operator)
		| DropMarker::JoinExpiriesOperator(operator)
		| DropMarker::JoinExpiriesGroup(operator, _) => *operator,
	}
}

pub(crate) fn record_state(inner: &mut SlotInner, key: EncodedKey, post: Option<EncodedPodRow>) {
	inner.live.record_state(key, post);
}

pub(crate) fn record_join_expiry(inner: &mut SlotInner, key: SlotJoinKey, expiry: Option<u64>, durable: bool) {
	inner.live.record_join_expiry(key, expiry, durable);
}

fn apply_write(inner: &mut SlotInner, write: &OperatorWrite) {
	match write {
		OperatorWrite::Insert {
			key,
			post,
			..
		} => record_state(inner, key.clone(), Some(post.clone())),
		OperatorWrite::Replace {
			key,
			post,
			..
		} => record_state(inner, key.clone(), Some(post.clone())),
		OperatorWrite::Remove {
			key,
			..
		} => record_state(inner, key.clone(), None),
		OperatorWrite::JoinExpiryInsert {
			group,
			side,
			row_num,
			at,
			..
		} => record_join_expiry(inner, (*group, *side, *row_num), Some(at.to_millis()), false),
		OperatorWrite::JoinExpiryReplace {
			group,
			side,
			row_num,
			at,
			..
		} => record_join_expiry(inner, (*group, *side, *row_num), Some(at.to_millis()), true),
		OperatorWrite::JoinExpiryRemove {
			group,
			side,
			row_num,
			pre,
			..
		} => record_join_expiry(inner, (*group, *side, *row_num), None, matches!(pre, DurablePre::Present(_))),
	}
}

fn clear_drop(inner: &mut SlotInner, marker: DropMarker) {
	let live = &mut inner.live;
	match marker {
		DropMarker::OperatorState(_) => {
			live.clear_state();
			live.retain_join_expiries(|_| false);
		}
		DropMarker::JoinExpiriesOperator(_) => {
			live.retain_join_expiries(|_| false);
		}
		DropMarker::JoinExpiriesGroup(_, group) => {
			live.retain_join_expiries(|(candidate, _, _)| *candidate != group);
		}
	}
}

fn take_all(inner: &mut SlotInner) -> OperatorLive {
	let taken = OperatorLive {
		operator: inner.live.operator,
		state: mem::take(&mut inner.live.state),
		join_expiries: mem::take(&mut inner.live.join_expiries),
		durable_join_expiries: BTreeSet::new(),
		bytes: inner.live.bytes,
	};
	for (key, entry) in taken.join_expiries.iter() {
		match entry {
			Some(_) => inner.live.durable_join_expiries.insert(*key),
			None => inner.live.durable_join_expiries.remove(key),
		};
	}
	inner.live.bytes = ByteSize::ZERO;
	taken
}

fn merge_into_batch(batch: &mut FlushBatch, operator: OperatorId, taken: &OperatorLive) {
	taken.state.for_each_entry(operator, |keyspace, group, suffix, entry| {
		batch.state.record_bytes(operator, keyspace, group, suffix, entry.post.clone());
	});
	for ((group, side, row_number), entry) in taken.join_expiries.iter() {
		batch.join_expiries.insert((operator, *group, *side, *row_number), *entry);
	}
	batch.bytes = batch.bytes.saturating_add(taken.bytes);
}

fn invalidate_flushed(point: Option<&PointTiers>, range: Option<&RangeTiers>, batch: &FlushBatch) {
	for marker in &batch.drops {
		match marker {
			DropMarker::OperatorState(operator) => {
				if let Some(range) = range {
					range.invalidate_operator(*operator);
				}
				if let Some(point) = point {
					point.invalidate_operator(*operator);
				}
			}
			DropMarker::JoinExpiriesOperator(_) | DropMarker::JoinExpiriesGroup(_, _) => {}
		}
	}
	for operator in batch.state.operators() {
		for (key, entry) in batch.state.encoded_entries(operator) {
			match &entry.post {
				Some(row) => {
					if let Some(range) = range {
						range.overwrite(operator, &key, row.clone());
					}
					if let Some(point) = point {
						point.overwrite(operator, &key, row.clone());
					}
				}
				None => {
					if let Some(range) = range {
						range.retract(operator, &key);
					}
					if let Some(point) = point {
						point.invalidate(operator, &key);
					}
				}
			}
		}
	}
}
