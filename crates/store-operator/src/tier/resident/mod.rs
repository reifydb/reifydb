// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batch;
pub mod flush;
pub mod state_map;

mod census;
mod checkpoint;
mod join_expiry;
mod state;

#[cfg(test)]
mod tests;

use std::{
	fmt,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
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
		point::OperatorPointTier,
		range::OperatorRangeTier,
		resident::{
			batch::{DropMarker, FlushBatch, JoinExpiryKey, StateKey},
			census::{BufferCensus, release_in_flight},
			flush::actor::FlushMessage,
		},
	},
	types::{DurablePre, OperatorWrite},
};

pub const FLUSH_BUDGET_BYTES: ByteSize = ByteSize::from_mib(100);

pub const SLICE_BYTES: ByteSize = ByteSize::from_mib(4);
const EVICT_HEADROOM: ByteSize = ByteSize::from_kib(256);

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
struct BufferInner {
	live: FlushBatch,
	in_flight: Option<Arc<FlushBatch>>,
	flushing: bool,
	cursor: Option<StateKey>,
	census: BufferCensus,
}

impl BufferInner {
	fn resident_bytes(&self) -> ByteSize {
		self.live.bytes.saturating_add(self.in_flight.as_ref().map_or(ByteSize::ZERO, |batch| batch.bytes))
	}

	fn any_drop(&self, predicate: impl Fn(&DropMarker) -> bool) -> bool {
		self.live.drops.iter().any(&predicate)
			|| self.in_flight.as_ref().is_some_and(|batch| batch.drops.iter().any(&predicate))
	}
}

struct OperatorSinks {
	persistent: OperatorPersistentTier,
	point: Option<OperatorPointTier>,
	range: Option<OperatorRangeTier>,
}

pub struct Shared {
	inner: Mutex<BufferInner>,
	idle: Condvar,
	drain: Mutex<()>,
	sinks: OnceLock<OperatorSinks>,
	budget: Arc<MemoryBudget>,
	slice: ByteSize,
	waker: Mutex<Option<ActorRef<FlushMessage>>>,
	metrics: Mutex<OperatorResidentStateMetrics>,
	triggered: AtomicBool,
}

impl Shared {
	fn new(cap: ByteSize) -> Self {
		Self {
			inner: Mutex::new(BufferInner::default()),
			idle: Condvar::new(),
			drain: Mutex::new(()),
			sinks: OnceLock::new(),
			budget: Arc::new(MemoryBudget::new(cap)),
			slice: cap.min(SLICE_BYTES),
			waker: Mutex::new(None),
			metrics: Mutex::new(OperatorResidentStateMetrics::default()),
			triggered: AtomicBool::new(false),
		}
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
		Self::with_budget(FLUSH_BUDGET_BYTES)
	}
}

impl OperatorResidentState {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_budget(budget: ByteSize) -> Self {
		Self {
			shared: Arc::new(Shared::new(budget)),
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
		point: Option<OperatorPointTier>,
		range: Option<OperatorRangeTier>,
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
		metrics.backlog = self.shared.inner.lock().resident_bytes();
		metrics
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		self.write(|write| record_writes(write, writes));
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
		self.write(|write| {
			record_writes(write, writes);
			for (flow, version) in checkpoints {
				write.record_checkpoint(*flow, Some(*version));
			}
			for flow in checkpoint_deletes {
				write.record_checkpoint(*flow, None);
			}
		});
	}

	pub fn record_drop(&self, marker: DropMarker) {
		let shared = self.shared();
		let mut inner = shared.inner.lock();
		while inner.flushing {
			shared.idle.wait(&mut inner);
		}
		let BufferInner {
			live,
			census,
			..
		} = &mut *inner;
		let before = live.bytes;
		live.clear_drop(marker, census);
		live.drops.push(marker);
		let after = live.bytes;
		shared.budget.release(before.saturating_sub(after));
	}

	pub fn flush_guard(&self) -> MutexGuard<'_, ()> {
		self.shared.drain.lock()
	}

	pub fn take_for_flush(&self) -> Option<Arc<FlushBatch>> {
		self.take_drain_slice()
	}

	pub fn complete_flush(&self) {
		let Some(batch) = self.shared.inner.lock().in_flight.clone() else {
			return;
		};
		self.settle(batch);
	}

	pub fn flush_all(&self) {
		let _guard = self.flush_guard();
		while let Some(batch) = self.take_drain_slice() {
			self.persist(&batch);
			self.settle(batch);
		}
	}

	pub fn evict_under_cap(&self) {
		let _guard = self.flush_guard();
		while self.shared.budget.over_budget() {
			let Some(batch) = self.take_evict_slice() else {
				break;
			};
			self.persist(&batch);
			self.settle(batch);
		}
	}

	fn take_drain_slice(&self) -> Option<Arc<FlushBatch>> {
		let mut inner = self.shared.inner.lock();
		if inner.live.is_empty() {
			return None;
		}
		release_in_flight(&mut inner);
		let batch = Arc::new(inner.live.drain_within(self.shared.slice));
		if !inner.live.is_empty() {
			self.shared.metrics.lock().budget_exhausted += 1;
		}
		inner.in_flight = Some(Arc::clone(&batch));
		inner.flushing = true;
		Some(batch)
	}

	fn evict_budget(&self) -> ByteSize {
		let over = self.shared.budget.used().saturating_sub(self.shared.budget.limit());
		over.saturating_add(EVICT_HEADROOM).min(self.shared.slice)
	}

	fn take_evict_slice(&self) -> Option<Arc<FlushBatch>> {
		let mut inner = self.shared.inner.lock();
		if inner.live.is_empty() {
			return None;
		}
		release_in_flight(&mut inner);
		let cursor = inner.cursor.take();
		let (taken, cursor) = inner.live.evict_within(self.evict_budget(), cursor);
		inner.cursor = cursor;
		if taken.is_empty() {
			return None;
		}
		let batch = Arc::new(taken);
		inner.in_flight = Some(Arc::clone(&batch));
		inner.flushing = true;
		Some(batch)
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
			let mut inner = self.shared.inner.lock();
			release_in_flight(&mut inner);
			inner.flushing = false;
		}
		self.shared.idle.notify_all();
		self.shared.budget.release(batch.bytes);
		self.shared.triggered.store(false, Ordering::Release);

		let entries = (batch.state.len() + batch.join_expiries.len()) as u64;
		{
			let mut metrics = self.shared.metrics.lock();
			metrics.slices += 1;
			metrics.persisted += entries;
			metrics.reclaimed += entries;
			metrics.evicted += batch.state.len() as u64;
			metrics.released = metrics.released.saturating_add(batch.bytes);
		}

		reifydb_assertions! {
			let inner = self.shared.inner.lock();
			let mut walked = walk(&inner.live);
			if let Some(pending) = inner.in_flight.as_ref() {
				walked = walked.saturating_add(walk(pending));
			}
			let counted = self.shared.budget.used();
			assert_eq!(
				counted, walked,
				"store::operator::resident resident state byte counter drifted: the budget carries {}, the resident set walks to {}",
				counted, walked
			);
		}
	}

	fn observe_write(&self) {
		if !self.shared.budget.over_budget() {
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

	pub(crate) fn write<R>(&self, mutate: impl FnOnce(&mut BufferWrite<'_>) -> R) -> R {
		let out = {
			let shared = self.shared();
			let mut inner = shared.inner.lock();
			let before = inner.live.bytes;
			let out = {
				let BufferInner {
					live,
					in_flight,
					census,
					..
				} = &mut *inner;
				mutate(&mut BufferWrite {
					live,
					in_flight: in_flight.as_deref(),
					census,
				})
			};
			let after = inner.live.bytes;
			shared.budget.charge(after.saturating_sub(before));
			shared.budget.release(before.saturating_sub(after));
			out
		};
		self.observe_write();
		out
	}
}

pub(crate) struct BufferWrite<'a> {
	live: &'a mut FlushBatch,
	in_flight: Option<&'a FlushBatch>,
	census: &'a mut BufferCensus,
}

impl BufferWrite<'_> {
	pub(crate) fn record_state(&mut self, key: StateKey, post: Option<EncodedPodRow>, durable_pre: DurablePre) {
		let previous = merged_state(self.live, self.in_flight, key.0, &key.1);
		let incoming = value_bytes(post.as_ref());
		if let Some(bytes) = previous {
			self.census.retract_state(key.0, &key.1, bytes);
		}
		if let Some(bytes) = incoming {
			self.census.admit_state(key.0, &key.1, bytes);
		}
		self.live.record_state(key, post, durable_pre);
	}

	pub(crate) fn record_join_expiry(&mut self, key: JoinExpiryKey, expiry: Option<u64>, durable: bool) {
		let before = merged_join_expiry(self.live, self.in_flight, &key);
		self.live.record_join_expiry(key, expiry, durable);
		let after = merged_join_expiry(self.live, self.in_flight, &key);
		match (before, after) {
			(false, true) => self.census.admit_join_expiry(key.0),
			(true, false) => self.census.retract_join_expiry(key.0),
			_ => {}
		}
	}

	pub(crate) fn record_checkpoint(&mut self, flow: FlowId, version: Option<CommitVersion>) {
		self.live.checkpoints.insert(flow, version);
	}
}

fn value_bytes(post: Option<&EncodedPodRow>) -> Option<u64> {
	post.map(|row| row.bytes().len() as u64)
}

fn merged_state(
	live: &FlushBatch,
	in_flight: Option<&FlushBatch>,
	operator: OperatorId,
	key: &EncodedKey,
) -> Option<u64> {
	let entry = match live.state.lookup(operator, key) {
		Some(entry) => entry,
		None => in_flight?.state.lookup(operator, key)?,
	};
	value_bytes(entry.post.as_ref())
}

fn merged_join_expiry(live: &FlushBatch, in_flight: Option<&FlushBatch>, key: &JoinExpiryKey) -> bool {
	match live.join_expiries.get(key) {
		Some(entry) => entry.is_some(),
		None => in_flight.and_then(|batch| batch.join_expiries.get(key)).is_some_and(|entry| entry.is_some()),
	}
}

#[cfg(reifydb_assertions)]
use crate::tier::resident::batch::{JOIN_EXPIRY_ENTRY_BYTES, state_entry_bytes};

#[cfg(reifydb_assertions)]
fn walk(batch: &FlushBatch) -> ByteSize {
	let mut total = ByteSize::ZERO;
	for ((_, key), entry) in &batch.state {
		total = total.saturating_add(state_entry_bytes(key, entry));
	}
	total.saturating_add(JOIN_EXPIRY_ENTRY_BYTES * batch.join_expiries.len() as u64)
}

fn invalidate_flushed(point: Option<&OperatorPointTier>, range: Option<&OperatorRangeTier>, batch: &FlushBatch) {
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
	for ((operator, key), entry) in &batch.state {
		match &entry.post {
			Some(row) => {
				if let Some(range) = range {
					range.overwrite(operator, key.clone(), row.clone());
				}
				if let Some(point) = point {
					point.overwrite(operator, key.clone(), row.clone());
				}
			}
			None => {
				if let Some(range) = range {
					range.retract(operator, key);
				}
				if let Some(point) = point {
					point.invalidate(operator, key);
				}
			}
		}
	}
}

fn record_writes(live: &mut BufferWrite<'_>, writes: &[OperatorWrite]) {
	for write in writes {
		match write {
			OperatorWrite::Insert {
				operator,
				key,
				post,
			} => {
				live.record_state((*operator, key.clone()), Some(post.clone()), DurablePre::Absent);
			}
			OperatorWrite::Replace {
				operator,
				key,
				pre_value_bytes,
				post,
			} => {
				live.record_state(
					(*operator, key.clone()),
					Some(post.clone()),
					DurablePre::Present(*pre_value_bytes),
				);
			}
			OperatorWrite::Remove {
				operator,
				key,
				pre,
			} => {
				live.record_state((*operator, key.clone()), None, *pre);
			}
			OperatorWrite::JoinExpiryInsert {
				operator,
				group,
				side,
				row_num: row_number,
				at,
			} => {
				live.record_join_expiry(
					(*operator, *group, *side, *row_number),
					Some(at.to_millis()),
					false,
				);
			}
			OperatorWrite::JoinExpiryReplace {
				operator,
				group,
				side,
				row_num: row_number,
				at,
			} => {
				live.record_join_expiry(
					(*operator, *group, *side, *row_number),
					Some(at.to_millis()),
					true,
				);
			}
			OperatorWrite::JoinExpiryRemove {
				operator,
				group,
				side,
				row_num: row_number,
				pre,
			} => {
				live.record_join_expiry(
					(*operator, *group, *side, *row_number),
					None,
					matches!(pre, DurablePre::Present(_)),
				);
			}
		}
	}
}
