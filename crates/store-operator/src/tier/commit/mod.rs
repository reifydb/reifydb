// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batch;

mod anchor;
mod census;
mod checkpoint;
mod state;

#[cfg(test)]
mod tests;

use std::{
	borrow::Cow,
	iter,
	sync::{Arc, OnceLock},
};

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId, util::budget::MemoryBudget};
use reifydb_runtime::{
	actor::mailbox::ActorRef,
	sync::{
		condvar::Condvar,
		mutex::{Mutex, MutexGuard},
		waiter::WaiterHandle,
	},
};
use reifydb_store::tier::commit::{
	CommitCensus, CommitConfig, CommitDomain, CommitTier, CommitWaker, Settlement, Slice,
};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

use crate::{
	flush::FlushMessage,
	tier::{
		commit::batch::{ANCHOR_ENTRY_BYTES, DropMarker, FlushBatch, state_entry_bytes},
		persistent::OperatorPersistentTier,
		point::OperatorPointTier,
		range::OperatorRangeTier,
	},
	types::{DurablePre, OperatorWrite},
};

pub const FLUSH_BUDGET_BYTES: ByteSize = ByteSize::from_mib(4);

const TICK_INTERVAL: Duration = Duration::from_seconds_const(5);

pub type OperatorCommitTier = CommitTier<OperatorCommitDomain>;

#[derive(Debug, Default)]
struct BufferInner {
	live: FlushBatch,
	in_flight: Option<Arc<FlushBatch>>,
	flushing: bool,
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
}

impl Shared {
	fn new(budget: Arc<MemoryBudget>) -> Self {
		Self {
			inner: Mutex::new(BufferInner::default()),
			idle: Condvar::new(),
			drain: Mutex::new(()),
			sinks: OnceLock::new(),
			budget,
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorCommitDomain;

impl CommitDomain for OperatorCommitDomain {
	type State = Shared;
	type Batch = Arc<FlushBatch>;
	type Ack = ();
	type Cutoff = ();
	type Kind = ();

	const SCOPE: &'static str = "store::operator::commit";

	const MAX_SLICES_PER_TICK: usize = usize::MAX;

	fn cutoff(_state: &Self::State) -> Option<Self::Cutoff> {
		Some(())
	}

	fn cutoff_all() -> Self::Cutoff {}

	fn kinds(_state: &Self::State) -> Vec<Self::Kind> {
		vec![()]
	}

	fn select(
		state: &Self::State,
		_kind: Self::Kind,
		_cutoff: Self::Cutoff,
		budget: ByteSize,
	) -> Option<Slice<Self>> {
		let mut inner = state.inner.lock();
		if inner.live.is_empty() {
			return None;
		}
		let batch = Arc::new(inner.live.split_within(budget));
		let more = !inner.live.is_empty();
		inner.in_flight = Some(Arc::clone(&batch));
		inner.flushing = true;
		Some(Slice {
			bytes: batch.bytes,
			batch,
			more,
		})
	}

	fn persist(state: &Self::State, batch: &Self::Batch) -> reifydb_value::Result<Self::Ack> {
		let sinks = state.sinks.get().expect("the operator commit tier flushed before its sinks were attached");
		sinks.persistent.flush_batch(batch);
		invalidate_flushed(sinks.point.as_ref(), sinks.range.as_ref(), batch);
		Ok(())
	}

	fn settle(state: &Self::State, batch: Self::Batch, _ack: Self::Ack) -> Settlement {
		{
			let mut inner = state.inner.lock();
			inner.in_flight = None;
			inner.flushing = false;
		}
		state.idle.notify_all();
		let entries = (batch.state.len() + batch.anchors.len()) as u64;
		Settlement {
			released: batch.bytes,
			entries,
			reclaimed: entries,
		}
	}

	fn resident_bytes(state: &Self::State) -> ByteSize {
		state.inner.lock().resident_bytes()
	}

	fn kind_name(_kind: Self::Kind) -> Cow<'static, str> {
		Cow::Borrowed("all")
	}

	fn census(state: &Self::State) -> CommitCensus {
		let inner = state.inner.lock();
		let mut walked = walk(&inner.live);
		if let Some(batch) = inner.in_flight.as_ref() {
			walked = walked.saturating_add(walk(batch));
		}
		CommitCensus {
			counted: state.budget.used(),
			walked,
		}
	}
}

fn walk(batch: &FlushBatch) -> ByteSize {
	let mut total = ByteSize::ZERO;
	for (key, entry) in &batch.state {
		total = total.saturating_add(state_entry_bytes(key, entry));
	}
	total.saturating_add(ANCHOR_ENTRY_BYTES * batch.anchors.len() as u64)
}

struct FlushWaker(ActorRef<FlushMessage>);

impl CommitWaker for FlushWaker {
	fn wake(&self) {
		let _ = self.0.send(FlushMessage::FlushPending {
			waiter: Arc::new(WaiterHandle::new()),
		});
	}
}

#[derive(Clone)]
pub struct OperatorCommitBuffer {
	tier: OperatorCommitTier,
}

impl std::fmt::Debug for OperatorCommitBuffer {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("OperatorCommitBuffer").field("budget", &self.budget()).finish()
	}
}

impl Default for OperatorCommitBuffer {
	fn default() -> Self {
		Self::with_budget(FLUSH_BUDGET_BYTES)
	}
}

impl OperatorCommitBuffer {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_budget(budget: ByteSize) -> Self {
		let tier = CommitTier::new(
			CommitConfig {
				budget: Some(budget),
				interval: TICK_INTERVAL,
			},
			Shared::new,
		)
		.expect("the operator commit tier is always built with a budget");
		Self {
			tier,
		}
	}

	pub(crate) fn shared(&self) -> &Shared {
		self.tier.state()
	}

	pub fn attach_flusher(&self, flusher: ActorRef<FlushMessage>) {
		self.tier.attach_waker(Arc::new(FlushWaker(flusher)));
	}

	pub fn attach_sinks(
		&self,
		persistent: OperatorPersistentTier,
		point: Option<OperatorPointTier>,
		range: Option<OperatorRangeTier>,
	) {
		let _ = self.shared().sinks.set(OperatorSinks {
			persistent,
			point,
			range,
		});
	}

	pub fn budget(&self) -> ByteSize {
		self.tier.budget().limit()
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		self.write(|live| record_writes(live, writes));
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
		self.write(|live| {
			record_writes(live, writes);
			for (flow, version) in checkpoints {
				live.checkpoints.insert(*flow, Some(*version));
			}
			for flow in checkpoint_deletes {
				live.checkpoints.insert(*flow, None);
			}
		});
	}

	pub fn record_drop(&self, marker: DropMarker) {
		let shared = self.shared();
		let mut inner = shared.inner.lock();
		while inner.flushing {
			shared.idle.wait(&mut inner);
		}
		let before = inner.live.bytes;
		inner.live.clear_drop(marker);
		inner.live.drops.push(marker);
		shared.budget.release(before.saturating_sub(inner.live.bytes));
	}

	pub fn flush_guard(&self) -> MutexGuard<'_, ()> {
		self.shared().drain.lock()
	}

	pub fn take_for_flush(&self) -> Option<Arc<FlushBatch>> {
		self.tier.take((), (), self.budget()).map(|slice| slice.batch)
	}

	pub fn complete_flush(&self) {
		let Some(batch) = self.shared().inner.lock().in_flight.clone() else {
			return;
		};
		self.tier.settle(batch, ());
	}

	/// Drains every slice under one guard, so a caller holding it waits out a whole drain rather than one
	/// slice; a shutdown that returned mid-drain would close the connection under the running flusher.
	pub fn flush_all(&self) {
		let _guard = self.flush_guard();
		self.tier.flush_all();
	}

	pub(crate) fn write<R>(&self, mutate: impl FnOnce(&mut FlushBatch) -> R) -> R {
		let out = {
			let shared = self.shared();
			let mut inner = shared.inner.lock();
			let before = inner.live.bytes;
			let out = mutate(&mut inner.live);
			let after = inner.live.bytes;
			shared.budget.charge(after.saturating_sub(before));
			shared.budget.release(before.saturating_sub(after));
			out
		};
		self.tier.observe_write();
		out
	}
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
			DropMarker::AnchorsOperator(_) | DropMarker::AnchorsGroup(_, _) => {}
		}
	}
	for ((operator, key), entry) in &batch.state {
		match &entry.post {
			Some(row) => {
				if let Some(range) = range {
					range.overwrite(*operator, key.clone(), row.clone());
				}
				if let Some(point) = point {
					point.overwrite(*operator, key.clone(), row.clone());
				}
			}
			None => {
				if let Some(range) = range {
					range.retract(*operator, key);
				}
				if let Some(point) = point {
					point.invalidate(*operator, key);
				}
			}
		}
	}
}

fn resident(inner: &BufferInner) -> impl Iterator<Item = &FlushBatch> {
	inner.in_flight.as_deref().into_iter().chain(iter::once(&inner.live))
}

fn record_writes(live: &mut FlushBatch, writes: &[OperatorWrite]) {
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
			OperatorWrite::AnchorInsert {
				operator,
				group,
				side,
				row_num: row_number,
				expiry,
			}
			| OperatorWrite::AnchorReplace {
				operator,
				group,
				side,
				row_num: row_number,
				expiry,
			} => {
				live.record_anchor((*operator, *group, *side, *row_number), Some(expiry.to_millis()));
			}
			OperatorWrite::AnchorRemove {
				operator,
				group,
				side,
				row_num: row_number,
			} => {
				live.record_anchor((*operator, *group, *side, *row_number), None);
			}
		}
	}
}
