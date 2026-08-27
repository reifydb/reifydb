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
	iter,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use reifydb_runtime::{
	actor::mailbox::ActorRef,
	sync::{
		condvar::Condvar,
		mutex::{Mutex, MutexGuard},
		waiter::WaiterHandle,
	},
};
use reifydb_value::byte_size::ByteSize;

use crate::{
	flush::FlushMessage,
	tier::commit::batch::{DropMarker, FlushBatch},
	types::{DurablePre, OperatorWrite},
};

pub const FLUSH_BUDGET_BYTES: ByteSize = ByteSize::from_mib(4);

#[derive(Debug, Default)]
struct BufferInner {
	live: FlushBatch,
	in_flight: Option<Arc<FlushBatch>>,
	flushing: bool,
	triggered: bool,
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

#[derive(Debug)]
struct Shared {
	inner: Mutex<BufferInner>,
	idle: Condvar,
	flush: Mutex<()>,
	flusher: OnceLock<ActorRef<FlushMessage>>,
	budget: AtomicU64,
}

impl Default for Shared {
	fn default() -> Self {
		Self::with_budget(FLUSH_BUDGET_BYTES)
	}
}

impl Shared {
	fn with_budget(budget: ByteSize) -> Self {
		Self {
			inner: Default::default(),
			idle: Default::default(),
			flush: Default::default(),
			flusher: OnceLock::new(),
			budget: AtomicU64::new(budget.as_bytes()),
		}
	}

	fn budget(&self) -> ByteSize {
		ByteSize::from_bytes(self.budget.load(Ordering::Relaxed))
	}
}

#[derive(Debug, Clone, Default)]
pub struct OperatorCommitBuffer {
	shared: Arc<Shared>,
}

impl OperatorCommitBuffer {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_budget(budget: ByteSize) -> Self {
		Self {
			shared: Arc::new(Shared::with_budget(budget)),
		}
	}

	pub fn attach_flusher(&self, flusher: ActorRef<FlushMessage>) {
		let _ = self.shared.flusher.set(flusher);
	}

	pub fn budget(&self) -> ByteSize {
		self.shared.budget()
	}

	pub fn set_budget(&self, budget: ByteSize) {
		self.shared.budget.store(budget.as_bytes(), Ordering::Relaxed);
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		let mut inner = self.shared.inner.lock();
		record_writes(&mut inner.live, writes);
		self.request_flush_when_full(&mut inner);
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
		let mut inner = self.shared.inner.lock();
		record_writes(&mut inner.live, writes);
		for (flow, version) in checkpoints {
			inner.live.checkpoints.insert(*flow, Some(*version));
		}
		for flow in checkpoint_deletes {
			inner.live.checkpoints.insert(*flow, None);
		}
		self.request_flush_when_full(&mut inner);
	}

	pub fn record_drop(&self, marker: DropMarker) {
		let mut inner = self.shared.inner.lock();
		while inner.flushing {
			self.shared.idle.wait(&mut inner);
		}
		inner.live.clear_drop(marker);
		inner.live.drops.push(marker);
	}

	pub fn flush_guard(&self) -> MutexGuard<'_, ()> {
		self.shared.flush.lock()
	}

	pub fn take_for_flush(&self) -> Option<Arc<FlushBatch>> {
		let mut inner = self.shared.inner.lock();
		if inner.live.is_empty() {
			return None;
		}
		let batch = Arc::new(inner.live.split_within(self.shared.budget()));
		inner.in_flight = Some(Arc::clone(&batch));
		inner.flushing = true;
		Some(batch)
	}

	pub fn complete_flush(&self) {
		let mut inner = self.shared.inner.lock();
		inner.in_flight = None;
		inner.flushing = false;
		inner.triggered = false;
		drop(inner);
		self.shared.idle.notify_all();
	}

	fn request_flush_when_full(&self, inner: &mut BufferInner) {
		if inner.triggered || inner.flushing || inner.resident_bytes() < self.shared.budget() {
			return;
		}
		let Some(flusher) = self.shared.flusher.get() else {
			return;
		};
		inner.triggered = true;
		let sent = flusher.send(FlushMessage::FlushPending {
			waiter: Arc::new(WaiterHandle::new()),
		});
		if sent.is_err() {
			inner.triggered = false;
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
