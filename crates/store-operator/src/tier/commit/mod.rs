// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batch;

mod anchor;
mod census;
mod checkpoint;
mod state;

#[cfg(test)]
mod tests;

use std::{iter, mem, sync::Arc};

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use reifydb_runtime::sync::{
	condvar::Condvar,
	mutex::{Mutex, MutexGuard},
};

use crate::{
	tier::commit::batch::{DropMarker, FlushBatch},
	types::OperatorWrite,
};

#[derive(Debug, Default)]
struct BufferInner {
	live: FlushBatch,
	in_flight: Option<Arc<FlushBatch>>,
	flushing: bool,
}

impl BufferInner {
	fn any_drop(&self, predicate: impl Fn(&DropMarker) -> bool) -> bool {
		self.live.drops.iter().any(&predicate)
			|| self.in_flight.as_ref().is_some_and(|batch| batch.drops.iter().any(&predicate))
	}
}

#[derive(Debug, Default)]
struct Shared {
	inner: Mutex<BufferInner>,
	idle: Condvar,
	flush: Mutex<()>,
}

#[derive(Debug, Clone, Default)]
pub struct OperatorCommitBuffer {
	shared: Arc<Shared>,
}

impl OperatorCommitBuffer {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		record_writes(&mut self.shared.inner.lock().live, writes);
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
		let batch = Arc::new(mem::take(&mut inner.live));
		inner.in_flight = Some(Arc::clone(&batch));
		inner.flushing = true;
		Some(batch)
	}

	pub fn complete_flush(&self) {
		let mut inner = self.shared.inner.lock();
		inner.in_flight = None;
		inner.flushing = false;
		drop(inner);
		self.shared.idle.notify_all();
	}
}

fn resident(inner: &BufferInner) -> impl Iterator<Item = &FlushBatch> {
	inner.in_flight.as_deref().into_iter().chain(iter::once(&inner.live))
}

fn record_writes(live: &mut FlushBatch, writes: &[OperatorWrite]) {
	for write in writes {
		match write {
			OperatorWrite::Set {
				operator,
				key,
				row,
			} => {
				live.state.insert((*operator, key.clone()), Some(row.clone()));
			}
			OperatorWrite::Remove {
				operator,
				key,
			} => {
				live.state.insert((*operator, key.clone()), None);
			}
			OperatorWrite::AnchorSet {
				operator,
				group,
				side,
				row_num: row_number,
				expiry,
			} => {
				live.anchors.insert((*operator, *group, *side, *row_number), Some(expiry.to_millis()));
			}
			OperatorWrite::AnchorRemove {
				operator,
				group,
				side,
				run_num: row_number,
			} => {
				live.anchors.insert((*operator, *group, *side, *row_number), None);
			}
		}
	}
}
