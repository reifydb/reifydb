// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, atomic::Ordering};

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use reifydb_runtime::sync::{mutex::MutexGuard, waiter::WaiterHandle};
use tracing::instrument;

use crate::{
	flush::actor::FlushMessage,
	tier::commit::{CdcCommitBufferTier, batch::FlushBatch, buffer::BufferInner},
};

impl CdcCommitBufferTier {
	#[instrument(name = "store::cdc::commit::append", level = "trace", skip(self, cdc), fields(version = cdc.version.0))]
	pub fn append(&self, cdc: Arc<Cdc>) -> bool {
		let mut inner = self.shared.inner.lock();
		if !inner.accepts(cdc.version) {
			return false;
		}
		if !self.stall_above_ceiling(&mut inner, cdc.version) {
			return false;
		}
		inner.append(cdc);
		self.request_flush_when_full(&mut inner);
		true
	}

	#[instrument(name = "store::cdc::commit::seal_floor", level = "debug", skip(self), fields(version = version.0))]
	pub fn seal_floor(&self, version: CommitVersion) {
		self.shared.inner.lock().sealed.insert(CommitVersion(0), version);
	}

	#[instrument(name = "store::cdc::commit::take_for_flush", level = "debug", skip(self))]
	pub fn take_for_flush(&self) -> Option<Arc<FlushBatch>> {
		let mut inner = self.shared.inner.lock();
		if inner.live.is_empty() {
			return None;
		}
		let batch = Arc::new(inner.cut_within(self.shared.cut_bytes));
		inner.in_flight = Some(Arc::clone(&batch));
		inner.flushing = true;
		Some(batch)
	}

	#[instrument(name = "store::cdc::commit::complete_flush", level = "debug", skip(self))]
	pub fn complete_flush(&self) {
		let mut inner = self.shared.inner.lock();
		let cut = inner.in_flight.take().is_some();
		inner.flushing = false;
		inner.triggered = false;
		drop(inner);
		if cut {
			self.shared.blocks_cut.fetch_add(1, Ordering::Relaxed);
		}
		self.shared.idle.notify_all();
	}

	#[instrument(name = "store::cdc::commit::flush_acquire", level = "debug", skip_all)]
	pub fn flush_guard(&self) -> MutexGuard<'_, ()> {
		self.shared.flush.lock()
	}

	#[instrument(name = "store::cdc::commit::stall_above_ceiling", level = "debug", skip_all)]

	fn stall_above_ceiling(&self, inner: &mut MutexGuard<'_, BufferInner>, version: CommitVersion) -> bool {
		while inner.resident_bytes() > self.shared.ceiling && self.shared.flusher.get().is_some() {
			self.request_flush(inner);
			self.shared.stalls.fetch_add(1, Ordering::Relaxed);
			self.shared.idle.wait(inner);
			if !inner.accepts(version) {
				return false;
			}
		}
		true
	}

	fn request_flush_when_full(&self, inner: &mut BufferInner) {
		if inner.live_bytes < self.shared.cut_bytes {
			return;
		}
		self.request_flush(inner);
	}

	#[instrument(name = "store::cdc::commit::request_flush", level = "debug", skip_all)]
	fn request_flush(&self, inner: &mut BufferInner) {
		if inner.triggered || inner.flushing || inner.live.is_empty() {
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
