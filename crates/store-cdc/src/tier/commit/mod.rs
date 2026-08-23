// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batch;
mod buffer;
mod read;
mod sealed;
mod write;

use std::sync::{
	Arc, OnceLock,
	atomic::{AtomicU64, Ordering},
};

use reifydb_runtime::{
	actor::mailbox::ActorRef,
	sync::{condvar::Condvar, mutex::Mutex},
};
use reifydb_value::{byte_size::ByteSize, count::Count};
use tracing::instrument;

use crate::{flush::actor::FlushMessage, tier::commit::buffer::BufferInner};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CdcCommitMetrics {
	pub resident_bytes: ByteSize,
	pub entries: Count,
	pub blocks_cut: u64,
	pub stalls: u64,
}

struct CdcCommitBufferTierInner {
	inner: Mutex<BufferInner>,
	idle: Condvar,
	flush: Mutex<()>,
	flusher: OnceLock<ActorRef<FlushMessage>>,
	cut_bytes: ByteSize,
	ceiling: ByteSize,
	blocks_cut: AtomicU64,
	stalls: AtomicU64,
}

impl CdcCommitBufferTierInner {
	fn new(cut_bytes: ByteSize, ceiling: ByteSize) -> Self {
		Self {
			inner: Default::default(),
			idle: Default::default(),
			flush: Default::default(),
			flusher: OnceLock::new(),
			cut_bytes,
			ceiling,
			blocks_cut: AtomicU64::new(0),
			stalls: AtomicU64::new(0),
		}
	}
}

#[derive(Clone)]
pub struct CdcCommitBufferTier {
	shared: Arc<CdcCommitBufferTierInner>,
}

impl CdcCommitBufferTier {
	#[instrument(name = "store::cdc::commit::new", level = "debug", skip_all, fields(cut_bytes = cut_bytes.as_bytes(), ceiling = ceiling.as_bytes()))]
	pub fn new(cut_bytes: ByteSize, ceiling: ByteSize) -> Self {
		Self {
			shared: Arc::new(CdcCommitBufferTierInner::new(cut_bytes, ceiling)),
		}
	}

	#[instrument(name = "store::cdc::commit::attach_flusher", level = "debug", skip_all)]
	pub fn attach_flusher(&self, flusher: ActorRef<FlushMessage>) {
		let _ = self.shared.flusher.set(flusher);
	}

	#[instrument(name = "store::cdc::commit::resident_bytes", level = "trace", skip(self))]
	pub fn resident_bytes(&self) -> ByteSize {
		self.shared.inner.lock().resident_bytes()
	}

	#[instrument(name = "store::cdc::commit::metrics", level = "trace", skip(self))]
	pub fn metrics(&self) -> CdcCommitMetrics {
		let inner = self.shared.inner.lock();
		CdcCommitMetrics {
			resident_bytes: inner.resident_bytes(),
			entries: Count::new(inner.entries() as u64),
			blocks_cut: self.shared.blocks_cut.load(Ordering::Relaxed),
			stalls: self.shared.stalls.load(Ordering::Relaxed),
		}
	}
}
