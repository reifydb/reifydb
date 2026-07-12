// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod actor;
pub mod scanner;

use std::sync::Arc;

use reifydb_core::interface::catalog::{config::GetConfig, ringbuffer::RingBuffer};

pub trait ListRingBuffers: Clone + Send + Sync + 'static {
	fn list_ringbuffers(&self) -> Vec<RingBuffer>;
	fn config(&self) -> Arc<dyn GetConfig>;
}

#[derive(Debug, Default)]
pub struct ReconcileStats {
	pub ringbuffers_scanned: u64,

	pub partitions_checked: u64,

	pub partitions_removed: u64,

	pub partitions_corrected: u64,
}
