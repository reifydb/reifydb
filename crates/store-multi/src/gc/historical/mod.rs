// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod actor;

use reifydb_core::common::CommitVersion;

pub trait QueryWatermark: Send + Sync + 'static {
	fn effective_gc_cutoff(&self) -> CommitVersion;
}

#[derive(Debug, Default)]
pub struct GcMetrics {
	pub shapes_scanned: u64,
	pub versions_dropped: u64,
}
