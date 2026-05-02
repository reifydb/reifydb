// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod actor;

use reifydb_core::common::CommitVersion;

pub trait QueryWatermark: Send + Sync + 'static {
	fn query_done_until(&self) -> CommitVersion;
}

#[derive(Debug, Default)]
pub struct GcStats {
	pub shapes_scanned: u64,
	pub versions_dropped: u64,
}
