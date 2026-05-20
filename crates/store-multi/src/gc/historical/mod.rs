// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod actor;

use reifydb_core::common::CommitVersion;

pub trait QueryWatermark: Send + Sync + 'static {
	fn effective_gc_cutoff(&self) -> CommitVersion;
}

#[derive(Debug, Default)]
pub struct GcStats {
	pub shapes_scanned: u64,
	pub versions_dropped: u64,
}
