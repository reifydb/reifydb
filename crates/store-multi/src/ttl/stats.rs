// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::shape::ShapeId;

/// Statistics from a single GC scan cycle.
#[derive(Debug, Default)]
pub(crate) struct GcScanStats {
	/// Number of shapes scanned for expired rows.
	pub shapes_scanned: u64,
	/// Number of shapes skipped (e.g. CleanupMode::Delete not supported in V1).
	pub shapes_skipped: u64,
	/// Number of rows identified as expired.
	pub rows_expired: u64,
	/// Number of versioned entries physically dropped.
	pub versions_dropped: u64,
	/// Bytes reclaimed per shape.
	pub bytes_reclaimed: HashMap<ShapeId, u64>,
}
