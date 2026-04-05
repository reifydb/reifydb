// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod actor;
pub(crate) mod scanner;

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::{config::GetConfig, shape::ShapeId},
	row::RowTtl,
};

/// Trait for providing TTL configurations to the actor.
pub trait ListRowTtls: Clone + Send + Sync + 'static {
	fn list_row_ttls(&self) -> Vec<(ShapeId, RowTtl)>;
	fn config(&self) -> Arc<dyn GetConfig>;
}

/// Statistics from a single row TTL scan cycle.
#[derive(Debug, Default)]
pub(crate) struct ScanStats {
	/// Number of shapes scanned for expired rows.
	pub shapes_scanned: u64,
	/// Number of shapes skipped (e.g. CleanupMode::Delete not supported in V1).
	pub shapes_skipped: u64,
	/// Number of rows identified as expired.
	pub rows_expired: u64,
	/// Number of versioned entries physically dropped.
	pub versions_dropped: u64,
	/// Bytes discovered during scan (current version size).
	pub bytes_discovered: HashMap<ShapeId, u64>,
	/// Bytes reclaimed per shape (all versions dropped).
	pub bytes_reclaimed: HashMap<ShapeId, u64>,
}
