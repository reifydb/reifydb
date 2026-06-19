// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod actor;
pub mod scanner;

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::{config::GetConfig, shape::ShapeId},
	row::RowSettings,
};

pub trait ListRowSettings: Clone + Send + Sync + 'static {
	fn list_row_settings(&self) -> Vec<(ShapeId, RowSettings)>;
	fn config(&self) -> Arc<dyn GetConfig>;
}

#[derive(Debug, Default)]
pub struct ScanStats {
	pub shapes_scanned: u64,

	pub shapes_skipped: u64,

	pub rows_expired: u64,

	pub versions_dropped: u64,

	pub bytes_discovered: HashMap<ShapeId, u64>,

	pub bytes_reclaimed: HashMap<ShapeId, u64>,
}
