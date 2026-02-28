// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_metric::metric::MetricReader;
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes storage statistics for indexes
///
/// Note: Index storage is tracked at the KeyKind level, not per-individual-index.
/// This table currently returns empty results. Per-index tracking may be added
/// in a future enhancement.
pub struct IndexStorageStats {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
	#[allow(dead_code)]
	stats_reader: MetricReader<SingleStore>,
}

impl IndexStorageStats {
	pub fn new(stats_reader: MetricReader<SingleStore>) -> Self {
		Self {
			definition: SystemCatalog::get_system_index_storage_stats_table_def().clone(),
			exhausted: false,
			stats_reader,
		}
	}
}

impl VTable for IndexStorageStats {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Index storage is tracked at the KeyKind level (KeyKind::Index, KeyKind::IndexEntry)
		// not at the per-index level. Per-index tracking would require parsing index keys
		// to extract IndexId, which is not currently implemented.
		//
		// Return empty result for now - this can be enhanced in the future to provide
		// aggregate stats by KeyKind or by implementing per-index Id tracking.

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("table_id"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("tier"),
				data: ColumnData::utf8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("current_key_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("current_value_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("current_total_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("current_count"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("historical_key_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("historical_value_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("historical_total_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("historical_count"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("total_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("cdc_key_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("cdc_value_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("cdc_total_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::internal("cdc_count"),
				data: ColumnData::uint8_with_capacity(0),
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
