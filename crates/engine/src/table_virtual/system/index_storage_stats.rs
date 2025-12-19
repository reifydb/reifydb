// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::system::SystemCatalog;
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::StorageTracker;
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes storage statistics for indexes
///
/// Note: Index storage is tracked at the KeyKind level, not per-individual-index.
/// This table currently returns empty results. Per-index tracking may be added
/// in a future enhancement.
pub struct IndexStorageStats {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	#[allow(dead_code)]
	stats_tracker: StorageTracker,
}

impl IndexStorageStats {
	pub fn new(stats_tracker: StorageTracker) -> Self {
		Self {
			definition: SystemCatalog::get_system_index_storage_stats_table_def().clone(),
			exhausted: false,
			stats_tracker,
		}
	}
}

impl<'a> TableVirtual<'a> for IndexStorageStats {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		// Index storage is tracked at the KeyKind level (KeyKind::Index, KeyKind::IndexEntry)
		// not at the per-index level. Per-index tracking would require parsing index keys
		// to extract IndexId, which is not currently implemented.
		//
		// Return empty result for now - this can be enhanced in the future to provide
		// aggregate stats by KeyKind or by implementing per-index ObjectId tracking.

		let columns = vec![
			Column {
				name: Fragment::owned_internal("id"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("table_id"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("tier"),
				data: ColumnData::utf8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("current_key_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("current_value_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("current_total_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("current_entry_count"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("historical_key_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("historical_value_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("historical_total_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("historical_entry_count"),
				data: ColumnData::uint8_with_capacity(0),
			},
			Column {
				name: Fragment::owned_internal("total_bytes"),
				data: ColumnData::uint8_with_capacity(0),
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
