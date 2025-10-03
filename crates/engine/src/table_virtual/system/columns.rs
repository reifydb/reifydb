// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system column information
pub struct ColumnsTable {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl ColumnsTable {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_columns_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<'a> TableVirtual<'a> for ColumnsTable {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut column_ids = Vec::new();
		let mut source_ids = Vec::new();
		let mut store_types = Vec::new();
		let mut column_names = Vec::new();
		let mut column_types = Vec::new();
		let mut positions = Vec::new();
		let mut auto_increments = Vec::new();

		let columns_list = CatalogStore::list_columns_all(txn)?;
		for info in columns_list {
			column_ids.push(info.column.id.0);
			source_ids.push(info.source_id.as_u64());
			store_types.push(if info.is_view {
				1u8
			} else {
				0u8
			});
			column_names.push(info.column.name);
			column_types.push(info.column.constraint.get_type().to_u8());
			positions.push(info.column.index.0);
			auto_increments.push(info.column.auto_increment);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("id"),
				data: ColumnData::uint8(column_ids),
			},
			Column {
				name: Fragment::owned_internal("source_id"),
				data: ColumnData::uint8(source_ids),
			},
			Column {
				name: Fragment::owned_internal("source_type"),
				data: ColumnData::uint1(store_types),
			},
			Column {
				name: Fragment::owned_internal("name"),
				data: ColumnData::utf8(column_names),
			},
			Column {
				name: Fragment::owned_internal("type"),
				data: ColumnData::uint1(column_types),
			},
			Column {
				name: Fragment::owned_internal("position"),
				data: ColumnData::uint2(positions),
			},
			Column {
				name: Fragment::owned_internal("auto_increment"),
				data: ColumnData::bool(auto_increments),
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
