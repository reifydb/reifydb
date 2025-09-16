// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system column information
pub struct ColumnsTable<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> ColumnsTable<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_columns_table_def().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for ColumnsTable<T> {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a, T>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> Result<Option<Batch>> {
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
			Column::ColumnQualified(ColumnQualified {
				name: "id".to_string(),
				data: ColumnData::uint8(column_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "source_id".to_string(),
				data: ColumnData::uint8(source_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "source_type".to_string(),
				data: ColumnData::uint1(store_types),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "name".to_string(),
				data: ColumnData::utf8(column_names),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "type".to_string(),
				data: ColumnData::uint1(column_types),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "position".to_string(),
				data: ColumnData::uint2(positions),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "auto_increment".to_string(),
				data: ColumnData::bool(auto_increments),
			}),
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
