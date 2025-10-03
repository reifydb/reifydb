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

/// Virtual table that exposes system primary key column relationships
pub struct PrimaryKeyColumns {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl PrimaryKeyColumns {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_primary_key_columns_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<'a> TableVirtual<'a> for PrimaryKeyColumns {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut pk_ids = Vec::new();
		let mut column_ids = Vec::new();
		let mut positions = Vec::new();

		let pk_columns = CatalogStore::list_primary_key_columns(txn)?;
		for (pk_id, column_id, position) in pk_columns {
			pk_ids.push(pk_id);
			column_ids.push(column_id);
			positions.push(position as u16);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("primary_key_id"),
				data: ColumnData::uint8(pk_ids),
			},
			Column {
				name: Fragment::owned_internal("column_id"),
				data: ColumnData::uint8(column_ids),
			},
			Column {
				name: Fragment::owned_internal("position"),
				data: ColumnData::uint2(positions),
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
