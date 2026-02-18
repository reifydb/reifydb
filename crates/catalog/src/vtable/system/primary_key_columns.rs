// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system primary key column relationships
pub struct PrimaryKeyColumns {
	pub(crate) definition: Arc<VTableDef>,
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

impl VTable for PrimaryKeyColumns {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
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
				name: Fragment::internal("primary_key_id"),
				data: ColumnData::uint8(pk_ids),
			},
			Column {
				name: Fragment::internal("column_id"),
				data: ColumnData::uint8(column_ids),
			},
			Column {
				name: Fragment::internal("position"),
				data: ColumnData::uint2(positions),
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
