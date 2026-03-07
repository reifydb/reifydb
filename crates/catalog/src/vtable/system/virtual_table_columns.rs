// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext, VTableRegistry},
};

/// Virtual table that exposes column information for all virtual tables
pub struct VirtualTableColumns {
	pub(crate) definition: Arc<VTableDef>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl VirtualTableColumns {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_virtual_table_columns_table_def().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl VTable for VirtualTableColumns {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut column_ids = Vec::new();
		let mut vtable_ids = Vec::new();
		let mut names = Vec::new();
		let mut types = Vec::new();
		let mut positions = Vec::new();

		// Add columns from system virtual tables
		for vtable_def in VTableRegistry::list_vtables(txn)? {
			for col in &vtable_def.columns {
				column_ids.push(col.id.0);
				vtable_ids.push(vtable_def.id.0);
				names.push(col.name.clone());
				types.push(col.constraint.get_type().to_u8());
				positions.push(col.index.0);
			}
		}

		// Add columns from user-defined virtual tables
		for vtable_def in self.catalog.list_user_vtables() {
			for col in &vtable_def.columns {
				column_ids.push(col.id.0);
				vtable_ids.push(vtable_def.id.0);
				names.push(col.name.clone());
				types.push(col.constraint.get_type().to_u8());
				positions.push(col.index.0);
			}
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(column_ids),
			},
			Column {
				name: Fragment::internal("vtable_id"),
				data: ColumnData::uint8(vtable_ids),
			},
			Column {
				name: Fragment::internal("name"),
				data: ColumnData::utf8(names),
			},
			Column {
				name: Fragment::internal("type"),
				data: ColumnData::uint1(types),
			},
			Column {
				name: Fragment::internal("position"),
				data: ColumnData::uint1(positions),
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
