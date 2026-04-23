// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext, VTableRegistry},
};

/// Virtual table that exposes column information for all virtual tables
pub struct SystemVirtualTableColumns {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemVirtualTableColumns {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_virtual_table_columns_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemVirtualTableColumns {
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
		for vtable in VTableRegistry::list_vtables(txn)? {
			for col in &vtable.columns {
				column_ids.push(col.id.0);
				vtable_ids.push(vtable.id.0);
				names.push(col.name.clone());
				types.push(col.constraint.get_type().to_u8());
				positions.push(col.index.0);
			}
		}

		// Add columns from user-defined virtual tables
		for vtable in self.catalog.list_user_vtables() {
			for col in &vtable.columns {
				column_ids.push(col.id.0);
				vtable_ids.push(vtable.id.0);
				names.push(col.name.clone());
				types.push(col.constraint.get_type().to_u8());
				positions.push(col.index.0);
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ColumnBuffer::uint8(column_ids)),
			ColumnWithName::new(Fragment::internal("vtable_id"), ColumnBuffer::uint8(vtable_ids)),
			ColumnWithName::new(Fragment::internal("name"), ColumnBuffer::utf8(names)),
			ColumnWithName::new(Fragment::internal("type"), ColumnBuffer::uint1(types)),
			ColumnWithName::new(Fragment::internal("position"), ColumnBuffer::uint1(positions)),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
