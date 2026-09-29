// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::tag::type_tag_byte;
use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::factory},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext, VTableRegistry},
};

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

		for vtable in VTableRegistry::list_vtables(txn)? {
			for col in &vtable.columns {
				column_ids.push(col.id.0);
				vtable_ids.push(vtable.id.0);
				names.push(col.name.clone());
				types.push(type_tag_byte(&col.constraint.get_type()));
				positions.push(col.index.0);
			}
		}

		for vtable in self.catalog.list_user_vtables() {
			for col in &vtable.columns {
				column_ids.push(col.id.0);
				vtable_ids.push(vtable.id.0);
				names.push(col.name.clone());
				types.push(type_tag_byte(&col.constraint.get_type()));
				positions.push(col.index.0);
			}
		}

		let columns = vec![
			factory::uint8("id", column_ids),
			factory::uint8("vtable_id", vtable_ids),
			factory::utf8("name", names),
			factory::uint1("type", types),
			factory::uint1("position", positions),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
