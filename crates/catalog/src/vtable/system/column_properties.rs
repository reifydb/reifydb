// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::factory},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemColumnProperties {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemColumnProperties {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemColumnProperties {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_column_properties_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemColumnProperties {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut property_ids = Vec::new();
		let mut column_ids = Vec::new();
		let mut property_types = Vec::new();
		let mut property_values = Vec::new();

		let properties = CatalogStore::list_column_properties_all(txn)?;
		for prop in properties {
			property_ids.push(prop.id.0);
			column_ids.push(prop.column.0);
			let (ty, val) = prop.property.to_u8();
			property_types.push(ty);
			property_values.push(val);
		}

		let columns = vec![
			factory::uint8("id", property_ids),
			factory::uint8("column_id", column_ids),
			factory::uint1("type", property_types),
			factory::uint1("value", property_values),
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
