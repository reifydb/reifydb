// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system column policy information
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
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(property_ids),
			},
			Column {
				name: Fragment::internal("column_id"),
				data: ColumnData::uint8(column_ids),
			},
			Column {
				name: Fragment::internal("type"),
				data: ColumnData::uint1(property_types),
			},
			Column {
				name: Fragment::internal("value"),
				data: ColumnData::uint1(property_values),
			},
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
