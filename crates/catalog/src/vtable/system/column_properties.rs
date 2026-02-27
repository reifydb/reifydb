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

/// Virtual table that exposes system column policy information
pub struct ColumnProperties {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl ColumnProperties {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_column_properties_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for ColumnProperties {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
