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

/// Virtual table that exposes system column information
pub struct SystemColumnsTable {
	pub(crate) definition: Arc<VTable>,
	exhausted: bool,
}

impl SystemColumnsTable {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_columns_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemColumnsTable {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut column_ids = Vec::new();
		let mut schema_ids = Vec::new();
		let mut schema_types = Vec::new();
		let mut column_names = Vec::new();
		let mut column_types = Vec::new();
		let mut positions = Vec::new();
		let mut auto_increments = Vec::new();
		let mut dictionary_ids = Vec::new();

		let columns_list = CatalogStore::list_columns_all(txn)?;
		for info in columns_list {
			column_ids.push(info.column.id.0);
			schema_ids.push(info.schema_id.as_u64());
			schema_types.push(info.schema_id.to_type_u8());
			column_names.push(info.column.name);
			column_types.push(info.column.constraint.get_type().to_u8());
			positions.push(info.column.index.0);
			auto_increments.push(info.column.auto_increment);
			dictionary_ids.push(info.column.dictionary_id.map(|d| d.0).unwrap_or(0));
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(column_ids),
			},
			Column {
				name: Fragment::internal("schema_id"),
				data: ColumnData::uint8(schema_ids),
			},
			Column {
				name: Fragment::internal("schema_type"),
				data: ColumnData::uint1(schema_types),
			},
			Column {
				name: Fragment::internal("name"),
				data: ColumnData::utf8(column_names),
			},
			Column {
				name: Fragment::internal("type"),
				data: ColumnData::uint1(column_types),
			},
			Column {
				name: Fragment::internal("position"),
				data: ColumnData::uint1(positions),
			},
			Column {
				name: Fragment::internal("auto_increment"),
				data: ColumnData::bool(auto_increments),
			},
			Column {
				name: Fragment::internal("dictionary_id"),
				data: ColumnData::uint8(dictionary_ids),
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTable {
		&self.definition
	}
}
