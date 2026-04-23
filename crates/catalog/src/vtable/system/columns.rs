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
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system column information
pub struct SystemColumnsTable {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemColumnsTable {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemColumnsTable {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_columns_table().clone(),
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
		let mut shape_ids = Vec::new();
		let mut shape_types = Vec::new();
		let mut column_names = Vec::new();
		let mut column_types = Vec::new();
		let mut positions = Vec::new();
		let mut auto_increments = Vec::new();
		let mut dictionary_ids = Vec::new();

		let columns_list = CatalogStore::list_columns_all(txn)?;
		for info in columns_list {
			column_ids.push(info.column.id.0);
			shape_ids.push(info.shape_id.as_u64());
			shape_types.push(info.shape_id.to_type_u8());
			column_names.push(info.column.name);
			column_types.push(info.column.constraint.get_type().to_u8());
			positions.push(info.column.index.0);
			auto_increments.push(info.column.auto_increment);
			dictionary_ids.push(info.column.dictionary_id.map(|d| d.0).unwrap_or(0));
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ColumnBuffer::uint8(column_ids)),
			ColumnWithName::new(Fragment::internal("shape_id"), ColumnBuffer::uint8(shape_ids)),
			ColumnWithName::new(Fragment::internal("shape_type"), ColumnBuffer::uint1(shape_types)),
			ColumnWithName::new(Fragment::internal("name"), ColumnBuffer::utf8(column_names)),
			ColumnWithName::new(Fragment::internal("type"), ColumnBuffer::uint1(column_types)),
			ColumnWithName::new(Fragment::internal("position"), ColumnBuffer::uint1(positions)),
			ColumnWithName::new(Fragment::internal("auto_increment"), ColumnBuffer::bool(auto_increments)),
			ColumnWithName::new(Fragment::internal("dictionary_id"), ColumnBuffer::uint8(dictionary_ids)),
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
