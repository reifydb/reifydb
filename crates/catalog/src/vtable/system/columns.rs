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
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

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
		let mut object_ids = Vec::new();
		let mut object_types = Vec::new();
		let mut column_names = Vec::new();
		let mut column_types = Vec::new();
		let mut positions = Vec::new();
		let mut auto_increments = Vec::new();
		let mut dictionary_ids = Vec::new();

		let columns_list = CatalogStore::list_columns_all(txn)?;
		for info in columns_list {
			column_ids.push(info.column.id.0);
			object_ids.push(info.object_id.as_u64());
			object_types.push(info.object_id.type_tag());
			column_names.push(info.column.name);
			column_types.push(type_tag_byte(&info.column.constraint.get_type()));
			positions.push(info.column.index.0);
			auto_increments.push(info.column.auto_increment);
			dictionary_ids.push(info.column.dictionary_id.map(|d| d.0).unwrap_or(0));
		}

		let columns = vec![
			factory::uint8("id", column_ids),
			factory::uint8("object_id", object_ids),
			factory::uint1("object_type", object_types),
			factory::utf8("name", column_names),
			factory::uint1("type", column_types),
			factory::uint1("position", positions),
			factory::bool("auto_increment", auto_increments),
			factory::uint8("dictionary_id", dictionary_ids),
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
