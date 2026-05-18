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

pub struct SystemDictionaries {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemDictionaries {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemDictionaries {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_dictionaries_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemDictionaries {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut names = Vec::new();
		let mut value_types = Vec::new();
		let mut id_types = Vec::new();

		let dictionaries = CatalogStore::list_all_dictionaries(txn)?;
		for dict in dictionaries {
			ids.push(dict.id.0);
			namespace_ids.push(dict.namespace.0);
			names.push(dict.name);
			value_types.push(dict.value_type.to_string());
			id_types.push(dict.id_type.to_string());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ColumnBuffer::uint8(ids)),
			ColumnWithName::new(Fragment::internal("namespace_id"), ColumnBuffer::uint8(namespace_ids)),
			ColumnWithName::new(Fragment::internal("name"), ColumnBuffer::utf8(names)),
			ColumnWithName::new(Fragment::internal("value_type"), ColumnBuffer::utf8(value_types)),
			ColumnWithName::new(Fragment::internal("id_type"), ColumnBuffer::utf8(id_types)),
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
