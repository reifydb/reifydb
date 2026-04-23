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

/// Virtual table that exposes system namespace information
pub struct SystemNamespaces {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemNamespaces {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemNamespaces {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_namespaces_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemNamespaces {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut namespace_ids = Vec::new();
		let mut namespace_names = Vec::new();
		let mut namespace_local_names = Vec::new();
		let mut namespace_parent_ids = Vec::new();

		let namespaces = CatalogStore::list_namespaces_all(txn)?;
		for namespace in namespaces {
			namespace_ids.push(namespace.id().0);
			namespace_names.push(namespace.name().to_string());
			namespace_local_names.push(namespace.local_name().to_string());
			namespace_parent_ids.push(namespace.parent_id().0);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ColumnBuffer::uint8(namespace_ids)),
			ColumnWithName::new(Fragment::internal("name"), ColumnBuffer::utf8(namespace_names)),
			ColumnWithName::new(
				Fragment::internal("local_name"),
				ColumnBuffer::utf8(namespace_local_names),
			),
			ColumnWithName::new(Fragment::internal("parent_id"), ColumnBuffer::uint8(namespace_parent_ids)),
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
