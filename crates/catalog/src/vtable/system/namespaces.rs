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

/// Virtual table that exposes system namespace information
pub struct Namespaces {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Namespaces {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_namespaces_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for Namespaces {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut namespace_ids = Vec::new();
		let mut namespace_names = Vec::new();
		let mut namespace_parent_ids = Vec::new();

		let namespaces = CatalogStore::list_namespaces_all(txn)?;
		for namespace in namespaces {
			namespace_ids.push(namespace.id.0);
			namespace_names.push(namespace.name);
			namespace_parent_ids.push(namespace.parent_id.0);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ColumnData::uint8(namespace_ids),
			},
			Column {
				name: Fragment::internal("name"),
				data: ColumnData::utf8(namespace_names),
			},
			Column {
				name: Fragment::internal("parent_id"),
				data: ColumnData::uint8(namespace_parent_ids),
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
