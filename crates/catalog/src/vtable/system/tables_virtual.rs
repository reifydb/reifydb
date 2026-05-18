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
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext, VTableRegistry},
};

pub struct SystemTablesVirtual {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemTablesVirtual {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_virtual_tables_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemTablesVirtual {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespaces = Vec::new();
		let mut names = Vec::new();
		let mut kinds = Vec::new();

		for vtable in VTableRegistry::list_vtables(txn)? {
			ids.push(vtable.id.0);
			namespaces.push(vtable.namespace.0);
			names.push(vtable.name.clone());
			kinds.push("system".to_string());
		}

		for def in self.catalog.list_user_vtables() {
			ids.push(def.id.0);
			namespaces.push(def.namespace.0);
			names.push(def.name.clone());
			kinds.push("user".to_string());
		}

		let mut id_col = ColumnBuffer::uint8_with_capacity(ids.len());
		let mut ns_col = ColumnBuffer::uint8_with_capacity(namespaces.len());
		let mut name_col = ColumnBuffer::utf8_with_capacity(names.len());
		let mut kind_col = ColumnBuffer::utf8_with_capacity(kinds.len());

		for id in ids {
			id_col.push(id);
		}
		for ns in namespaces {
			ns_col.push(ns);
		}
		for name in &names {
			name_col.push(name.as_str());
		}
		for kind in &kinds {
			kind_col.push(kind.as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), id_col),
			ColumnWithName::new(Fragment::internal("namespace_id"), ns_col),
			ColumnWithName::new(Fragment::internal("name"), name_col),
			ColumnWithName::new(Fragment::internal("kind"), kind_col),
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
