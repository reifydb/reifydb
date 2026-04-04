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
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext, VTableRegistry},
};

/// Virtual table that exposes information about all virtual tables (system and user-defined)
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

		// Collect all virtual tables (system only - user-defined tables require catalog access)
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

		// Add user-defined virtual tables from catalog
		for def in self.catalog.list_user_vtables() {
			ids.push(def.id.0);
			namespaces.push(def.namespace.0);
			names.push(def.name.clone());
			kinds.push("user".to_string());
		}

		// Build column data
		let mut id_col = ColumnData::uint8_with_capacity(ids.len());
		let mut ns_col = ColumnData::uint8_with_capacity(namespaces.len());
		let mut name_col = ColumnData::utf8_with_capacity(names.len());
		let mut kind_col = ColumnData::utf8_with_capacity(kinds.len());

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
			Column {
				name: Fragment::internal("id"),
				data: id_col,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: ns_col,
			},
			Column {
				name: Fragment::internal("name"),
				data: name_col,
			},
			Column {
				name: Fragment::internal("kind"),
				data: kind_col,
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
