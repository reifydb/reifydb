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
	catalog::Catalog,
	system::{SystemCatalog, ids::vtable},
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes information about all virtual tables (system and user-defined)
pub struct TablesVirtual {
	pub(crate) definition: Arc<VTableDef>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl TablesVirtual {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			definition: SystemCatalog::get_system_virtual_tables_table_def().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl VTable for TablesVirtual {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Collect all virtual tables (system only - user-defined tables require catalog access)
		let mut ids = Vec::new();
		let mut namespaces = Vec::new();
		let mut names = Vec::new();
		let mut kinds = Vec::new();

		// Add system virtual tables
		let system_tables = [
			(vtable::SEQUENCES, "sequences"),
			(vtable::NAMESPACES, "namespaces"),
			(vtable::TABLES, "tables"),
			(vtable::VIEWS, "views"),
			(vtable::FLOWS, "flows"),
			(vtable::COLUMNS, "columns"),
			(vtable::COLUMN_POLICIES, "column_policies"),
			(vtable::PRIMARY_KEYS, "primary_keys"),
			(vtable::PRIMARY_KEY_COLUMNS, "primary_key_columns"),
			(vtable::VERSIONS, "versions"),
			(vtable::PRIMITIVE_RETENTION_POLICIES, "primitive_retention_policies"),
			(vtable::OPERATOR_RETENTION_POLICIES, "operator_retention_policies"),
			(vtable::CDC_CONSUMERS, "cdc_consumers"),
			(vtable::FLOW_OPERATORS, "flow_operators"),
			(vtable::FLOW_NODES, "flow_nodes"),
			(vtable::FLOW_EDGES, "flow_edges"),
			(vtable::DICTIONARIES, "dictionaries"),
			(vtable::VIRTUAL_TABLES, "virtual_tables"),
		];

		for (id, name) in system_tables {
			ids.push(id.0);
			namespaces.push(1u64); // system namespace
			names.push(name.to_string());
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
