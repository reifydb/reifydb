// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{Batch, QueryTransaction, VTableDef},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	system::{SystemCatalog, ids::vtable},
	transaction::MaterializedCatalogTransaction,
	vtable::{VTable, VTableContext},
};

/// Virtual table that exposes information about all virtual tables (system and user-defined)
pub struct TablesVirtual {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl TablesVirtual {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_virtual_tables_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: QueryTransaction + MaterializedCatalogTransaction> VTable<T> for TablesVirtual {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Collect all virtual tables (system + user-defined)
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

		// Add user-defined virtual tables
		let catalog = txn.catalog();
		let user_tables = catalog.list_vtable_user_all();

		for table_def in user_tables {
			ids.push(table_def.id.0);
			namespaces.push(table_def.namespace.0);
			names.push(table_def.name.clone());
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
