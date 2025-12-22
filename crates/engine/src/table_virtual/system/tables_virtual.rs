// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_catalog::system::{SystemCatalog, ids::table_virtual};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes information about all virtual tables (system and user-defined)
pub struct TablesVirtual {
	definition: Arc<TableVirtualDef>,
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
impl TableVirtual for TablesVirtual {
	async fn initialize<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		_ctx: TableVirtualContext,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next<'a>(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch>> {
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
			(table_virtual::SEQUENCES, "sequences"),
			(table_virtual::NAMESPACES, "namespaces"),
			(table_virtual::TABLES, "tables"),
			(table_virtual::VIEWS, "views"),
			(table_virtual::FLOWS, "flows"),
			(table_virtual::COLUMNS, "columns"),
			(table_virtual::COLUMN_POLICIES, "column_policies"),
			(table_virtual::PRIMARY_KEYS, "primary_keys"),
			(table_virtual::PRIMARY_KEY_COLUMNS, "primary_key_columns"),
			(table_virtual::VERSIONS, "versions"),
			(table_virtual::SOURCE_RETENTION_POLICIES, "source_retention_policies"),
			(table_virtual::OPERATOR_RETENTION_POLICIES, "operator_retention_policies"),
			(table_virtual::CDC_CONSUMERS, "cdc_consumers"),
			(table_virtual::FLOW_OPERATORS, "flow_operators"),
			(table_virtual::FLOW_NODES, "flow_nodes"),
			(table_virtual::FLOW_EDGES, "flow_edges"),
			(table_virtual::DICTIONARIES, "dictionaries"),
			(table_virtual::VIRTUAL_TABLES, "virtual_tables"),
		];

		for (id, name) in system_tables {
			ids.push(id.0);
			namespaces.push(1u64); // system namespace
			names.push(name.to_string());
			kinds.push("system".to_string());
		}

		// Add user-defined virtual tables
		let catalog = txn.catalog();
		let user_tables = catalog.list_table_virtual_user_all();

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

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
