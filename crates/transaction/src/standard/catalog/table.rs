// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackTableChangeOperations,
	id::{NamespaceId, TableId},
	table::TableDef,
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalTableChanges,
	},
	standard::StandardCommandTransaction,
};

impl CatalogTrackTableChangeOperations for StandardCommandTransaction {
	fn track_table_def_created(&mut self, table: TableDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(table),
			op: Create,
		};
		self.changes.add_table_def_change(change);
		Ok(())
	}

	fn track_table_def_updated(&mut self, pre: TableDef, post: TableDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_table_def_change(change);
		Ok(())
	}

	fn track_table_def_deleted(&mut self, table: TableDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(table),
			post: None,
			op: Delete,
		};
		self.changes.add_table_def_change(change);
		Ok(())
	}
}

impl TransactionalTableChanges for StandardCommandTransaction {
	fn find_table(&self, id: TableId) -> Option<&TableDef> {
		for change in self.changes.table_def.iter().rev() {
			if let Some(table) = &change.post {
				if table.id == id {
					return Some(table);
				}
			} else if let Some(table) = &change.pre {
				if table.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_table_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&TableDef> {
		self.changes
			.table_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|t| t.namespace == namespace && t.name == name))
	}

	fn is_table_deleted(&self, id: TableId) -> bool {
		self.changes
			.table_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|t| t.id) == Some(id))
	}

	fn is_table_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.table_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|t| t.namespace == namespace && t.name == name)
					.unwrap_or(false)
		})
	}
}
