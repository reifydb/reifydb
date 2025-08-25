// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{
	OperationType::Delete, SchemaDef, SchemaId, TableDef, TableId,
	TransactionId, ViewDef, ViewId,
};

/// Tracks all catalog changes within a transaction
#[derive(Default, Debug, Clone)]
pub struct TransactionalChanges {
	/// Transaction ID this change set belongs to
	pub txn_id: TransactionId,
	/// All schema definition changes in order (no coalescing)
	pub schema_def: Vec<Change<SchemaDef>>,
	/// All table definition changes in order (no coalescing)
	pub table_def: Vec<Change<TableDef>>,
	/// All view definition changes in order (no coalescing)
	pub view_def: Vec<Change<ViewDef>>,
	/// Order of operations for replay/rollback
	pub log: Vec<Operation>,
}

impl TransactionalChanges {
	pub fn add_schema_def_change(&mut self, change: Change<SchemaDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.schema_def.push(change);
		self.log.push(Operation::Schema {
			id,
			op,
		});
	}

	pub fn add_table_def_change(&mut self, change: Change<TableDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|t| t.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.table_def.push(change);
		self.log.push(Operation::Table {
			id,
			op,
		});
	}

	pub fn add_view_def_change(&mut self, change: Change<ViewDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|v| v.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.view_def.push(change);
		self.log.push(Operation::View {
			id,
			op,
		});
	}
}

/// Represents a single change
#[derive(Debug, Clone)]
pub struct Change<T> {
	/// State before the change (None for CREATE)
	pub pre: Option<T>,

	/// State after the change (None for DELETE)
	pub post: Option<T>,

	/// Type of operation
	pub op: OperationType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OperationType {
	Create,
	Update,
	Delete,
}

/// Log entry for operation ordering
#[derive(Debug, Clone)]
pub enum Operation {
	Schema {
		id: SchemaId,
		op: OperationType,
	},
	Table {
		id: TableId,
		op: OperationType,
	},
	View {
		id: ViewId,
		op: OperationType,
	},
}

impl TransactionalChanges {
	pub fn new(txn_id: TransactionId) -> Self {
		Self {
			txn_id,
			schema_def: Vec::new(),
			table_def: Vec::new(),
			view_def: Vec::new(),
			log: Vec::new(),
		}
	}

	/// Check if a schema exists in this transaction's view
	pub fn schema_def_exists(&self, id: SchemaId) -> bool {
		self.get_schema_def(id).is_some()
	}

	/// Get current state of a schema within this transaction
	pub fn get_schema_def(&self, id: SchemaId) -> Option<&SchemaDef> {
		// Find the last change for this schema ID
		for change in self.schema_def.iter().rev() {
			if let Some(schema) = &change.post {
				if schema.id == id {
					return Some(schema);
				}
			} else if let Some(schema) = &change.pre {
				if schema.id == id && change.op == Delete {
					// Schema was deleted
					return None;
				}
			}
		}
		None
	}

	/// Check if a table exists in this transaction's view
	pub fn table_def_exists(&self, id: TableId) -> bool {
		self.get_table_def(id).is_some()
	}

	/// Get current state of a table within this transaction
	pub fn get_table_def(&self, id: TableId) -> Option<&TableDef> {
		// Find the last change for this table ID
		for change in self.table_def.iter().rev() {
			if let Some(table) = &change.post {
				if table.id == id {
					return Some(table);
				}
			} else if let Some(table) = &change.pre {
				if table.id == id && change.op == Delete {
					// Table was deleted
					return None;
				}
			}
		}
		None
	}

	/// Check if a view exists in this transaction's view
	pub fn view_def_exists(&self, id: ViewId) -> bool {
		self.get_view_def(id).is_some()
	}

	/// Get current state of a view within this transaction
	pub fn get_view_def(&self, id: ViewId) -> Option<&ViewDef> {
		// Find the last change for this view ID
		for change in self.view_def.iter().rev() {
			if let Some(view) = &change.post {
				if view.id == id {
					return Some(view);
				}
			} else if let Some(view) = &change.pre {
				if view.id == id && change.op == Delete {
					// View was deleted
					return None;
				}
			}
		}
		None
	}

	/// Get all pending changes for commit
	pub fn get_pending_changes(&self) -> &[Operation] {
		&self.log
	}

	/// Get the transaction ID
	pub fn txn_id(&self) -> TransactionId {
		self.txn_id
	}

	/// Get schema definition changes
	pub fn schema_def(&self) -> &[Change<SchemaDef>] {
		&self.schema_def
	}

	/// Get table definition changes
	pub fn table_def(&self) -> &[Change<TableDef>] {
		&self.table_def
	}

	/// Get view definition changes
	pub fn view_def(&self) -> &[Change<ViewDef>] {
		&self.view_def
	}

	/// Clear all changes (for rollback)
	pub fn clear(&mut self) {
		self.schema_def.clear();
		self.table_def.clear();
		self.view_def.clear();
		self.log.clear();
	}
}
