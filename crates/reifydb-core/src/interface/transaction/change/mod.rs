// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod schema;
mod table;
mod view;

use crate::interface::{
	SchemaDef, SchemaId, TableDef, TableId, TransactionId, ViewDef, ViewId,
};
use crate::{error, internal_error};
use std::collections::HashMap;

/// Tracks all catalog changes within a transaction
#[derive(Debug, Clone)]
pub struct TransactionalChanges {
	/// Transaction ID this change set belongs to
	txn_id: TransactionId,
	/// Schema definition changes indexed by SchemaId
	schema_def: HashMap<SchemaId, Change<SchemaDef>>,
	/// Table definition changes indexed by TableId
	table_def: HashMap<TableId, Change<TableDef>>,
	/// View definition changes indexed by ViewId
	view_def: HashMap<ViewId, Change<ViewDef>>,
	/// Order of operations for replay/rollback
	log: Vec<Operation>,
}

/// Represents a single change
#[derive(Debug, Clone)]
pub struct Change<T> {
	/// State before the change (None for CREATE)
	pub pre: Option<T>,

	/// State after the change (None for DELETE)
	pub post: Option<T>,

	/// Type of operation
	pub operation: OperationType,
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
			schema_def: HashMap::new(),
			table_def: HashMap::new(),
			view_def: HashMap::new(),
			log: Vec::new(),
		}
	}

	/// Check if a schema exists in this transaction's view
	pub fn schema_exists(&self, id: SchemaId) -> bool {
		match self.schema_def.get(&id) {
			Some(change) => change.post.is_some(),
			None => false,
		}
	}

	/// Get current state of a schema within this transaction
	pub fn get_schema(&self, id: SchemaId) -> Option<&SchemaDef> {
		self.schema_def.get(&id).and_then(|change| change.post.as_ref())
	}

	/// Check if a table exists in this transaction's view
	pub fn table_exists(&self, id: TableId) -> bool {
		match self.table_def.get(&id) {
			Some(change) => change.post.is_some(),
			None => false,
		}
	}

	/// Get current state of a table within this transaction
	pub fn get_table(&self, id: TableId) -> Option<&TableDef> {
		self.table_def.get(&id).and_then(|change| change.post.as_ref())
	}

	/// Check if a view exists in this transaction's view
	pub fn view_exists(&self, id: ViewId) -> bool {
		match self.view_def.get(&id) {
			Some(change) => change.post.is_some(),
			None => false,
		}
	}

	/// Get current state of a view within this transaction
	pub fn get_view(&self, id: ViewId) -> Option<&ViewDef> {
		self.view_def.get(&id).and_then(|change| change.post.as_ref())
	}

	/// Get all pending changes for commit
	pub fn get_pending_changes(&self) -> &[Operation] {
		&self.log
	}

	/// Check if there are any pending changes
	pub fn has_changes(&self) -> bool {
		!self.log.is_empty()
	}

	/// Get the transaction ID
	pub fn txn_id(&self) -> TransactionId {
		self.txn_id
	}

	/// Get schema definition changes
	pub fn schema_def(&self) -> &HashMap<SchemaId, Change<SchemaDef>> {
		&self.schema_def
	}

	/// Get table definition changes
	pub fn table_def(&self) -> &HashMap<TableId, Change<TableDef>> {
		&self.table_def
	}

	/// Get view definition changes
	pub fn view_def(&self) -> &HashMap<ViewId, Change<ViewDef>> {
		&self.view_def
	}

	/// Clear all changes (for rollback)
	pub fn clear(&mut self) {
		self.schema_def.clear();
		self.table_def.clear();
		self.view_def.clear();
		self.log.clear();
	}

	/// Helper to get schema name from SchemaId
	pub(crate) fn get_schema_name(
		&self,
		schema_id: SchemaId,
	) -> crate::Result<String> {
		self.schema_def
			.get(&schema_id)
			.and_then(|change| {
				change.post.as_ref().or(change.pre.as_ref())
			})
			.map(|schema| schema.name.clone())
			.ok_or_else(|| {
				error!(internal_error!(
					"Schema {} not found in transaction changes - this should never happen",
					schema_id
				))
			})
	}
}
