// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{
	SchemaDef, SchemaId, TableDef, TableId, TransactionId, ViewDef, ViewId,
};
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

	/// Add a schema definition creation
	pub fn add_schema_def_create(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()> {
		if self.schema_def.contains_key(&schema.id) {
			return Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict()));
		}

		self.schema_def.insert(
			schema.id,
			Change {
				pre: None,
				post: Some(schema.clone()),
				operation: OperationType::Create,
			},
		);

		self.log.push(Operation::Schema {
			id: schema.id,
			op: OperationType::Create,
		});

		Ok(())
	}

	/// Add a schema definition update
	pub fn add_schema_def_update(
		&mut self,
		pre: SchemaDef,
		post: SchemaDef,
	) -> crate::Result<()> {
		match self.schema_def.get_mut(&post.id) {
            Some(existing) if existing.operation == OperationType::Create => {
                // Coalesce with create - just update the "post" state
                existing.post = Some(post);
                Ok(())
            }
            Some(existing) if existing.operation == OperationType::Update => {
                // Coalesce multiple updates - keep original "pre", update "post"
                existing.post = Some(post);
                Ok(())
            }
            Some(_) => Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict())),
            None => {
                self.schema_def.insert(
                    post.id,
                    Change {
                        pre: Some(pre),
                        post: Some(post.clone()),
                        operation: OperationType::Update,
                    },
                );

                self.log.push(Operation::Schema {
                    id: post.id,
                    op: OperationType::Update,
                });

                Ok(())
            }
        }
	}

	/// Add a schema definition deletion
	pub fn add_schema_def_delete(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()> {
		match self.schema_def.get_mut(&schema.id) {
            Some(existing) if existing.operation == OperationType::Create => {
                // Created and deleted in same transaction - remove entirely
                self.schema_def.remove(&schema.id);
                // Remove from operation log
                self.log.retain(|op| {
                    !matches!(op, Operation::Schema { id, .. } if *id == schema.id)
                });
                Ok(())
            }
            Some(existing) if existing.operation == OperationType::Update => {
                // Convert update to delete, keep original pre state
                existing.post = None;
                existing.operation = OperationType::Delete;
                // Update operation log
                if let Some(op) = self.log.iter_mut().rev().find(|op| {
                    matches!(op, Operation::Schema { id, .. } if *id == schema.id)
                }) {
                    *op = Operation::Schema {
                        id: schema.id,
                        op: OperationType::Delete,
                    };
                }
                Ok(())
            }
            Some(_) => Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict())),
            None => {
                self.schema_def.insert(
                    schema.id,
                    Change {
                        pre: Some(schema.clone()),
                        post: None,
                        operation: OperationType::Delete,
                    },
                );

                self.log.push(Operation::Schema {
                    id: schema.id,
                    op: OperationType::Delete,
                });

                Ok(())
            }
        }
	}

	/// Add a table definition creation
	pub fn add_table_def_create(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		if self.table_def.contains_key(&table.id) {
			return Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict()));
		}

		self.table_def.insert(
			table.id,
			Change {
				pre: None,
				post: Some(table.clone()),
				operation: OperationType::Create,
			},
		);

		self.log.push(Operation::Table {
			id: table.id,
			op: OperationType::Create,
		});

		Ok(())
	}

	/// Add a table definition update
	pub fn add_table_def_update(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> crate::Result<()> {
		match self.table_def.get_mut(&post.id) {
            Some(existing) if existing.operation == OperationType::Create => {
                // Coalesce with create - just update the "post" state
                existing.post = Some(post);
                Ok(())
            }
            Some(existing) if existing.operation == OperationType::Update => {
                // Coalesce multiple updates - keep original "pre", update "post"
                existing.post = Some(post);
                Ok(())
            }
            Some(_) => Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict())),
            None => {
                self.table_def.insert(
                    post.id,
                    Change {
                        pre: Some(pre),
                        post: Some(post.clone()),
                        operation: OperationType::Update,
                    },
                );

                self.log.push(Operation::Table {
                    id: post.id,
                    op: OperationType::Update,
                });

                Ok(())
            }
        }
	}

	/// Add a table definition deletion
	pub fn add_table_def_delete(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		match self.table_def.get_mut(&table.id) {
            Some(existing) if existing.operation == OperationType::Create => {
                // Created and deleted in same transaction - remove entirely
                self.table_def.remove(&table.id);
                // Remove from operation log
                self.log.retain(|op| {
                    !matches!(op, Operation::Table { id, .. } if *id == table.id)
                });
                Ok(())
            }
            Some(existing) if existing.operation == OperationType::Update => {
                // Convert update to delete, keep original pre state
                existing.post = None;
                existing.operation = OperationType::Delete;
                // Update operation log
                if let Some(op) = self.log.iter_mut().rev().find(|op| {
                    matches!(op, Operation::Table { id, .. } if *id == table.id)
                }) {
                    *op = Operation::Table {
                        id: table.id,
                        op: OperationType::Delete,
                    };
                }
                Ok(())
            }
            Some(_) => Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict())),
            None => {
                self.table_def.insert(
                    table.id,
                    Change {
                        pre: Some(table.clone()),
                        post: None,
                        operation: OperationType::Delete,
                    },
                );

                self.log.push(Operation::Table {
                    id: table.id,
                    op: OperationType::Delete,
                });

                Ok(())
            }
        }
	}

	/// Add a view definition creation
	pub fn add_view_def_create(&mut self, view: ViewDef) -> crate::Result<()> {
		if self.view_def.contains_key(&view.id) {
			return Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict()));
		}

		self.view_def.insert(
			view.id,
			Change {
				pre: None,
				post: Some(view.clone()),
				operation: OperationType::Create,
			},
		);

		self.log.push(Operation::View {
			id: view.id,
			op: OperationType::Create,
		});

		Ok(())
	}

	/// Add a view definition update
	pub fn add_view_def_update(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()> {
		match self.view_def.get_mut(&post.id) {
            Some(existing) if existing.operation == OperationType::Create => {
                // Coalesce with create - just update the "post" state
                existing.post = Some(post);
                Ok(())
            }
            Some(existing) if existing.operation == OperationType::Update => {
                // Coalesce multiple updates - keep original "pre", update "post"
                existing.post = Some(post);
                Ok(())
            }
            Some(_) => Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict())),
            None => {
                self.view_def.insert(
                    post.id,
                    Change {
                        pre: Some(pre),
                        post: Some(post.clone()),
                        operation: OperationType::Update,
                    },
                );

                self.log.push(Operation::View {
                    id: post.id,
                    op: OperationType::Update,
                });

                Ok(())
            }
        }
	}

	/// Add a view definition deletion
	pub fn add_view_def_delete(&mut self, view: ViewDef) -> crate::Result<()> {
		match self.view_def.get_mut(&view.id) {
            Some(existing) if existing.operation == OperationType::Create => {
                // Created and deleted in same transaction - remove entirely
                self.view_def.remove(&view.id);
                // Remove from operation log
                self.log.retain(|op| {
                    !matches!(op, Operation::View { id, .. } if *id == view.id)
                });
                Ok(())
            }
            Some(existing) if existing.operation == OperationType::Update => {
                // Convert update to delete, keep original pre state
                existing.post = None;
                existing.operation = OperationType::Delete;
                // Update operation log
                if let Some(op) = self.log.iter_mut().rev().find(|op| {
                    matches!(op, Operation::View { id, .. } if *id == view.id)
                }) {
                    *op = Operation::View {
                        id: view.id,
                        op: OperationType::Delete,
                    };
                }
                Ok(())
            }
            Some(_) => Err(crate::error!(crate::result::error::diagnostic::transaction::transaction_conflict())),
            None => {
                self.view_def.insert(
                    view.id,
                    Change {
                        pre: Some(view.clone()),
                        post: None,
                        operation: OperationType::Delete,
                    },
                );

                self.log.push(Operation::View {
                    id: view.id,
                    op: OperationType::Delete,
                });

                Ok(())
            }
        }
	}

	/// Check if a schema exists in this transaction's view
	pub fn schema_exists(&self, id: SchemaId) -> bool {
		match self.schema_def.get(&id) {
			Some(change) => change.post.is_some(),
			None => false, // Would need to check MaterializedCatalog
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
			None => false, // Would need to check MaterializedCatalog
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
			None => false, // Would need to check MaterializedCatalog
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
}
