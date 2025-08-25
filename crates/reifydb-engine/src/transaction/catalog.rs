// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use Operation::{Schema, View};
use OperationType::Update;
use reifydb_catalog::{
	CatalogSchemaDefOperations, CatalogTransaction,
	CatalogTransactionOperations, MaterializedCatalog,
};
use reifydb_core::{
	Version,
	diagnostic::catalog::{
		cannot_delete_already_deleted_schema,
		cannot_delete_already_deleted_table,
		cannot_delete_already_deleted_view,
		cannot_update_deleted_schema, cannot_update_deleted_table,
		cannot_update_deleted_view,
		schema_already_pending_in_transaction,
		table_already_pending_in_transaction,
		view_already_pending_in_transaction,
	},
	interface::{
		Change, Operation,
		Operation::Table,
		OperationType,
		OperationType::{Create, Delete},
		SchemaDef, TableDef, Transaction, VersionedQueryTransaction,
		ViewDef,
	},
	return_error,
};

use crate::StandardCommandTransaction;

impl<T: Transaction> CatalogTransactionOperations
	for StandardCommandTransaction<T>
{
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}

	fn version(&self) -> Version {
		self.versioned.as_ref().unwrap().version()
	}

	fn track_schema_def_created(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()> {
		if self.changes.schema_def.contains_key(&schema.id) {
			return_error!(schema_already_pending_in_transaction(
				&schema.name
			));
		}

		self.changes.change_schema_def(
			schema.id,
			Change {
				pre: None,
				post: Some(schema),
				op: Create,
			},
		);

		Ok(())
	}

	fn track_schema_def_updated(
		&mut self,
		pre: SchemaDef,
		post: SchemaDef,
	) -> crate::Result<()> {
		match self.changes.schema_def.get_mut(&post.id) {
			Some(existing) if existing.op == Create => {
				// Coalesce with create - just update the
				// post-state
				existing.post = Some(post);
				Ok(())
			}
			Some(existing) if existing.op == Update => {
				// Coalesce multiple updates - keep original
				// pre, update post
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				return_error!(cannot_update_deleted_schema(
					&post.name
				));
			}
			None => {
				self.changes.change_schema_def(
					post.id,
					Change {
						pre: Some(pre),
						post: Some(post),
						op: Update,
					},
				);
				Ok(())
			}
		}
	}

	fn track_schema_def_deleted(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()> {
		match self.changes.schema_def.get_mut(&schema.id) {
			Some(existing) if existing.op == Create => {
				// Created and deleted in same transaction -
				// remove entirely
				self.changes.schema_def.remove(&schema.id);
				// Remove from operation log
				self.changes.log.retain(
					|op| !matches!(op, Schema { id, .. } if *id == schema.id),
				);
				Ok(())
			}
			Some(existing) if existing.op == Update => {
				// Convert update to delete, keep original
				// pre-state
				existing.post = None;
				existing.op = Delete;
				// Update operation log
				if let Some(op) =
					self.changes.log.iter_mut().rev().find(
						|op| matches!(op, Schema { id, .. } if *id == schema.id),
					) {
					*op = Schema {
						id: schema.id,
						op: Delete,
					};
				}
				Ok(())
			}
			Some(_) => {
				return_error!(
					cannot_delete_already_deleted_schema(
						&schema.name
					)
				);
			}
			None => {
				self.changes.change_schema_def(
					schema.id,
					Change {
						pre: Some(schema.clone()),
						post: None,
						op: Delete,
					},
				);
				Ok(())
			}
		}
	}

	fn track_table_def_created(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		if self.changes.table_def.contains_key(&table.id) {
			let schema = self.get_schema(table.schema)?;
			return_error!(table_already_pending_in_transaction(
				&schema.name,
				&table.name
			));
		}

		self.changes.change_table_def(
			table.id,
			Change {
				pre: None,
				post: Some(table),
				op: Create,
			},
		);

		Ok(())
	}

	fn track_table_def_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> crate::Result<()> {
		match self.changes.table_def.get_mut(&post.id) {
			Some(existing) if existing.op == Create => {
				existing.post = Some(post);
				Ok(())
			}
			Some(existing) if existing.op == Update => {
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				let schema = self.get_schema(post.schema)?;
				return_error!(cannot_update_deleted_table(
					&schema.name,
					&post.name
				));
			}
			None => {
				self.changes.change_table_def(
					post.id,
					Change {
						pre: Some(pre),
						post: Some(post),
						op: Update,
					},
				);
				Ok(())
			}
		}
	}

	fn track_table_def_deleted(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		match self.changes.table_def.get_mut(&table.id) {
			Some(existing) if existing.op == Create => {
				self.changes.table_def.remove(&table.id);
				self.changes.log.retain(
					|op| !matches!(op, Table { id, .. } if *id == table.id),
				);
				Ok(())
			}
			Some(existing) if existing.op == Update => {
				existing.post = None;
				existing.op = Delete;
				if let Some(op) =
					self.changes.log.iter_mut().rev().find(
						|op| matches!(op, Table { id, .. } if *id == table.id),
					) {
					*op = Table {
						id: table.id,
						op: Delete,
					};
				}
				Ok(())
			}
			Some(_) => {
				let schema = self.get_schema(table.schema)?;
				return_error!(
					cannot_delete_already_deleted_table(
						&schema.name,
						&table.name
					)
				);
			}
			None => {
				self.changes.change_table_def(
					table.id,
					Change {
						pre: Some(table.clone()),
						post: None,
						op: Delete,
					},
				);
				Ok(())
			}
		}
	}

	fn track_view_def_created(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()> {
		if self.changes.view_def.contains_key(&view.id) {
			let schema = self.get_schema(view.schema)?;
			return_error!(view_already_pending_in_transaction(
				&schema.name,
				&view.name
			));
		}

		self.changes.change_view_def(
			view.id,
			Change {
				pre: None,
				post: Some(view),
				op: Create,
			},
		);

		Ok(())
	}

	fn track_view_def_updated(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()> {
		match self.changes.view_def.get_mut(&post.id) {
			Some(existing) if existing.op == Create => {
				existing.post = Some(post);
				Ok(())
			}
			Some(existing) if existing.op == Update => {
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				let schema = self.get_schema(post.schema)?;
				return_error!(cannot_update_deleted_view(
					&schema.name,
					&post.name
				));
			}
			None => {
				self.changes.change_view_def(
					post.id,
					Change {
						pre: Some(pre),
						post: Some(post),
						op: Update,
					},
				);
				Ok(())
			}
		}
	}

	fn track_view_def_deleted(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()> {
		match self.changes.view_def.get_mut(&view.id) {
			Some(existing) if existing.op == Create => {
				self.changes.view_def.remove(&view.id);
				self.changes.log.retain(
					|op| !matches!(op, View { id, .. } if *id == view.id),
				);
				Ok(())
			}
			Some(existing) if existing.op == Update => {
				existing.post = None;
				existing.op = Delete;
				if let Some(op) =
					self.changes.log.iter_mut().rev().find(
						|op| matches!(op, View { id, .. } if *id == view.id),
					) {
					*op = View {
						id: view.id,
						op: Delete,
					};
				}
				Ok(())
			}
			Some(_) => {
				let schema = self.get_schema(view.schema)?;
				return_error!(
					cannot_delete_already_deleted_view(
						&schema.name,
						&view.name
					)
				);
			}
			None => {
				self.changes.change_view_def(
					view.id,
					Change {
						pre: Some(view.clone()),
						post: None,
						op: Delete,
					},
				);
				Ok(())
			}
		}
	}
}

// Implement the blanket CatalogTransaction trait
impl<T: Transaction> CatalogTransaction for StandardCommandTransaction<T> {}
