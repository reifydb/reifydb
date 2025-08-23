// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{Change, Operation, OperationType, TransactionalChanges};
use crate::interface::TableDef;
use crate::result::error::diagnostic::catalog::{
	table_already_pending_in_transaction,
	cannot_update_deleted_table,
	cannot_delete_already_deleted_table,
};
use crate::return_error;

impl TransactionalChanges {
	/// Add a table definition creation
	pub fn add_table_def_create(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		if self.table_def.contains_key(&table.id) {
			return_error!(table_already_pending_in_transaction(
				&self.get_schema_name(table.schema)?,
				&table.name
			));
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
			Some(_) => {
				return_error!(cannot_update_deleted_table(
					&self.get_schema_name(post.schema)?,
					&post.name
				));
			}
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
			Some(_) => {
				return_error!(cannot_delete_already_deleted_table(
					&self.get_schema_name(table.schema)?,
					&table.name
				));
			}
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
}