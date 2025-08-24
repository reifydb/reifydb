// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{Change, Operation, OperationType, TransactionalChanges};
use crate::interface::SchemaDef;
use crate::result::error::diagnostic::catalog::{
	schema_already_pending_in_transaction,
	cannot_update_deleted_schema,
	cannot_delete_already_deleted_schema,
};
use crate::return_error;

impl TransactionalChanges {
	/// Add a schema definition creation
	pub fn add_schema_def_create(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()> {
		if self.schema_def.contains_key(&schema.id) {
			return_error!(schema_already_pending_in_transaction(
				&schema.name
			));
		}

		self.change_schema_def(
			schema.id,
			Change {
				pre: None,
				post: Some(schema.clone()),
				op: OperationType::Create,
			},
			OperationType::Create,
		);

		Ok(())
	}

	/// Add a schema definition update
	pub fn add_schema_def_update(
		&mut self,
		pre: SchemaDef,
		post: SchemaDef,
	) -> crate::Result<()> {
		match self.schema_def.get_mut(&post.id) {
			Some(existing)
				if existing.op
					== OperationType::Create =>
			{
				// Coalesce with create - just update the "post" state
				existing.post = Some(post);
				Ok(())
			}
			Some(existing)
				if existing.op
					== OperationType::Update =>
			{
				// Coalesce multiple updates - keep original "pre", update "post"
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				return_error!(cannot_update_deleted_schema(
					&post.name
				));
			}
			None => {
				self.change_schema_def(
					post.id,
					Change {
						pre: Some(pre),
						post: Some(post.clone()),
						op: OperationType::Update,
					},
					OperationType::Update,
				);

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
			Some(existing)
				if existing.op
					== OperationType::Create =>
			{
				// Created and deleted in same transaction - remove entirely
				self.schema_def.remove(&schema.id);
				// Remove from operation log
				self.log.retain(
					|op| !matches!(op, Operation::Schema { id, .. } if *id == schema.id),
				);
				Ok(())
			}
			Some(existing)
				if existing.op
					== OperationType::Update =>
			{
				// Convert update to delete, keep original pre state
				existing.post = None;
				existing.op = OperationType::Delete;
				// Update operation log
				if let Some(op) =
					self.log.iter_mut().rev().find(
						|op| matches!(op, Operation::Schema { id, .. } if *id == schema.id),
					) {
					*op = Operation::Schema {
						id: schema.id,
						op: OperationType::Delete,
					};
				}
				Ok(())
			}
			Some(_) => {
				return_error!(cannot_delete_already_deleted_schema(
					&schema.name
				));
			}
			None => {
				self.change_schema_def(
					schema.id,
					Change {
						pre: Some(schema.clone()),
						post: None,
						op: OperationType::Delete,
					},
					OperationType::Delete,
				);

				Ok(())
			}
		}
	}
}
