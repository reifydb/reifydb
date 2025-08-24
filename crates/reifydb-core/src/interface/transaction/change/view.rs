// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{Change, Operation, OperationType, TransactionalChanges};
use crate::interface::ViewDef;
use crate::result::error::diagnostic::catalog::{
	view_already_pending_in_transaction,
	cannot_update_deleted_view,
	cannot_delete_already_deleted_view,
};
use crate::return_error;

impl TransactionalChanges {
	/// Add a view definition creation
	pub fn add_view_def_create(&mut self, view: ViewDef) -> crate::Result<()> {
		if self.view_def.contains_key(&view.id) {
			return_error!(view_already_pending_in_transaction(
				&self.get_schema_name(view.schema)?,
				&view.name
			));
		}

		self.change_view_def(
			view.id,
			Change {
				pre: None,
				post: Some(view.clone()),
				op: OperationType::Create,
			},
			OperationType::Create,
		);

		Ok(())
	}

	/// Add a view definition update
	pub fn add_view_def_update(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()> {
		match self.view_def.get_mut(&post.id) {
			Some(existing) if existing.op == OperationType::Create => {
				// Coalesce with create - just update the "post" state
				existing.post = Some(post);
				Ok(())
			}
			Some(existing) if existing.op == OperationType::Update => {
				// Coalesce multiple updates - keep original "pre", update "post"
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				return_error!(cannot_update_deleted_view(
					&self.get_schema_name(post.schema)?,
					&post.name
				));
			}
			None => {
				self.change_view_def(
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

	/// Add a view definition deletion
	pub fn add_view_def_delete(&mut self, view: ViewDef) -> crate::Result<()> {
		match self.view_def.get_mut(&view.id) {
			Some(existing) if existing.op == OperationType::Create => {
				// Created and deleted in same transaction - remove entirely
				self.view_def.remove(&view.id);
				// Remove from operation log
				self.log.retain(|op| {
					!matches!(op, Operation::View { id, .. } if *id == view.id)
				});
				Ok(())
			}
			Some(existing) if existing.op == OperationType::Update => {
				// Convert update to delete, keep original pre state
				existing.post = None;
				existing.op = OperationType::Delete;
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
			Some(_) => {
				return_error!(cannot_delete_already_deleted_view(
					&self.get_schema_name(view.schema)?,
					&view.name
				));
			}
			None => {
				self.change_view_def(
					view.id,
					Change {
						pre: Some(view.clone()),
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