// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperationType::Update;
use reifydb_catalog::{CatalogStore, view::ViewToCreate};
use reifydb_core::{
	diagnostic::catalog::{
		cannot_delete_already_deleted_view, cannot_update_deleted_view,
		view_already_pending_in_transaction,
	},
	interface::{
		Change, CommandTransaction, Operation, OperationType,
		OperationType::{Create, Delete},
		Transaction, ViewDef, ViewId,
		interceptor::ViewDefInterceptor,
	},
	return_error,
};

use crate::{
	StandardCommandTransaction, transaction::operation::get_schema_name,
};

pub(crate) trait ViewDefCreateOperation {
	fn create_view_def(
		&mut self,
		view: ViewToCreate,
	) -> crate::Result<ViewDef>;
}

impl<T: Transaction> ViewDefCreateOperation for StandardCommandTransaction<T> {
	fn create_view_def(
		&mut self,
		view: ViewToCreate,
	) -> crate::Result<ViewDef> {
		let result = CatalogStore::create_deferred_view(self, view)?;
		track_created(self, result.clone())?;
		ViewDefInterceptor::post_create(self, &result)?;
		Ok(result)
	}
}

fn track_created<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	view: ViewDef,
) -> crate::Result<()> {
	let changes = txn.get_changes_mut();

	if changes.view_def.contains_key(&view.id) {
		return_error!(view_already_pending_in_transaction(
			&get_schema_name(txn, view.schema)?,
			&view.name
		));
	}

	changes.change_view_def(
		view.id,
		Change {
			pre: None,
			post: Some(view.clone()),
			op: Create,
		},
	);

	Ok(())
}

pub(crate) trait ViewDefUpdateOperation {
	fn update_view_def(
		&mut self,
		view_id: ViewId,
	) -> crate::Result<ViewDef>;
}

impl<T: Transaction> ViewDefUpdateOperation for StandardCommandTransaction<T> {
	fn update_view_def(
		&mut self,
		_view_id: ViewId,
	) -> crate::Result<ViewDef> {
		// let pre = CatalogStore::get_view(self, view_id)?;
		//
		// ViewDefInterceptor::pre_update(self, &pre)?;
		// let post = ViewDef {
		// 	id: view_id,
		// 	schema: pre.schema,
		// 	name: updates.view,
		// 	kind: ViewKind::Deferred,
		// 	columns: vec![],
		// };
		// // FIXME
		// ViewDefInterceptor::post_update(self, &pre, &post)?;
		// track_updated(self, pre.clone(), post.clone())?;
		// Ok(post)
		unimplemented!()
	}
}

fn _track_updated<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	pre: ViewDef,
	post: ViewDef,
) -> crate::Result<()> {
	let changes = txn.get_changes_mut();
	match changes.view_def.get_mut(&post.id) {
		Some(existing) if existing.op == Create => {
			// Coalesce with create - just update the "post" state
			existing.post = Some(post);
			Ok(())
		}
		Some(existing) if existing.op == Update => {
			// Coalesce multiple updates - keep original "pre",
			// update "post"
			existing.post = Some(post);
			Ok(())
		}
		Some(_) => {
			return_error!(cannot_update_deleted_view(
				&get_schema_name(txn, post.schema)?,
				&post.name
			));
		}
		None => {
			changes.change_view_def(
				post.id,
				Change {
					pre: Some(pre),
					post: Some(post.clone()),
					op: Update,
				},
			);
			Ok(())
		}
	}
}

pub(crate) trait ViewDefDeleteOperation {
	fn delete_view_def(&mut self, view_id: ViewId) -> crate::Result<()>;
}

impl<T: Transaction> ViewDefDeleteOperation for StandardCommandTransaction<T> {
	fn delete_view_def(&mut self, _view_id: ViewId) -> crate::Result<()> {
		// let view = CatalogStore::get_view(self, view_id)?;
		//
		// ViewDefInterceptor::pre_delete(self, &view)?;
		// // CatalogStore::delete_view(self, view_id)?;
		// track_deleted(self, view.clone())?;
		// Ok(())
		unimplemented!()
	}
}

fn _track_deleted<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	view: ViewDef,
) -> crate::Result<()> {
	let changes = txn.get_changes_mut();
	match changes.view_def.get_mut(&view.id) {
		Some(existing) if existing.op == Create => {
			// Created and deleted in same transaction - remove
			// entirely
			changes.view_def.remove(&view.id);
			// Remove from operation log
			changes.log.retain(
				|op| !matches!(op, Operation::View { id, .. } if *id == view.id),
			);
			Ok(())
		}
		Some(existing) if existing.op == Update => {
			// Convert update to delete, keep original pre state
			existing.post = None;
			existing.op = Delete;
			// Update operation log
			if let Some(op) = changes.log.iter_mut().rev().find(
				|op| matches!(op, Operation::View { id, .. } if *id == view.id),
			) {
				*op = Operation::View {
					id: view.id,
					op: Delete,
				};
			}
			Ok(())
		}
		Some(_) => {
			return_error!(cannot_delete_already_deleted_view(
				&get_schema_name(txn, view.schema)?,
				&view.name
			));
		}
		None => {
			changes.change_view_def(
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
