// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::StandardCommandTransaction;
use reifydb_catalog::schema::SchemaToCreate;
use reifydb_catalog::CatalogStore;
use reifydb_core::diagnostic::catalog::{
	cannot_delete_already_deleted_schema, cannot_update_deleted_schema,
	schema_already_pending_in_transaction,
};
use reifydb_core::interface::interceptor::SchemaDefInterceptor;
use reifydb_core::interface::OperationType::{Create, Delete, Update};
use reifydb_core::interface::{
	Change, CommandTransaction, Operation, OperationType, SchemaDef,
	SchemaId, Transaction, TransactionalChanges,
};
use reifydb_core::return_error;

pub(crate) trait SchemaDefCreateOperation {
	fn create_schema_def(
		&mut self,
		schema: SchemaToCreate,
	) -> crate::Result<SchemaDef>;
}

impl<T: Transaction> SchemaDefCreateOperation
	for StandardCommandTransaction<T>
{
	fn create_schema_def(
		&mut self,
		schema: SchemaToCreate,
	) -> crate::Result<SchemaDef> {
		let result = CatalogStore::create_schema(self, schema)?;
		track_created(self.get_changes_mut(), result.clone())?;
		<Self as SchemaDefInterceptor<Self>>::post_create(
			self, &result,
		)?;
		Ok(result)
	}
}

fn track_created(
	changes: &mut TransactionalChanges,
	schema: SchemaDef,
) -> crate::Result<()> {
	if changes.schema_def.contains_key(&schema.id) {
		return_error!(schema_already_pending_in_transaction(
			&schema.name
		));
	}

	changes.change_schema_def(
		schema.id,
		Change {
			pre: None,
			post: Some(schema),
			op: Create,
		},
	);

	Ok(())
}

pub(crate) trait SchemaDefUpdateOperation {
	fn update_schema_def(
		&mut self,
		schema_id: SchemaId,
	) -> crate::Result<SchemaDef>;
}

impl<T: Transaction> SchemaDefUpdateOperation
	for StandardCommandTransaction<T>
{
	fn update_schema_def(
		&mut self,
		schema_id: SchemaId,
	) -> crate::Result<SchemaDef> {
		// // Get the current state before update
		// let pre = CatalogStore::get_schema(self, schema_id)?;
		//
		// // Apply the update (you'll need to implement update_schema in CatalogStore)
		// // For now, we'll assume it exists or needs to be created
		// // let post = CatalogStore::update_schema(self, schema_id, updates)?;
		// // SchemaDefInterceptor::pre_update(self, &pre)?;
		//
		// // For now, creating a placeholder - you'll need to implement the actual update
		// let post = SchemaDef {
		// 	id: schema_id,
		// 	name: updates.name,
		// };
		//
		// track_updated(
		// 	self.get_changes_mut(),
		// 	pre.clone(),
		// 	post.clone(),
		// )?;
		// // SchemaDefInterceptor::post_update(self, &pre, &post)?;
		//
		// Ok(post)
		unimplemented!()
	}
}

fn _track_updated(
	changes: &mut TransactionalChanges,
	pre: SchemaDef,
	post: SchemaDef,
) -> crate::Result<()> {
	match changes.schema_def.get_mut(&post.id) {
		Some(existing) if existing.op == Create => {
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
			return_error!(cannot_update_deleted_schema(&post.name));
		}
		None => {
			changes.change_schema_def(
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

pub(crate) trait SchemaDefDeleteOperation {
	fn delete_schema_def(
		&mut self,
		schema_id: SchemaId,
	) -> crate::Result<()>;
}

impl<T: Transaction> SchemaDefDeleteOperation
	for StandardCommandTransaction<T>
{
	fn delete_schema_def(
		&mut self,
		schema_id: SchemaId,
	) -> crate::Result<()> {
		// let schema = CatalogStore::get_schema(self, schema_id)?;
		//
		// SchemaDefInterceptor::pre_delete(self, &schema)?;
		// // CatalogStore::delete_schema(self, schema_id)?;
		// track_deleted(self.get_changes_mut(), schema.clone())?;
		// Ok(())
		unimplemented!()
	}
}

fn _track_deleted(
	changes: &mut TransactionalChanges,
	schema: SchemaDef,
) -> crate::Result<()> {
	match changes.schema_def.get_mut(&schema.id) {
		Some(existing) if existing.op == OperationType::Create => {
			// Created and deleted in same transaction - remove entirely
			changes.schema_def.remove(&schema.id);
			// Remove from operation log
			changes.log.retain(
				|op| !matches!(op, Operation::Schema { id, .. } if *id == schema.id),
			);
			Ok(())
		}
		Some(existing) if existing.op == OperationType::Update => {
			// Convert update to delete, keep original pre state
			existing.post = None;
			existing.op = Delete;
			// Update operation log
			if let Some(op) = changes.log.iter_mut().rev().find(
				|op| matches!(op, Operation::Schema { id, .. } if *id == schema.id),
			) {
				*op = Operation::Schema {
					id: schema.id,
					op: Delete,
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
			changes.change_schema_def(
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
