// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperationType::Update;
use reifydb_catalog::{CatalogStore, table::TableToCreate};
use reifydb_core::{
	diagnostic::catalog::{
		cannot_delete_already_deleted_table,
		cannot_update_deleted_table,
		table_already_pending_in_transaction,
	},
	interface::{
		Change, CommandTransaction, Operation, OperationType,
		OperationType::{Create, Delete},
		TableDef, TableId, Transaction,
		interceptor::TableDefInterceptor,
	},
	return_error,
};

use crate::{
	StandardCommandTransaction, transaction::operation::get_schema_name,
};

pub(crate) trait TableDefCreateOperation {
	fn create_table_def(
		&mut self,
		table: TableToCreate,
	) -> crate::Result<TableDef>;
}

impl<T: Transaction> TableDefCreateOperation for StandardCommandTransaction<T> {
	fn create_table_def(
		&mut self,
		table: TableToCreate,
	) -> crate::Result<TableDef> {
		let result = CatalogStore::create_table(self, table)?;
		track_created(self, result.clone())?;
		TableDefInterceptor::post_create(self, &result)?;
		Ok(result)
	}
}

fn track_created<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	table: TableDef,
) -> crate::Result<()> {
	let changes = txn.get_changes_mut();

	if changes.table_def.contains_key(&table.id) {
		return_error!(table_already_pending_in_transaction(
			&get_schema_name(txn, table.schema)?,
			&table.name
		));
	}

	changes.change_table_def(
		table.id,
		Change {
			pre: None,
			post: Some(table.clone()),
			op: Create,
		},
	);

	Ok(())
}

pub(crate) trait TableDefUpdateOperation {
	fn update_table_def(
		&mut self,
		table_id: TableId,
	) -> crate::Result<TableDef>;
}

impl<T: Transaction> TableDefUpdateOperation for StandardCommandTransaction<T> {
	fn update_table_def(
		&mut self,
		_table_id: TableId,
	) -> crate::Result<TableDef> {
		// let pre = CatalogStore::get_table(self, table_id)?;
		//
		// TableDefInterceptor::pre_update(self, &pre)?;
		// let post = TableDef {
		// 	id: table_id,
		// 	schema: pre.schema,
		// 	name: updates.table,
		// 	columns: vec![],
		// };
		// // FIXME
		// TableDefInterceptor::post_update(self, &pre, &post)?;
		// track_updated(self, pre.clone(), post.clone())?;
		// Ok(post)
		unimplemented!()
	}
}

fn _track_updated<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	pre: TableDef,
	post: TableDef,
) -> crate::Result<()> {
	let changes = txn.get_changes_mut();
	match changes.table_def.get_mut(&post.id) {
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
			return_error!(cannot_update_deleted_table(
				&get_schema_name(txn, post.schema)?,
				&post.name
			));
		}
		None => {
			changes.change_table_def(
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

pub(crate) trait TableDefDeleteOperation {
	fn delete_table_def(&mut self, table_id: TableId) -> crate::Result<()>;
}

impl<T: Transaction> TableDefDeleteOperation for StandardCommandTransaction<T> {
	fn delete_table_def(
		&mut self,
		_table_id: TableId,
	) -> crate::Result<()> {
		// let table = CatalogStore::get_table(self, table_id)?;
		//
		// TableDefInterceptor::pre_delete(self, &table)?;
		// // CatalogStore::delete_table(self, table_id)?;
		// track_deleted(self, table.clone())?;
		// Ok(())
		unimplemented!()
	}
}

fn _track_deleted<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	table: TableDef,
) -> crate::Result<()> {
	let changes = txn.get_changes_mut();
	match changes.table_def.get_mut(&table.id) {
		Some(existing) if existing.op == Create => {
			// Created and deleted in same transaction - remove
			// entirely
			changes.table_def.remove(&table.id);
			// Remove from operation log
			changes.log.retain(
				|op| !matches!(op, Operation::Table { id, .. } if *id == table.id),
			);
			Ok(())
		}
		Some(existing) if existing.op == Update => {
			// Convert update to delete, keep original pre state
			existing.post = None;
			existing.op = Delete;
			// Update operation log
			if let Some(op) = changes.log.iter_mut().rev().find(
				|op| matches!(op, Operation::Table { id, .. } if *id == table.id),
			) {
				*op = Operation::Table {
					id: table.id,
					op: Delete,
				};
			}
			Ok(())
		}
		Some(_) => {
			return_error!(cannot_delete_already_deleted_table(
				&get_schema_name(txn, table.schema)?,
				&table.name
			));
		}
		None => {
			changes.change_table_def(
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
