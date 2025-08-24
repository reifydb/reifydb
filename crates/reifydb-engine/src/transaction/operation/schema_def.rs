// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::StandardCommandTransaction;
use reifydb_catalog::schema::SchemaToCreate;
use reifydb_catalog::CatalogStore;
use reifydb_core::catalog::{Change, OperationType, TransactionalChanges};
use reifydb_core::diagnostic::catalog::schema_already_pending_in_transaction;
use reifydb_core::interface::{CommandTransaction, SchemaDef, Transaction};
use reifydb_core::return_error;
use OperationType::Create;

pub(crate) trait SchemaDefCreateOperation {
	fn create_schema_def(
		&mut self,
		schema: SchemaToCreate,
	) -> crate::Result<()>;
}

impl<T: Transaction> SchemaDefCreateOperation
	for StandardCommandTransaction<T>
{
	fn create_schema_def(
		&mut self,
		schema: SchemaToCreate,
	) -> crate::Result<()> {
		// add to transactional change
		let schema = CatalogStore::create_schema(self, schema)?;
		track_create_change(self.get_changes_mut(), schema)?;
		// Catalog::create_schema - storage
		// SchemaDefPostCreateInterceptor
		// SchemaDefHook

		todo!()
	}
}

fn track_create_change(
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
		Create,
	);

	Ok(())
}
