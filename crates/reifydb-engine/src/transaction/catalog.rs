// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogSchemaDefOperations, CatalogTransaction,
	CatalogTransactionOperations, MaterializedCatalog,
};
use reifydb_core::{
	Version,
	diagnostic::catalog::{
		schema_already_pending_in_transaction,
		table_already_pending_in_transaction,
		view_already_pending_in_transaction,
	},
	interface::{
		Change,
		OperationType::{Create, Delete, Update},
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
		// Check if schema was already created in this transaction
		let already_created =
			self.changes.schema_def.iter().any(|change| {
				change.post
					.as_ref()
					.map(|s| s.id == schema.id)
					.unwrap_or(false) && change.op == Create
			});

		if already_created {
			return_error!(schema_already_pending_in_transaction(
				&schema.name
			));
		}

		self.changes.add_schema_def_change(Change {
			pre: None,
			post: Some(schema),
			op: Create,
		});

		Ok(())
	}

	fn track_schema_def_updated(
		&mut self,
		pre: SchemaDef,
		post: SchemaDef,
	) -> crate::Result<()> {
		debug_assert_eq!(
			pre.id, post.id,
			"Schema ID must remain the same during update"
		);

		self.changes.add_schema_def_change(Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		});

		Ok(())
	}

	fn track_schema_def_deleted(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()> {
		self.changes.add_schema_def_change(Change {
			pre: Some(schema),
			post: None,
			op: Delete,
		});

		Ok(())
	}

	fn track_table_def_created(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		// Check if table was already created in this transaction
		let already_created =
			self.changes.table_def.iter().any(|change| {
				change.post
					.as_ref()
					.map(|t| t.id == table.id)
					.unwrap_or(false) && change.op == Create
			});

		if already_created {
			let schema = self.get_schema(table.schema)?;
			return_error!(table_already_pending_in_transaction(
				&schema.name,
				&table.name
			));
		}

		self.changes.add_table_def_change(Change {
			pre: None,
			post: Some(table),
			op: Create,
		});

		Ok(())
	}

	fn track_table_def_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> crate::Result<()> {
		debug_assert_eq!(
			pre.id, post.id,
			"Table ID must remain the same during update"
		);
		debug_assert_eq!(
			pre.schema, post.schema,
			"Table schema must remain the same during update"
		);

		self.changes.add_table_def_change(Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		});

		Ok(())
	}

	fn track_table_def_deleted(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		self.changes.add_table_def_change(Change {
			pre: Some(table),
			post: None,
			op: Delete,
		});

		Ok(())
	}

	fn track_view_def_created(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()> {
		// Check if view was already created in this transaction
		let already_created =
			self.changes.view_def.iter().any(|change| {
				change.post
					.as_ref()
					.map(|v| v.id == view.id)
					.unwrap_or(false) && change.op == Create
			});

		if already_created {
			let schema = self.get_schema(view.schema)?;
			return_error!(view_already_pending_in_transaction(
				&schema.name,
				&view.name
			));
		}

		self.changes.add_view_def_change(Change {
			pre: None,
			post: Some(view),
			op: Create,
		});

		Ok(())
	}

	fn track_view_def_updated(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()> {
		debug_assert_eq!(
			pre.id, post.id,
			"View ID must remain the same during update"
		);
		debug_assert_eq!(
			pre.schema, post.schema,
			"View schema must remain the same during update"
		);

		self.changes.add_view_def_change(Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		});

		Ok(())
	}

	fn track_view_def_deleted(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()> {
		self.changes.add_view_def_change(Change {
			pre: Some(view),
			post: None,
			op: Delete,
		});

		Ok(())
	}
}

// Implement the blanket CatalogTransaction trait
impl<T: Transaction> CatalogTransaction for StandardCommandTransaction<T> {}

#[cfg(test)]
mod tests {
	use reifydb_catalog::CatalogTransactionOperations;
	use reifydb_core::interface::{
		Operation,
		OperationType::{Create, Delete, Update},
		SchemaDef, SchemaId, TableDef, TableId, ViewDef, ViewId,
		ViewKind,
	};

	use crate::test_utils::create_test_command_transaction;

	// Helper functions to create test definitions
	fn test_schema_def(id: u64, name: &str) -> SchemaDef {
		SchemaDef {
			id: SchemaId(id),
			name: name.to_string(),
		}
	}

	fn test_table_def(id: u64, schema_id: u64, name: &str) -> TableDef {
		TableDef {
			id: TableId(id),
			schema: SchemaId(schema_id),
			name: name.to_string(),
			columns: vec![],
		}
	}

	fn test_view_def(id: u64, schema_id: u64, name: &str) -> ViewDef {
		ViewDef {
			id: ViewId(id),
			schema: SchemaId(schema_id),
			name: name.to_string(),
			columns: vec![],
			kind: ViewKind::Deferred,
		}
	}

	mod track_schema_def_created {
		use super::*;

		#[test]
		fn test_successful_creation() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");

			let result =
				txn.track_schema_def_created(schema.clone());
			assert!(result.is_ok());

			// Verify the change was recorded in the Vec
			assert_eq!(txn.changes.schema_def.len(), 1);
			let change = &txn.changes.schema_def[0];
			assert!(change.pre.is_none());
			assert_eq!(
				change.post.as_ref().unwrap().name,
				"test_schema"
			);
			assert_eq!(change.op, Create);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Schema {
					id,
					op,
				} if *id == schema.id && *op == Create => {}
				_ => panic!(
					"Expected Schema operation with Create"
				),
			}
		}

		#[test]
		fn test_error_when_already_created() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");

			// First creation should succeed
			txn.track_schema_def_created(schema.clone()).unwrap();

			// Second creation should fail
			let result = txn.track_schema_def_created(schema);
			assert!(result.is_err());
			let err = result.unwrap_err();
			assert_eq!(err.diagnostic().code, "CA_011");
		}
	}

	mod track_schema_def_updated {
		use super::*;

		#[test]
		fn test_multiple_updates_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema_v1 = test_schema_def(1, "schema_v1");
			let schema_v2 = test_schema_def(1, "schema_v2");
			let schema_v3 = test_schema_def(1, "schema_v3");

			// First update
			txn.track_schema_def_updated(
				schema_v1.clone(),
				schema_v2.clone(),
			)
			.unwrap();

			// Should have one change
			assert_eq!(txn.changes.schema_def.len(), 1);
			assert_eq!(
				txn.changes.schema_def[0]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"schema_v1"
			);
			assert_eq!(
				txn.changes.schema_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"schema_v2"
			);
			assert_eq!(txn.changes.schema_def[0].op, Update);

			// Second update - should NOT coalesce, just add another
			// change
			txn.track_schema_def_updated(
				schema_v2,
				schema_v3.clone(),
			)
			.unwrap();

			// Should now have TWO changes (no coalescing)
			assert_eq!(txn.changes.schema_def.len(), 2);

			// First update unchanged
			assert_eq!(
				txn.changes.schema_def[0]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"schema_v1"
			);
			assert_eq!(
				txn.changes.schema_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"schema_v2"
			);

			// Second update recorded separately
			assert_eq!(
				txn.changes.schema_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"schema_v2"
			);
			assert_eq!(
				txn.changes.schema_def[1]
					.post
					.as_ref()
					.unwrap()
					.name,
				"schema_v3"
			);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[test]
		fn test_create_then_update_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema_v1 = test_schema_def(1, "schema_v1");
			let schema_v2 = test_schema_def(1, "schema_v2");

			// First track creation
			txn.track_schema_def_created(schema_v1.clone())
				.unwrap();
			assert_eq!(txn.changes.schema_def.len(), 1);
			assert_eq!(txn.changes.schema_def[0].op, Create);

			// Then track update - should NOT coalesce
			txn.track_schema_def_updated(
				schema_v1,
				schema_v2.clone(),
			)
			.unwrap();

			// Should have TWO changes now
			assert_eq!(txn.changes.schema_def.len(), 2);

			// First is still Create
			assert_eq!(txn.changes.schema_def[0].op, Create);
			assert_eq!(
				txn.changes.schema_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"schema_v1"
			);

			// Second is Update
			assert_eq!(txn.changes.schema_def[1].op, Update);
			assert_eq!(
				txn.changes.schema_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"schema_v1"
			);
			assert_eq!(
				txn.changes.schema_def[1]
					.post
					.as_ref()
					.unwrap()
					.name,
				"schema_v2"
			);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[test]
		fn test_normal_update() {
			let mut txn = create_test_command_transaction();
			let schema_v1 = test_schema_def(1, "schema_v1");
			let schema_v2 = test_schema_def(1, "schema_v2");

			let result = txn.track_schema_def_updated(
				schema_v1.clone(),
				schema_v2.clone(),
			);
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.schema_def.len(), 1);
			let change = &txn.changes.schema_def[0];
			assert_eq!(
				change.pre.as_ref().unwrap().name,
				"schema_v1"
			);
			assert_eq!(
				change.post.as_ref().unwrap().name,
				"schema_v2"
			);
			assert_eq!(change.op, Update);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Schema {
					id,
					op,
				} if *id == SchemaId(1) && *op == Update => {}
				_ => panic!(
					"Expected Schema operation with Update"
				),
			}
		}
	}

	mod track_schema_def_deleted {
		use super::*;

		#[test]
		fn test_delete_after_create_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");

			// First track creation
			txn.track_schema_def_created(schema.clone()).unwrap();
			assert_eq!(txn.changes.log.len(), 1);
			assert_eq!(txn.changes.schema_def.len(), 1);

			// Then track deletion - should NOT remove, just add
			// another change
			let result =
				txn.track_schema_def_deleted(schema.clone());
			assert!(result.is_ok());

			// Should have TWO changes now (no coalescing)
			assert_eq!(txn.changes.schema_def.len(), 2);

			// First is Create
			assert_eq!(txn.changes.schema_def[0].op, Create);

			// Second is Delete
			assert_eq!(txn.changes.schema_def[1].op, Delete);
			assert_eq!(
				txn.changes.schema_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"test_schema"
			);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[test]
		fn test_delete_after_update_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema_v1 = test_schema_def(1, "schema_v1");
			let schema_v2 = test_schema_def(1, "schema_v2");

			// First track update
			txn.track_schema_def_updated(
				schema_v1.clone(),
				schema_v2.clone(),
			)
			.unwrap();
			assert_eq!(txn.changes.schema_def.len(), 1);

			// Then track deletion
			let result = txn.track_schema_def_deleted(schema_v2);
			assert!(result.is_ok());

			// Should have TWO changes (no coalescing)
			assert_eq!(txn.changes.schema_def.len(), 2);

			// First is Update
			assert_eq!(txn.changes.schema_def[0].op, Update);

			// Second is Delete
			assert_eq!(txn.changes.schema_def[1].op, Delete);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[test]
		fn test_normal_delete() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");

			let result =
				txn.track_schema_def_deleted(schema.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.schema_def.len(), 1);
			let change = &txn.changes.schema_def[0];
			assert_eq!(
				change.pre.as_ref().unwrap().name,
				"test_schema"
			);
			assert!(change.post.is_none());
			assert_eq!(change.op, Delete);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Schema {
					id,
					op,
				} if *id == schema.id && *op == Delete => {}
				_ => panic!(
					"Expected Schema operation with Delete"
				),
			}
		}
	}

	mod track_table_def_created {
		use super::*;

		#[test]
		fn test_successful_creation() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema.clone()).unwrap();

			let table = test_table_def(1, 1, "test_table");
			let result = txn.track_table_def_created(table.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.table_def.len(), 1);
			let change = &txn.changes.table_def[0];
			assert!(change.pre.is_none());
			assert_eq!(
				change.post.as_ref().unwrap().name,
				"test_table"
			);
			assert_eq!(change.op, Create);

			// Verify operation was logged (schema + table)
			assert_eq!(txn.changes.log.len(), 2);
			match &txn.changes.log[1] {
				Operation::Table {
					id,
					op,
				} if *id == table.id && *op == Create => {}
				_ => panic!(
					"Expected Table operation with Create"
				),
			}
		}

		#[test]
		fn test_error_when_already_created() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema).unwrap();

			let table = test_table_def(1, 1, "test_table");

			// First creation should succeed
			txn.track_table_def_created(table.clone()).unwrap();

			// Second creation should fail
			let result = txn.track_table_def_created(table);
			assert!(result.is_err());
			let err = result.unwrap_err();
			assert_eq!(err.diagnostic().code, "CA_012");
		}
	}

	mod track_table_def_updated {
		use super::*;

		#[test]
		fn test_multiple_updates_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let table_v1 = test_table_def(1, 1, "table_v1");
			let table_v2 = test_table_def(1, 1, "table_v2");
			let table_v3 = test_table_def(1, 1, "table_v3");

			// First update
			txn.track_table_def_updated(
				table_v1.clone(),
				table_v2.clone(),
			)
			.unwrap();

			// Should have one change
			assert_eq!(txn.changes.table_def.len(), 1);
			assert_eq!(
				txn.changes.table_def[0]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"table_v1"
			);
			assert_eq!(
				txn.changes.table_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"table_v2"
			);
			assert_eq!(txn.changes.table_def[0].op, Update);

			// Second update - should NOT coalesce
			txn.track_table_def_updated(table_v2, table_v3.clone())
				.unwrap();

			// Should now have TWO changes
			assert_eq!(txn.changes.table_def.len(), 2);

			// First update unchanged
			assert_eq!(
				txn.changes.table_def[0]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"table_v1"
			);
			assert_eq!(
				txn.changes.table_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"table_v2"
			);

			// Second update recorded separately
			assert_eq!(
				txn.changes.table_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"table_v2"
			);
			assert_eq!(
				txn.changes.table_def[1]
					.post
					.as_ref()
					.unwrap()
					.name,
				"table_v3"
			);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[test]
		fn test_create_then_update_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema).unwrap();

			let table_v1 = test_table_def(1, 1, "table_v1");
			let table_v2 = test_table_def(1, 1, "table_v2");

			// First track creation
			txn.track_table_def_created(table_v1.clone()).unwrap();
			assert_eq!(txn.changes.table_def.len(), 1);
			assert_eq!(txn.changes.table_def[0].op, Create);

			// Then track update - should NOT coalesce
			txn.track_table_def_updated(table_v1, table_v2.clone())
				.unwrap();

			// Should have TWO changes now
			assert_eq!(txn.changes.table_def.len(), 2);

			// First is still Create
			assert_eq!(txn.changes.table_def[0].op, Create);
			assert_eq!(
				txn.changes.table_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"table_v1"
			);

			// Second is Update
			assert_eq!(txn.changes.table_def[1].op, Update);
			assert_eq!(
				txn.changes.table_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"table_v1"
			);
			assert_eq!(
				txn.changes.table_def[1]
					.post
					.as_ref()
					.unwrap()
					.name,
				"table_v2"
			);
		}
	}

	mod track_table_def_deleted {
		use super::*;

		#[test]
		fn test_delete_after_create_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema).unwrap();

			let table = test_table_def(1, 1, "test_table");

			// First track creation
			txn.track_table_def_created(table.clone()).unwrap();
			assert_eq!(txn.changes.table_def.len(), 1);

			// Then track deletion - should NOT remove
			let result = txn.track_table_def_deleted(table.clone());
			assert!(result.is_ok());

			// Should have TWO changes now
			assert_eq!(txn.changes.table_def.len(), 2);

			// First is Create
			assert_eq!(txn.changes.table_def[0].op, Create);

			// Second is Delete
			assert_eq!(txn.changes.table_def[1].op, Delete);
			assert_eq!(
				txn.changes.table_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"test_table"
			);
		}

		#[test]
		fn test_delete_after_update_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let table_v1 = test_table_def(1, 1, "table_v1");
			let table_v2 = test_table_def(1, 1, "table_v2");

			// First track update
			txn.track_table_def_updated(
				table_v1.clone(),
				table_v2.clone(),
			)
			.unwrap();
			assert_eq!(txn.changes.table_def.len(), 1);

			// Then track deletion
			let result = txn.track_table_def_deleted(table_v2);
			assert!(result.is_ok());

			// Should have TWO changes
			assert_eq!(txn.changes.table_def.len(), 2);

			// First is Update
			assert_eq!(txn.changes.table_def[0].op, Update);

			// Second is Delete
			assert_eq!(txn.changes.table_def[1].op, Delete);
		}
	}

	mod track_view_def_created {
		use super::*;

		#[test]
		fn test_successful_creation() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema).unwrap();

			let view = test_view_def(1, 1, "test_view");
			let result = txn.track_view_def_created(view.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.view_def.len(), 1);
			let change = &txn.changes.view_def[0];
			assert!(change.pre.is_none());
			assert_eq!(
				change.post.as_ref().unwrap().name,
				"test_view"
			);
			assert_eq!(change.op, Create);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 2); // schema + view
			match &txn.changes.log[1] {
				Operation::View {
					id,
					op,
				} if *id == view.id && *op == Create => {}
				_ => panic!(
					"Expected View operation with Create"
				),
			}
		}

		#[test]
		fn test_error_when_already_created() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema).unwrap();

			let view = test_view_def(1, 1, "test_view");

			// First creation should succeed
			txn.track_view_def_created(view.clone()).unwrap();

			// Second creation should fail
			let result = txn.track_view_def_created(view);
			assert!(result.is_err());
			let err = result.unwrap_err();
			assert_eq!(err.diagnostic().code, "CA_013");
		}
	}

	mod track_view_def_updated {
		use super::*;

		#[test]
		fn test_multiple_updates_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let view_v1 = test_view_def(1, 1, "view_v1");
			let view_v2 = test_view_def(1, 1, "view_v2");
			let view_v3 = test_view_def(1, 1, "view_v3");

			// First update
			txn.track_view_def_updated(
				view_v1.clone(),
				view_v2.clone(),
			)
			.unwrap();

			// Should have one change
			assert_eq!(txn.changes.view_def.len(), 1);
			assert_eq!(
				txn.changes.view_def[0]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"view_v1"
			);
			assert_eq!(
				txn.changes.view_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"view_v2"
			);
			assert_eq!(txn.changes.view_def[0].op, Update);

			// Second update - should NOT coalesce
			txn.track_view_def_updated(view_v2, view_v3.clone())
				.unwrap();

			// Should now have TWO changes
			assert_eq!(txn.changes.view_def.len(), 2);

			// First update unchanged
			assert_eq!(
				txn.changes.view_def[0]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"view_v1"
			);
			assert_eq!(
				txn.changes.view_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"view_v2"
			);

			// Second update recorded separately
			assert_eq!(
				txn.changes.view_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"view_v2"
			);
			assert_eq!(
				txn.changes.view_def[1]
					.post
					.as_ref()
					.unwrap()
					.name,
				"view_v3"
			);
		}

		#[test]
		fn test_create_then_update_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema).unwrap();

			let view_v1 = test_view_def(1, 1, "view_v1");
			let view_v2 = test_view_def(1, 1, "view_v2");

			// First track creation
			txn.track_view_def_created(view_v1.clone()).unwrap();
			assert_eq!(txn.changes.view_def.len(), 1);
			assert_eq!(txn.changes.view_def[0].op, Create);

			// Then track update - should NOT coalesce
			txn.track_view_def_updated(view_v1, view_v2.clone())
				.unwrap();

			// Should have TWO changes now
			assert_eq!(txn.changes.view_def.len(), 2);

			// First is still Create
			assert_eq!(txn.changes.view_def[0].op, Create);
			assert_eq!(
				txn.changes.view_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"view_v1"
			);

			// Second is Update
			assert_eq!(txn.changes.view_def[1].op, Update);
			assert_eq!(
				txn.changes.view_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"view_v1"
			);
			assert_eq!(
				txn.changes.view_def[1]
					.post
					.as_ref()
					.unwrap()
					.name,
				"view_v2"
			);
		}
	}

	mod track_view_def_deleted {
		use super::*;

		#[test]
		fn test_delete_after_create_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let schema = test_schema_def(1, "test_schema");
			txn.track_schema_def_created(schema).unwrap();

			let view = test_view_def(1, 1, "test_view");

			// First track creation
			txn.track_view_def_created(view.clone()).unwrap();
			assert_eq!(txn.changes.view_def.len(), 1);

			// Then track deletion - should NOT remove
			let result = txn.track_view_def_deleted(view.clone());
			assert!(result.is_ok());

			// Should have TWO changes now
			assert_eq!(txn.changes.view_def.len(), 2);

			// First is Create
			assert_eq!(txn.changes.view_def[0].op, Create);

			// Second is Delete
			assert_eq!(txn.changes.view_def[1].op, Delete);
			assert_eq!(
				txn.changes.view_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"test_view"
			);
		}

		#[test]
		fn test_delete_after_update_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let view_v1 = test_view_def(1, 1, "view_v1");
			let view_v2 = test_view_def(1, 1, "view_v2");

			// First track update
			txn.track_view_def_updated(
				view_v1.clone(),
				view_v2.clone(),
			)
			.unwrap();
			assert_eq!(txn.changes.view_def.len(), 1);

			// Then track deletion
			let result = txn.track_view_def_deleted(view_v2);
			assert!(result.is_ok());

			// Should have TWO changes
			assert_eq!(txn.changes.view_def.len(), 2);

			// First is Update
			assert_eq!(txn.changes.view_def[0].op, Update);

			// Second is Delete
			assert_eq!(txn.changes.view_def[1].op, Delete);
		}
	}
}
