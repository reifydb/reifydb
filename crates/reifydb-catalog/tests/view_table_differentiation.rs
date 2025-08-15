// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{Catalog, table::TableToCreate, view::ViewToCreate};
use reifydb_core::interface::{TableId, ViewId};
use reifydb_transaction::test_utils::create_test_command_transaction;

#[test]
fn test_view_and_table_can_coexist_with_same_name() {
	let mut txn = create_test_command_transaction();

	// Create a schema
	let schema = Catalog::create_schema(
		&mut txn,
		reifydb_catalog::schema::SchemaToCreate {
			schema_span: None,
			name: "test_schema".to_string(),
		},
	)
	.unwrap();

	// Create a table named "users"
	let table = Catalog::create_table(
		&mut txn,
		TableToCreate {
			span: None,
			schema: "test_schema".to_string(),
			table: "users".to_string(),
			columns: vec![],
		},
	)
	.unwrap();

	// Create a view also named "users" - this should succeed
	// as views and tables are stored separately
	let view = Catalog::create_view(
		&mut txn,
		ViewToCreate {
			span: None,
			schema: "test_schema".to_string(),
			view: "users".to_string(),
			columns: vec![],
		},
	)
	.unwrap();

	// Verify both exist and have different IDs
	assert_eq!(table.name, "users");
	assert_eq!(view.name, "users");
	assert_eq!(table.schema, schema.id);
	assert_eq!(view.schema, schema.id);

	// The IDs should be the same numerically since they share sequence,
	// but they are different types
	assert_eq!(table.id.0, 1025);
	assert_eq!(view.id.0, 1025);

	// Verify we can retrieve both independently
	let retrieved_table = Catalog::get_table(&mut txn, table.id)
		.unwrap()
		.expect("Table should exist");
	assert_eq!(retrieved_table.name, "users");

	let retrieved_view = Catalog::get_view(&mut txn, view.id)
		.unwrap()
		.expect("View should exist");
	assert_eq!(retrieved_view.name, "users");
}

#[test]
fn test_view_ids_are_separate_from_table_ids() {
	let mut txn = create_test_command_transaction();

	// Create a schema
	Catalog::create_schema(
		&mut txn,
		reifydb_catalog::schema::SchemaToCreate {
			schema_span: None,
			name: "test_schema".to_string(),
		},
	)
	.unwrap();

	// Create multiple tables
	let table1 = Catalog::create_table(
		&mut txn,
		TableToCreate {
			span: None,
			schema: "test_schema".to_string(),
			table: "table1".to_string(),
			columns: vec![],
		},
	)
	.unwrap();

	let table2 = Catalog::create_table(
		&mut txn,
		TableToCreate {
			span: None,
			schema: "test_schema".to_string(),
			table: "table2".to_string(),
			columns: vec![],
		},
	)
	.unwrap();

	// Create multiple views
	let view1 = Catalog::create_view(
		&mut txn,
		ViewToCreate {
			span: None,
			schema: "test_schema".to_string(),
			view: "view1".to_string(),
			columns: vec![],
		},
	)
	.unwrap();

	let view2 = Catalog::create_view(
		&mut txn,
		ViewToCreate {
			span: None,
			schema: "test_schema".to_string(),
			view: "view2".to_string(),
			columns: vec![],
		},
	)
	.unwrap();

	// Tables should have sequential IDs
	assert_eq!(table1.id, TableId(1025));
	assert_eq!(table2.id, TableId(1026));

	// Views should also have sequential IDs
	assert_eq!(view1.id, ViewId(1025));
	assert_eq!(view2.id, ViewId(1026));

	// Even though the numeric values overlap, they are different types
	// and stored with different keys, so there's no conflict
}
