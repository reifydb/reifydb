// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(test)]
mod tests {
	use crate::interface::change::{TransactionalChanges, OperationType, Operation};
	use crate::interface::{
		SchemaDef, SchemaId, TableDef, TableId, TransactionId,
		ViewDef, ViewId,
	};

	#[test]
	fn test_schema_create_and_update() {
		let txn_id = TransactionId::generate();
		let mut changes = TransactionalChanges::new(txn_id);

		// Create a schema
		let schema1 = SchemaDef {
			id: SchemaId(1),
			name: "test_schema".to_string(),
		};

		changes.add_schema_def_create(schema1.clone()).unwrap();

		// Verify it exists in transaction view
		assert!(changes.schema_exists(SchemaId(1)));
		assert_eq!(
			changes.get_schema(SchemaId(1)).unwrap().name,
			"test_schema"
		);

		// Update the same schema (should coalesce with create)
		let schema2 = SchemaDef {
			id: SchemaId(1),
			name: "updated_schema".to_string(),
		};

		changes.add_schema_def_update(schema1.clone(), schema2.clone())
			.unwrap();

		// Should still show as CREATE operation with updated name
		assert_eq!(
			changes.get_schema(SchemaId(1)).unwrap().name,
			"updated_schema"
		);
		let schema_changes = changes.schema_def();
		assert_eq!(
			schema_changes.get(&SchemaId(1)).unwrap().operation,
			OperationType::Create
		);
		assert_eq!(
			schema_changes
				.get(&SchemaId(1))
				.unwrap()
				.post
				.as_ref()
				.unwrap()
				.name,
			"updated_schema"
		);
	}

	#[test]
	fn test_create_and_delete_same_transaction() {
		let txn_id = TransactionId::generate();
		let mut changes = TransactionalChanges::new(txn_id);

		// Create a table
		let table = TableDef {
			id: TableId(1),
			schema: SchemaId(1),
			name: "test_table".to_string(),
			columns: vec![],
		};

		changes.add_table_def_create(table.clone()).unwrap();
		assert!(changes.table_exists(TableId(1)));
		assert_eq!(changes.get_pending_changes().len(), 1);

		// Delete the same table - should remove it entirely
		changes.add_table_def_delete(table).unwrap();

		// Should not exist anymore
		assert!(!changes.table_exists(TableId(1)));
		assert_eq!(changes.get_pending_changes().len(), 0);
		assert!(!changes.has_changes());
	}

	#[test]
	fn test_multiple_updates_coalesce() {
		let txn_id = TransactionId::generate();
		let mut changes = TransactionalChanges::new(txn_id);

		// Initial view
		let view1 = ViewDef {
			id: ViewId(1),
			schema: SchemaId(1),
			name: "view_v1".to_string(),
			kind: crate::interface::ViewKind::Deferred,
			columns: vec![],
		};

		// First update
		let view2 = ViewDef {
			id: ViewId(1),
			schema: SchemaId(1),
			name: "view_v2".to_string(),
			kind: crate::interface::ViewKind::Deferred,
			columns: vec![],
		};

		// Second update
		let view3 = ViewDef {
			id: ViewId(1),
			schema: SchemaId(1),
			name: "view_v3".to_string(),
			kind: crate::interface::ViewKind::Transactional,
			columns: vec![],
		};

		// Add as update (assuming it existed before)
		changes.add_view_def_update(view1.clone(), view2.clone()).unwrap();
		changes.add_view_def_update(view2, view3.clone()).unwrap();

		// Should keep original pre and latest post
		let view_changes = changes.view_def();
		let change = view_changes.get(&ViewId(1)).unwrap();
		assert_eq!(change.operation, OperationType::Update);
		assert_eq!(change.pre.as_ref().unwrap().name, "view_v1");
		assert_eq!(change.post.as_ref().unwrap().name, "view_v3");
		assert_eq!(
			change.post.as_ref().unwrap().kind,
			crate::interface::ViewKind::Transactional
		);
	}

	#[test]
	fn test_conflict_detection() {
		let txn_id = TransactionId::generate();
		let mut changes = TransactionalChanges::new(txn_id);

		let schema = SchemaDef {
			id: SchemaId(1),
			name: "test".to_string(),
		};

		// Create a schema
		changes.add_schema_def_create(schema.clone()).unwrap();

		// Try to create the same schema again - should fail
		assert!(changes.add_schema_def_create(schema.clone()).is_err());

		// For update/delete conflict, let's test with an existing (updated) schema
		let schema2 = SchemaDef {
			id: SchemaId(2),
			name: "existing".to_string(),
		};

		let schema2_updated = SchemaDef {
			id: SchemaId(2),
			name: "existing_updated".to_string(),
		};

		// Add an update (simulating an existing schema being updated)
		changes.add_schema_def_update(
			schema2.clone(),
			schema2_updated.clone(),
		)
		.unwrap();

		// Try to delete it
		changes.add_schema_def_delete(schema2_updated.clone()).unwrap();

		// Now it should be marked for deletion - trying to update should fail
		assert!(changes
			.add_schema_def_update(schema2.clone(), schema2_updated)
			.is_err());
	}

	#[test]
	fn test_operation_log() {
		let txn_id = TransactionId::generate();
		let mut changes = TransactionalChanges::new(txn_id);

		// Add various operations
		changes.add_schema_def_create(SchemaDef {
			id: SchemaId(1),
			name: "schema1".to_string(),
		})
		.unwrap();

		changes.add_table_def_create(TableDef {
			id: TableId(1),
			schema: SchemaId(1),
			name: "table1".to_string(),
			columns: vec![],
		})
		.unwrap();

		changes.add_view_def_create(ViewDef {
			id: ViewId(1),
			schema: SchemaId(1),
			name: "view1".to_string(),
			kind: crate::interface::ViewKind::Deferred,
			columns: vec![],
		})
		.unwrap();

		// Check operation log
		let ops = changes.get_pending_changes();
		assert_eq!(ops.len(), 3);

		// Operations should be in order
		matches!(
			&ops[0],
			Operation::Schema { .. }
		);
		matches!(
			&ops[1],
			Operation::Table { .. }
		);
		matches!(
			&ops[2],
			Operation::View { .. }
		);
	}
}
