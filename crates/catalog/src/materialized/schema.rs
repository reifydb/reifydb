// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion,
	interface::{SchemaDef, SchemaId},
};

use crate::materialized::{MaterializedCatalog, VersionedSchemaDef};

impl MaterializedCatalog {
	/// Find a schema by ID at a specific version
	pub fn find_schema(
		&self,
		schema: SchemaId,
		version: CommitVersion,
	) -> Option<SchemaDef> {
		self.schemas.get(&schema).and_then(|entry| {
			let versioned = entry.value();
			versioned.get(version)
		})
	}

	/// Find a schema by name at a specific version
	pub fn find_schema_by_name(
		&self,
		schema: &str,
		version: CommitVersion,
	) -> Option<SchemaDef> {
		self.schemas_by_name.get(schema).and_then(|entry| {
			let schema_id = *entry.value();
			self.find_schema(schema_id, version)
		})
	}

	pub fn set_schema(
		&self,
		id: SchemaId,
		version: CommitVersion,
		schema: Option<SchemaDef>,
	) {
		// Look up the current schema to update the index
		if let Some(entry) = self.schemas.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.schemas_by_name.remove(&pre.name);
			}
		}

		// Add new name to index if setting a new value
		if let Some(ref new) = schema {
			self.schemas_by_name.insert(new.name.clone(), id);
		}

		// Update the versioned schema
		let versioned = self
			.schemas
			.get_or_insert_with(id, VersionedSchemaDef::new);
		versioned.value().insert(version, schema);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn create_test_schema(id: SchemaId, name: &str) -> SchemaDef {
		SchemaDef {
			id,
			name: name.to_string(),
		}
	}

	#[test]
	fn test_set_and_find_schema() {
		let catalog = MaterializedCatalog::new();
		let schema_id = SchemaId(1);
		let schema = create_test_schema(schema_id, "test_schema");

		// Set schema at version 1
		catalog.set_schema(schema_id, 1, Some(schema.clone()));

		// Find schema at version 1
		let found = catalog.find_schema(schema_id, 1);
		assert_eq!(found, Some(schema.clone()));

		// Find schema at later version (should return same schema)
		let found = catalog.find_schema(schema_id, 5);
		assert_eq!(found, Some(schema));

		// Schema shouldn't exist at version 0
		let found = catalog.find_schema(schema_id, 0);
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_schema_by_name() {
		let catalog = MaterializedCatalog::new();
		let schema_id = SchemaId(1);
		let schema = create_test_schema(schema_id, "named_schema");

		// Set schema
		catalog.set_schema(schema_id, 1, Some(schema.clone()));

		// Find by name
		let found = catalog.find_schema_by_name("named_schema", 1);
		assert_eq!(found, Some(schema));

		// Shouldn't find with wrong name
		let found = catalog.find_schema_by_name("wrong_name", 1);
		assert_eq!(found, None);
	}

	#[test]
	fn test_schema_rename() {
		let catalog = MaterializedCatalog::new();
		let schema_id = SchemaId(1);

		// Create and set initial schema
		let schema_v1 = create_test_schema(schema_id, "old_name");
		catalog.set_schema(schema_id, 1, Some(schema_v1.clone()));

		// Verify initial state
		assert!(catalog.find_schema_by_name("old_name", 1).is_some());
		assert!(catalog.find_schema_by_name("new_name", 1).is_none());

		// Rename the schema
		let mut schema_v2 = schema_v1.clone();
		schema_v2.name = "new_name".to_string();
		catalog.set_schema(schema_id, 2, Some(schema_v2.clone()));

		// Old name should be gone
		assert!(catalog.find_schema_by_name("old_name", 2).is_none());

		// New name can be found
		assert_eq!(
			catalog.find_schema_by_name("new_name", 2),
			Some(schema_v2.clone())
		);

		// Historical query at version 1 should still show old name
		assert_eq!(catalog.find_schema(schema_id, 1), Some(schema_v1));

		// Current version should show new name
		assert_eq!(catalog.find_schema(schema_id, 2), Some(schema_v2));
	}

	#[test]
	fn test_schema_deletion() {
		let catalog = MaterializedCatalog::new();
		let schema_id = SchemaId(1);

		// Create and set schema
		let schema = create_test_schema(schema_id, "deletable_schema");
		catalog.set_schema(schema_id, 1, Some(schema.clone()));

		// Verify it exists
		assert_eq!(
			catalog.find_schema(schema_id, 1),
			Some(schema.clone())
		);
		assert!(catalog
			.find_schema_by_name("deletable_schema", 1)
			.is_some());

		// Delete the schema
		catalog.set_schema(schema_id, 2, None);

		// Should not exist at version 2
		assert_eq!(catalog.find_schema(schema_id, 2), None);
		assert!(catalog
			.find_schema_by_name("deletable_schema", 2)
			.is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_schema(schema_id, 1), Some(schema));
	}

	#[test]
	fn test_multiple_schemas() {
		let catalog = MaterializedCatalog::new();

		let schema1 = create_test_schema(SchemaId(1), "schema1");
		let schema2 = create_test_schema(SchemaId(2), "schema2");
		let schema3 = create_test_schema(SchemaId(3), "schema3");

		// Set multiple schemas
		catalog.set_schema(SchemaId(1), 1, Some(schema1.clone()));
		catalog.set_schema(SchemaId(2), 1, Some(schema2.clone()));
		catalog.set_schema(SchemaId(3), 1, Some(schema3.clone()));

		// All should be findable
		assert_eq!(
			catalog.find_schema_by_name("schema1", 1),
			Some(schema1)
		);
		assert_eq!(
			catalog.find_schema_by_name("schema2", 1),
			Some(schema2)
		);
		assert_eq!(
			catalog.find_schema_by_name("schema3", 1),
			Some(schema3)
		);
	}

	#[test]
	fn test_schema_versioning() {
		let catalog = MaterializedCatalog::new();
		let schema_id = SchemaId(1);

		// Create multiple versions
		let schema_v1 = create_test_schema(schema_id, "schema_v1");
		let mut schema_v2 = schema_v1.clone();
		schema_v2.name = "schema_v2".to_string();
		let mut schema_v3 = schema_v2.clone();
		schema_v3.name = "schema_v3".to_string();

		// Set at different versions
		catalog.set_schema(schema_id, 10, Some(schema_v1.clone()));
		catalog.set_schema(schema_id, 20, Some(schema_v2.clone()));
		catalog.set_schema(schema_id, 30, Some(schema_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_schema(schema_id, 5), None);
		assert_eq!(
			catalog.find_schema(schema_id, 10),
			Some(schema_v1.clone())
		);
		assert_eq!(catalog.find_schema(schema_id, 15), Some(schema_v1));
		assert_eq!(
			catalog.find_schema(schema_id, 20),
			Some(schema_v2.clone())
		);
		assert_eq!(catalog.find_schema(schema_id, 25), Some(schema_v2));
		assert_eq!(
			catalog.find_schema(schema_id, 30),
			Some(schema_v3.clone())
		);
		assert_eq!(
			catalog.find_schema(schema_id, 100),
			Some(schema_v3)
		);
	}
}
