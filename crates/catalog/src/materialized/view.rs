// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion,
	interface::{SchemaId, ViewDef, ViewId},
};

use crate::materialized::{MaterializedCatalog, VersionedViewDef};

impl MaterializedCatalog {
	/// Find a view by ID at a specific version
	pub fn find_view(
		&self,
		view: ViewId,
		version: CommitVersion,
	) -> Option<ViewDef> {
		self.views.get(&view).and_then(|entry| {
			let versioned = entry.value();
			versioned.get(version)
		})
	}

	/// Find a view by name in a schema at a specific version
	pub fn find_view_by_name(
		&self,
		schema: SchemaId,
		name: &str,
		version: CommitVersion,
	) -> Option<ViewDef> {
		self.views_by_name.get(&(schema, name.to_string())).and_then(
			|entry| {
				let view_id = *entry.value();
				self.find_view(view_id, version)
			},
		)
	}

	pub fn set_view(
		&self,
		id: ViewId,
		version: CommitVersion,
		view: Option<ViewDef>,
	) {
		// Look up the current view to update the index
		if let Some(entry) = self.views.get(&id) {
			if let Some(pre) = entry.value().get_latest() {
				// Remove old name from index
				self.views_by_name.remove(&(
					pre.schema,
					pre.name.clone(),
				));
			}
		}

		// Add new name to index if setting a new value
		if let Some(ref new) = view {
			self.views_by_name
				.insert((new.schema, new.name.clone()), id);
		}

		// Update the versioned view
		let versioned = self
			.views
			.get_or_insert_with(id, VersionedViewDef::new);

		versioned.value().insert(version, view);
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{
		ColumnDef, ColumnId, ColumnIndex, ViewKind,
	};
	use reifydb_type::{Type, TypeConstraint};

	use super::*;

	fn create_test_view(
		id: ViewId,
		schema: SchemaId,
		name: &str,
	) -> ViewDef {
		ViewDef {
			id,
			schema,
			name: name.to_string(),
			kind: ViewKind::Deferred,
			columns: vec![
				ColumnDef {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Int1,
						),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				ColumnDef {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
			],
			primary_key: None,
		}
	}

	#[test]
	fn test_set_and_find_view() {
		let catalog = MaterializedCatalog::new();
		let view_id = ViewId(1);
		let schema_id = SchemaId(1);
		let view = create_test_view(view_id, schema_id, "test_view");

		// Set view at version 1
		catalog.set_view(view_id, 1, Some(view.clone()));

		// Find view at version 1
		let found = catalog.find_view(view_id, 1);
		assert_eq!(found, Some(view.clone()));

		// Find view at later version (should return same view)
		let found = catalog.find_view(view_id, 5);
		assert_eq!(found, Some(view));

		// View shouldn't exist at version 0
		let found = catalog.find_view(view_id, 0);
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_view_by_name() {
		let catalog = MaterializedCatalog::new();
		let view_id = ViewId(1);
		let schema_id = SchemaId(1);
		let view = create_test_view(view_id, schema_id, "named_view");

		// Set view
		catalog.set_view(view_id, 1, Some(view.clone()));

		// Find by name
		let found =
			catalog.find_view_by_name(schema_id, "named_view", 1);
		assert_eq!(found, Some(view));

		// Shouldn't find with wrong name
		let found =
			catalog.find_view_by_name(schema_id, "wrong_name", 1);
		assert_eq!(found, None);

		// Shouldn't find in wrong schema
		let found =
			catalog.find_view_by_name(SchemaId(2), "named_view", 1);
		assert_eq!(found, None);
	}

	#[test]
	fn test_view_rename() {
		let catalog = MaterializedCatalog::new();
		let view_id = ViewId(1);
		let schema_id = SchemaId(1);

		// Create and set initial view
		let view_v1 = create_test_view(view_id, schema_id, "old_name");
		catalog.set_view(view_id, 1, Some(view_v1.clone()));

		// Verify initial state
		assert!(catalog
			.find_view_by_name(schema_id, "old_name", 1)
			.is_some());
		assert!(catalog
			.find_view_by_name(schema_id, "new_name", 1)
			.is_none());

		// Rename the view
		let mut view_v2 = view_v1.clone();
		view_v2.name = "new_name".to_string();
		catalog.set_view(view_id, 2, Some(view_v2.clone()));

		// Old name should be gone
		assert!(catalog
			.find_view_by_name(schema_id, "old_name", 2)
			.is_none());

		// New name can be found
		assert_eq!(
			catalog.find_view_by_name(schema_id, "new_name", 2),
			Some(view_v2.clone())
		);

		// Historical query at version 1 should still show old name
		assert_eq!(catalog.find_view(view_id, 1), Some(view_v1));

		// Current version should show new name
		assert_eq!(catalog.find_view(view_id, 2), Some(view_v2));
	}

	#[test]
	fn test_view_move_between_schemas() {
		let catalog = MaterializedCatalog::new();
		let view_id = ViewId(1);
		let schema1 = SchemaId(1);
		let schema2 = SchemaId(2);

		// Create view in schema1
		let view_v1 =
			create_test_view(view_id, schema1, "movable_view");
		catalog.set_view(view_id, 1, Some(view_v1.clone()));

		// Verify it's in schema1
		assert!(catalog
			.find_view_by_name(schema1, "movable_view", 1)
			.is_some());
		assert!(catalog
			.find_view_by_name(schema2, "movable_view", 1)
			.is_none());

		// Move to schema2
		let mut view_v2 = view_v1.clone();
		view_v2.schema = schema2;
		catalog.set_view(view_id, 2, Some(view_v2.clone()));

		// Should no longer be in schema1
		assert!(catalog
			.find_view_by_name(schema1, "movable_view", 2)
			.is_none());

		// Should now be in schema2
		assert!(catalog
			.find_view_by_name(schema2, "movable_view", 2)
			.is_some());
	}

	#[test]
	fn test_view_deletion() {
		let catalog = MaterializedCatalog::new();
		let view_id = ViewId(1);
		let schema_id = SchemaId(1);

		// Create and set view
		let view =
			create_test_view(view_id, schema_id, "deletable_view");
		catalog.set_view(view_id, 1, Some(view.clone()));

		// Verify it exists
		assert_eq!(catalog.find_view(view_id, 1), Some(view.clone()));
		assert!(catalog
			.find_view_by_name(schema_id, "deletable_view", 1)
			.is_some());

		// Delete the view
		catalog.set_view(view_id, 2, None);

		// Should not exist at version 2
		assert_eq!(catalog.find_view(view_id, 2), None);
		assert!(catalog
			.find_view_by_name(schema_id, "deletable_view", 2)
			.is_none());

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_view(view_id, 1), Some(view));
	}

	#[test]
	fn test_multiple_views_in_schema() {
		let catalog = MaterializedCatalog::new();
		let schema_id = SchemaId(1);

		let view1 = create_test_view(ViewId(1), schema_id, "view1");
		let view2 = create_test_view(ViewId(2), schema_id, "view2");
		let view3 = create_test_view(ViewId(3), schema_id, "view3");

		// Set multiple views
		catalog.set_view(ViewId(1), 1, Some(view1.clone()));
		catalog.set_view(ViewId(2), 1, Some(view2.clone()));
		catalog.set_view(ViewId(3), 1, Some(view3.clone()));

		// All should be findable
		assert_eq!(
			catalog.find_view_by_name(schema_id, "view1", 1),
			Some(view1)
		);
		assert_eq!(
			catalog.find_view_by_name(schema_id, "view2", 1),
			Some(view2)
		);
		assert_eq!(
			catalog.find_view_by_name(schema_id, "view3", 1),
			Some(view3)
		);
	}

	#[test]
	fn test_view_versioning() {
		let catalog = MaterializedCatalog::new();
		let view_id = ViewId(1);
		let schema_id = SchemaId(1);

		// Create multiple versions
		let view_v1 = create_test_view(view_id, schema_id, "view_v1");
		let mut view_v2 = view_v1.clone();
		view_v2.name = "view_v2".to_string();
		let mut view_v3 = view_v2.clone();
		view_v3.name = "view_v3".to_string();

		// Set at different versions
		catalog.set_view(view_id, 10, Some(view_v1.clone()));
		catalog.set_view(view_id, 20, Some(view_v2.clone()));
		catalog.set_view(view_id, 30, Some(view_v3.clone()));

		// Query at different versions
		assert_eq!(catalog.find_view(view_id, 5), None);
		assert_eq!(
			catalog.find_view(view_id, 10),
			Some(view_v1.clone())
		);
		assert_eq!(catalog.find_view(view_id, 15), Some(view_v1));
		assert_eq!(
			catalog.find_view(view_id, 20),
			Some(view_v2.clone())
		);
		assert_eq!(catalog.find_view(view_id, 25), Some(view_v2));
		assert_eq!(
			catalog.find_view(view_id, 30),
			Some(view_v3.clone())
		);
		assert_eq!(catalog.find_view(view_id, 100), Some(view_v3));
	}
}
