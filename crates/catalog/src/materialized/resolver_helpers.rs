// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, Result,
	interface::{ColumnDef, ViewDef},
};

use crate::materialized::MaterializedCatalog;

/// Helper methods for identifier resolution
impl MaterializedCatalog {
	/// Check if a schema exists at the given version
	pub fn schema_exists(
		&self,
		name: &str,
		version: CommitVersion,
	) -> bool {
		self.schemas_by_name
			.get(name)
			.and_then(|entry| {
				let schema_id = *entry.value();
				self.find_schema(schema_id, version)
			})
			.is_some()
	}

	/// Check if a table exists in the given schema at the given version
	pub fn table_exists(
		&self,
		schema: &str,
		table: &str,
		version: CommitVersion,
	) -> bool {
		self.find_schema_by_name(schema, version)
			.and_then(|schema_def| {
				self.find_table_by_name(
					schema_def.id,
					table,
					version,
				)
			})
			.is_some()
	}

	/// Check if a view exists in the given schema at the given version
	pub fn view_exists(
		&self,
		schema: &str,
		view: &str,
		version: CommitVersion,
	) -> bool {
		self.find_schema_by_name(schema, version)
			.and_then(|schema_def| {
				self.find_view_by_name(
					schema_def.id,
					view,
					version,
				)
			})
			.is_some()
	}

	/// Check if a source (table or view) exists
	pub fn source_exists(
		&self,
		schema: &str,
		name: &str,
		version: CommitVersion,
	) -> bool {
		self.table_exists(schema, name, version)
			|| self.view_exists(schema, name, version)
	}

	/// Check if a function exists (placeholder - functions not yet in
	/// catalog)
	pub fn function_exists(
		&self,
		_namespaces: &[String],
		_name: &str,
		_version: CommitVersion,
	) -> bool {
		// TODO: Implement when functions are added to catalog
		// For now, return true to not block development
		true
	}

	/// Check if a sequence exists (placeholder - sequences not yet in
	/// catalog)
	pub fn sequence_exists(
		&self,
		_schema: &str,
		_name: &str,
		_version: CommitVersion,
	) -> bool {
		// TODO: Implement when sequences are added to catalog
		// For now, return true to not block development
		true
	}

	/// Check if an index exists (placeholder - indexes not yet in catalog)
	pub fn index_exists(
		&self,
		_schema: &str,
		_table: &str,
		_index: &str,
		_version: CommitVersion,
	) -> bool {
		// TODO: Implement when indexes are added to catalog
		// For now, return true to not block development
		true
	}

	/// Get a view definition
	pub fn get_view(
		&self,
		schema: &str,
		name: &str,
		version: CommitVersion,
	) -> Result<ViewDef> {
		self.find_schema_by_name(schema, version)
			.and_then(|schema_def| {
				self.find_view_by_name(
					schema_def.id,
					name,
					version,
				)
			})
			.ok_or_else(|| {
				reifydb_core::error!(reifydb_core::diagnostic::catalog::view_not_found(
					reifydb_type::Fragment::None,
					schema,
					name
				))
			})
	}

	/// Check if a table has a specific column
	pub fn table_has_column(
		&self,
		schema: &str,
		table: &str,
		column: &str,
		version: CommitVersion,
	) -> bool {
		self.find_schema_by_name(schema, version)
			.and_then(|schema_def| {
				self.find_table_by_name(
					schema_def.id,
					table,
					version,
				)
			})
			.map(|table_def| {
				table_def
					.columns
					.iter()
					.any(|col| col.name == column)
			})
			.unwrap_or(false)
	}

	/// Check if a view has a specific column
	pub fn view_has_column(
		&self,
		schema: &str,
		view: &str,
		column: &str,
		version: CommitVersion,
	) -> bool {
		self.find_schema_by_name(schema, version)
			.and_then(|schema_def| {
				self.find_view_by_name(
					schema_def.id,
					view,
					version,
				)
			})
			.map(|view_def| {
				view_def.columns
					.iter()
					.any(|col| col.name == column)
			})
			.unwrap_or(false)
	}

	/// Get columns for a table
	pub fn get_table_columns(
		&self,
		schema: &str,
		table: &str,
		version: CommitVersion,
	) -> Result<Vec<ColumnDef>> {
		self.find_schema_by_name(schema, version)
			.and_then(|schema_def| {
				self.find_table_by_name(
					schema_def.id,
					table,
					version,
				)
			})
			.map(|table_def| table_def.columns.clone())
			.ok_or_else(|| {
				reifydb_core::error!(reifydb_core::diagnostic::catalog::table_not_found(
					reifydb_type::Fragment::None,
					schema,
					table
				))
			})
	}

	/// Get columns for a view
	pub fn get_view_columns(
		&self,
		schema: &str,
		view: &str,
		version: CommitVersion,
	) -> Result<Vec<ColumnDef>> {
		self.find_schema_by_name(schema, version)
			.and_then(|schema_def| {
				self.find_view_by_name(
					schema_def.id,
					view,
					version,
				)
			})
			.map(|view_def| view_def.columns.clone())
			.ok_or_else(|| {
				reifydb_core::error!(reifydb_core::diagnostic::catalog::view_not_found(
					reifydb_type::Fragment::None,
					schema,
					view
				))
			})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{
		ColumnDef, ColumnId, ColumnIndex, SchemaDef, SchemaId,
		TableDef, TableId,
	};
	use reifydb_type::TypeConstraint;

	use super::*;

	#[test]
	fn test_schema_exists() {
		let catalog = MaterializedCatalog::new();
		let version: CommitVersion = 1;

		// Add a schema
		let schema = SchemaDef {
			id: SchemaId(1),
			name: "test_schema".to_string(),
		};
		catalog.set_schema(SchemaId(1), version, Some(schema));

		assert!(catalog.schema_exists("test_schema", version));
		assert!(!catalog.schema_exists("nonexistent", version));
	}

	#[test]
	fn test_table_exists() {
		let catalog = MaterializedCatalog::new();
		let version: CommitVersion = 1;

		// Add schema
		let schema = SchemaDef {
			id: SchemaId(1),
			name: "test_schema".to_string(),
		};
		catalog.set_schema(SchemaId(1), version, Some(schema));

		// Add table
		let table = TableDef {
			id: TableId(1),
			schema: SchemaId(1),
			name: "test_table".to_string(),
			columns: vec![],
			primary_key: None,
		};
		catalog.set_table(TableId(1), version, Some(table));

		assert!(catalog.table_exists(
			"test_schema",
			"test_table",
			version
		));
		assert!(!catalog.table_exists(
			"test_schema",
			"nonexistent",
			version
		));
	}

	#[test]
	fn test_table_has_column() {
		let catalog = MaterializedCatalog::new();
		let version: CommitVersion = 1;

		// Add schema
		let schema = SchemaDef {
			id: SchemaId(1),
			name: "test_schema".to_string(),
		};
		catalog.set_schema(SchemaId(1), version, Some(schema));

		// Add table with columns
		let table = TableDef {
			id: TableId(1),
			schema: SchemaId(1),
			name: "test_table".to_string(),
			columns: vec![
				ColumnDef {
					id: ColumnId(1),
					name: "id".to_string(),
					index: ColumnIndex(0),
					constraint: TypeConstraint::unconstrained(
						reifydb_type::Type::Int4,
					),
					auto_increment: false,
					policies: vec![],
				},
				ColumnDef {
					id: ColumnId(2),
					name: "name".to_string(),
					index: ColumnIndex(1),
					constraint: TypeConstraint::unconstrained(
						reifydb_type::Type::Utf8,
					),
					auto_increment: false,
					policies: vec![],
				},
			],
			primary_key: None,
		};
		catalog.set_table(TableId(1), version, Some(table));

		assert!(catalog.table_has_column(
			"test_schema",
			"test_table",
			"id",
			version
		));
		assert!(catalog.table_has_column(
			"test_schema",
			"test_table",
			"name",
			version
		));
		assert!(!catalog.table_has_column(
			"test_schema",
			"test_table",
			"nonexistent",
			version
		));
	}
}
