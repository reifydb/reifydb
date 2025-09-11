// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion,
	interface::{
		OperationType, SchemaDef, SchemaId, SourceDef, SourceId,
		TableDef, TableId, TransactionalChanges, ViewDef, ViewId,
	},
};
use reifydb_type::IntoFragment;

use crate::MaterializedCatalog;

// Schema query operations
pub trait CatalogSchemaQueryOperations {
	fn find_schema_by_name(
		&mut self,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SchemaDef>>;

	fn find_schema(
		&mut self,
		id: SchemaId,
	) -> crate::Result<Option<SchemaDef>>;

	fn get_schema(&mut self, id: SchemaId) -> crate::Result<SchemaDef>;

	fn get_schema_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a>,
	) -> crate::Result<SchemaDef>;
}

// Source query operations
pub trait CatalogSourceQueryOperations {
	fn find_source_by_name<'a>(
		&mut self,
		schema: SchemaId,
		source: impl IntoFragment<'a>,
	) -> crate::Result<Option<SourceDef>>;

	fn find_source(
		&mut self,
		id: SourceId,
	) -> crate::Result<Option<SourceDef>>;

	fn get_source_by_name<'a>(
		&mut self,
		schema: SchemaId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<SourceDef>;
}

// Table query operations
pub trait CatalogTableQueryOperations {
	fn find_table_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>>;

	fn find_table(
		&mut self,
		id: TableId,
	) -> crate::Result<Option<TableDef>>;

	fn get_table_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<TableDef>;
}

// View query operations
pub trait CatalogViewQueryOperations {
	fn find_view_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>>;

	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>>;

	fn get_view_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<ViewDef>;
}

// Combined catalog query transaction trait
pub trait CatalogQueryTransaction:
	CatalogSchemaQueryOperations
	+ CatalogSourceQueryOperations
	+ CatalogTableQueryOperations
	+ CatalogViewQueryOperations
{
}

pub trait CatalogTransaction {
	fn catalog(&self) -> &MaterializedCatalog;
	fn version(&self) -> CommitVersion;
}

// Extension trait for TransactionalChanges with catalog-specific helpers
pub trait TransactionalChangesExt {
	fn find_schema_by_name(&self, name: &str) -> Option<&SchemaDef>;

	fn is_schema_deleted_by_name(&self, name: &str) -> bool;

	fn find_table_by_name(
		&self,
		schema: SchemaId,
		name: &str,
	) -> Option<&TableDef>;

	fn is_table_deleted_by_name(
		&self,
		schema: SchemaId,
		name: &str,
	) -> bool;

	fn find_view_by_name(
		&self,
		schema: SchemaId,
		name: &str,
	) -> Option<&ViewDef>;

	fn is_view_deleted_by_name(&self, schema: SchemaId, name: &str)
	-> bool;
}

impl TransactionalChangesExt for TransactionalChanges {
	fn find_schema_by_name(&self, name: &str) -> Option<&SchemaDef> {
		self.schema_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|s| s.name == name)
		})
	}

	fn is_schema_deleted_by_name(&self, name: &str) -> bool {
		self.schema_def.iter().rev().any(|change| {
			change.op == OperationType::Delete
				&& change.pre.as_ref().map(|s| s.name.as_str())
					== Some(name)
		})
	}

	fn find_table_by_name(
		&self,
		schema: SchemaId,
		name: &str,
	) -> Option<&TableDef> {
		self.table_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|t| {
				t.schema == schema && t.name == name
			})
		})
	}

	fn is_table_deleted_by_name(
		&self,
		schema: SchemaId,
		name: &str,
	) -> bool {
		self.table_def.iter().rev().any(|change| {
			change.op == OperationType::Delete
				&& change
					.pre
					.as_ref()
					.map(|t| {
						t.schema == schema
							&& t.name == name
					})
					.unwrap_or(false)
		})
	}

	fn find_view_by_name(
		&self,
		schema: SchemaId,
		name: &str,
	) -> Option<&ViewDef> {
		self.view_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|v| {
				v.schema == schema && v.name == name
			})
		})
	}

	fn is_view_deleted_by_name(
		&self,
		schema: SchemaId,
		name: &str,
	) -> bool {
		self.view_def.iter().rev().any(|change| {
			change.op == OperationType::Delete
				&& change
					.pre
					.as_ref()
					.map(|v| {
						v.schema == schema
							&& v.name == name
					})
					.unwrap_or(false)
		})
	}
}
