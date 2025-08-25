// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod schema;
mod table;
mod view;

use reifydb_core::{
	Version,
	interface::{
		CommandTransaction, OperationType, SchemaDef, SchemaId,
		TableDef, TableId, TransactionalChanges, ViewDef, ViewId,
	},
};

use crate::{
	MaterializedCatalog, schema::SchemaToCreate, table::TableToCreate,
	view::ViewToCreate,
};

// Schema operations
pub trait CatalogSchemaOperations {
	fn create_schema(
		&mut self,
		schema: SchemaToCreate,
	) -> crate::Result<SchemaDef>;

	fn find_schema_by_name(
		&mut self,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SchemaDef>>;

	fn find_schema(
		&mut self,
		id: SchemaId,
	) -> crate::Result<Option<SchemaDef>>;

	fn get_schema(&mut self, id: SchemaId) -> crate::Result<SchemaDef>;

	// TODO: Implement when update/delete are ready
	// fn update_schema(&mut self, schema_id: SchemaId, updates:
	// SchemaUpdates) -> crate::Result<SchemaDef>; fn delete_schema(&mut
	// self, schema_id: SchemaId) -> crate::Result<()>;
}

// Table operations
pub trait CatalogTableOperations {
	fn create_table(
		&mut self,
		table: TableToCreate,
	) -> crate::Result<TableDef>;

	fn find_table_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>>;

	fn find_table(
		&mut self,
		id: TableId,
	) -> crate::Result<Option<TableDef>>;

	// TODO: Implement when update/delete are ready
	// fn update_table(&mut self, table_id: TableId, updates: TableUpdates)
	// -> crate::Result<TableDef>; fn delete_table(&mut self, table_id:
	// TableId) -> crate::Result<()>;
}

// View operations
pub trait CatalogViewOperations {
	fn create_view(&mut self, view: ViewToCreate)
	-> crate::Result<ViewDef>;

	fn find_view_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>>;

	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>>;

	// TODO: Implement when update/delete are ready
	// fn update_view(&mut self, view_id: ViewId, updates: ViewUpdates) ->
	// crate::Result<ViewDef>; fn delete_view(&mut self, view_id: ViewId)
	// -> crate::Result<()>;
}

// Combined catalog transaction trait
pub trait CatalogTransaction:
	CatalogSchemaOperations + CatalogTableOperations + CatalogViewOperations
{
}

// Context trait that provides access to catalog-specific state
pub trait CatalogTransactionContext: CommandTransaction {
	fn catalog(&self) -> &MaterializedCatalog;
	fn version(&self) -> Version;

	// Schema tracking methods
	fn track_schema_created(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()>;
	fn track_schema_updated(
		&mut self,
		pre: SchemaDef,
		post: SchemaDef,
	) -> crate::Result<()>;
	fn track_schema_deleted(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()>;

	// Table tracking methods
	fn track_table_created(&mut self, table: TableDef)
	-> crate::Result<()>;

	fn track_table_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> crate::Result<()>;
	fn track_table_deleted(&mut self, table: TableDef)
	-> crate::Result<()>;

	// View tracking methods
	fn track_view_created(&mut self, view: ViewDef) -> crate::Result<()>;
	fn track_view_updated(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()>;
	fn track_view_deleted(&mut self, view: ViewDef) -> crate::Result<()>;
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
		self.schema_def.values().find_map(|change| {
			change.post.as_ref().filter(|s| s.name == name)
		})
	}

	fn is_schema_deleted_by_name(&self, name: &str) -> bool {
		self.schema_def.values().any(|change| {
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
		self.table_def.values().find_map(|change| {
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
		self.table_def.values().any(|change| {
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
		self.view_def.values().find_map(|change| {
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
		self.view_def.values().any(|change| {
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
