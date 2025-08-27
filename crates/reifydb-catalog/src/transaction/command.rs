// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, SchemaDef, TableDef, ViewDef,
};

use super::query::{
	CatalogQueryTransaction, CatalogQueryTransactionOperations,
};
use crate::{schema::SchemaToCreate, table::TableToCreate, view::ViewToCreate};

// Schema command operations
pub trait CatalogSchemaCommandOperations {
	fn create_schema(
		&mut self,
		schema: SchemaToCreate,
	) -> crate::Result<SchemaDef>;

	// TODO: Implement when update/delete are ready
	// fn update_schema(&mut self, schema_id: SchemaId, updates:
	// SchemaUpdates) -> crate::Result<SchemaDef>; fn delete_schema(&mut
	// self, schema_id: SchemaId) -> crate::Result<()>;
}

// Table command operations
pub trait CatalogTableCommandOperations {
	fn create_table(
		&mut self,
		table: TableToCreate,
	) -> crate::Result<TableDef>;

	// TODO: Implement when update/delete are ready
	// fn update_table(&mut self, table_id: TableId, updates: TableUpdates)
	// -> crate::Result<TableDef>; fn delete_table(&mut self, table_id:
	// TableId) -> crate::Result<()>;
}

// View command operations
pub trait CatalogViewCommandOperations {
	fn create_view(&mut self, view: ViewToCreate)
	-> crate::Result<ViewDef>;

	// TODO: Implement when update/delete are ready
	// fn update_view(&mut self, view_id: ViewId, updates: ViewUpdates) ->
	// crate::Result<ViewDef>; fn delete_view(&mut self, view_id: ViewId)
	// -> crate::Result<()>;
}

// Combined catalog command transaction trait that extends query capabilities
pub trait CatalogCommandTransaction:
	CatalogQueryTransaction
	+ CatalogSchemaCommandOperations
	+ CatalogTableCommandOperations
	+ CatalogViewCommandOperations
{
}

// Context trait that provides access to catalog-specific state and tracking for
// commands
pub trait CatalogCommandTransactionOperations:
	CommandTransaction + CatalogQueryTransactionOperations
{
	// Schema tracking methods
	fn track_schema_def_created(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()>;

	fn track_schema_def_updated(
		&mut self,
		pre: SchemaDef,
		post: SchemaDef,
	) -> crate::Result<()>;

	fn track_schema_def_deleted(
		&mut self,
		schema: SchemaDef,
	) -> crate::Result<()>;

	// Table tracking methods
	fn track_table_def_created(
		&mut self,
		table: TableDef,
	) -> crate::Result<()>;

	fn track_table_def_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> crate::Result<()>;

	fn track_table_def_deleted(
		&mut self,
		table: TableDef,
	) -> crate::Result<()>;

	// View tracking methods
	fn track_view_def_created(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()>;

	fn track_view_def_updated(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()>;

	fn track_view_def_deleted(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()>;
}
