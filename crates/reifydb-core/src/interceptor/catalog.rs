// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	catalog::MaterializedCatalog,
	interface::change::OperationType,
	interceptor::{
		PostCommitContext, PostCommitInterceptor,
		SchemaDefPostCreateContext, SchemaDefPostCreateInterceptor,
		SchemaDefPostUpdateContext, SchemaDefPostUpdateInterceptor,
		SchemaDefPreDeleteContext, SchemaDefPreDeleteInterceptor,
		SchemaDefPreUpdateContext, SchemaDefPreUpdateInterceptor,
		TableDefPostCreateContext, TableDefPostCreateInterceptor,
		TableDefPostUpdateContext, TableDefPostUpdateInterceptor,
		TableDefPreDeleteContext, TableDefPreDeleteInterceptor,
		TableDefPreUpdateContext, TableDefPreUpdateInterceptor,
		ViewDefPostCreateContext, ViewDefPostCreateInterceptor,
		ViewDefPostUpdateContext, ViewDefPostUpdateInterceptor,
		ViewDefPreDeleteContext, ViewDefPreDeleteInterceptor,
		ViewDefPreUpdateContext, ViewDefPreUpdateInterceptor,
	},
	interface::CommandTransaction,
};

/// Interceptor that updates the materialized catalog for schema definitions
pub struct MaterializedSchemaInterceptor {
}

impl MaterializedSchemaInterceptor {
	pub fn new() -> Self {
		Self {
		}
	}
}

impl<CT: CommandTransaction> SchemaDefPostCreateInterceptor<CT>
	for MaterializedSchemaInterceptor
{
	fn intercept(
		&self,
		ctx: &mut SchemaDefPostCreateContext<CT>,
	) -> crate::Result<()> {
		// Record in transaction-local storage
		// ctx.txn.get_changes_mut()
		// 	.add_schema_def_create(ctx.post.clone())?;

		Ok(())
	}
}

impl<CT: CommandTransaction> SchemaDefPreUpdateInterceptor<CT>
	for MaterializedSchemaInterceptor
{
	fn intercept(
		&self,
		_ctx: &mut SchemaDefPreUpdateContext<CT>,
	) -> crate::Result<()> {
		// Nothing to do on pre-update
		Ok(())
	}
}

impl<CT: CommandTransaction> SchemaDefPostUpdateInterceptor<CT>
	for MaterializedSchemaInterceptor
{
	fn intercept(
		&self,
		ctx: &mut SchemaDefPostUpdateContext<CT>,
	) -> crate::Result<()> {
		// Use pre/post from context directly
		ctx.txn.get_changes_mut()
			.add_schema_def_update(ctx.pre.clone(), ctx.post.clone())?;

		Ok(())
	}
}

impl<CT: CommandTransaction> SchemaDefPreDeleteInterceptor<CT>
	for MaterializedSchemaInterceptor
{
	fn intercept(
		&self,
		ctx: &mut SchemaDefPreDeleteContext<CT>,
	) -> crate::Result<()> {
		// Use pre from context
		ctx.txn.get_changes_mut().add_schema_def_delete(ctx.pre.clone())?;

		Ok(())
	}
}

/// Interceptor that updates the materialized catalog for table definitions
pub struct MaterializedTableInterceptor {
}

impl MaterializedTableInterceptor {
	pub fn new() -> Self {
		Self {
		}
	}
}

impl<CT: CommandTransaction> TableDefPostCreateInterceptor<CT>
	for MaterializedTableInterceptor
{
	fn intercept(
		&self,
		ctx: &mut TableDefPostCreateContext<CT>,
	) -> crate::Result<()> {
		// Record in transaction-local storage
		ctx.txn.get_changes_mut().add_table_def_create(ctx.post.clone())?;

		Ok(())
	}
}

impl<CT: CommandTransaction> TableDefPreUpdateInterceptor<CT>
	for MaterializedTableInterceptor
{
	fn intercept(
		&self,
		_ctx: &mut TableDefPreUpdateContext<CT>,
	) -> crate::Result<()> {
		// Nothing to do on pre-update
		Ok(())
	}
}

impl<CT: CommandTransaction> TableDefPostUpdateInterceptor<CT>
	for MaterializedTableInterceptor
{
	fn intercept(
		&self,
		ctx: &mut TableDefPostUpdateContext<CT>,
	) -> crate::Result<()> {
		// Use pre/post from context directly
		ctx.txn.get_changes_mut()
			.add_table_def_update(ctx.pre.clone(), ctx.post.clone())?;

		Ok(())
	}
}

impl<CT: CommandTransaction> TableDefPreDeleteInterceptor<CT>
	for MaterializedTableInterceptor
{
	fn intercept(
		&self,
		ctx: &mut TableDefPreDeleteContext<CT>,
	) -> crate::Result<()> {
		// Use pre from context
		ctx.txn.get_changes_mut().add_table_def_delete(ctx.pre.clone())?;

		Ok(())
	}
}

/// Interceptor that updates the materialized catalog for view definitions
pub struct MaterializedViewInterceptor {
}

impl MaterializedViewInterceptor {
	pub fn new() -> Self {
		Self {
		}
	}
}

impl<CT: CommandTransaction> ViewDefPostCreateInterceptor<CT>
	for MaterializedViewInterceptor
{
	fn intercept(
		&self,
		ctx: &mut ViewDefPostCreateContext<CT>,
	) -> crate::Result<()> {
		// Record in transaction-local storage
		ctx.txn.get_changes_mut().add_view_def_create(ctx.post.clone())?;

		Ok(())
	}
}

impl<CT: CommandTransaction> ViewDefPreUpdateInterceptor<CT>
	for MaterializedViewInterceptor
{
	fn intercept(
		&self,
		_ctx: &mut ViewDefPreUpdateContext<CT>,
	) -> crate::Result<()> {
		// Nothing to do on pre-update
		Ok(())
	}
}

impl<CT: CommandTransaction> ViewDefPostUpdateInterceptor<CT>
	for MaterializedViewInterceptor
{
	fn intercept(
		&self,
		ctx: &mut ViewDefPostUpdateContext<CT>,
	) -> crate::Result<()> {
		// Use pre/post from context directly
		ctx.txn.get_changes_mut()
			.add_view_def_update(ctx.pre.clone(), ctx.post.clone())?;

		Ok(())
	}
}

impl<CT: CommandTransaction> ViewDefPreDeleteInterceptor<CT>
	for MaterializedViewInterceptor
{
	fn intercept(
		&self,
		ctx: &mut ViewDefPreDeleteContext<CT>,
	) -> crate::Result<()> {
		// Use pre from context
		ctx.txn.get_changes_mut().add_view_def_delete(ctx.pre.clone())?;

		Ok(())
	}
}

/// Post-commit interceptor that finalizes catalog changes
pub struct CatalogCommitInterceptor<CT: CommandTransaction> {
	_phantom: std::marker::PhantomData<CT>,
}

impl<CT: CommandTransaction> CatalogCommitInterceptor<CT> {
	pub fn new() -> Self {
		Self {
			_phantom: std::marker::PhantomData,
		}
	}
}

impl<CT: CommandTransaction> PostCommitInterceptor<CT>
	for CatalogCommitInterceptor<CT>
{
	fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()> {
		// let version = ctx.version;
		//
		// // Get catalog changes from the context
		// if let Some(ref changes) = ctx.catalog_changes {
		// 	// Apply schema changes
		// 	for (id, change) in changes.schema_def() {
		// 		match change.op {
		// 			OperationType::Create
		// 			| OperationType::Update => {
		// 				if let Some(schema) =
		// 					&change.post
		// 				{
		// 					CatalogStore::schemas
        //                         .get_or_insert_with(*id, || crate::catalog::versioned::VersionedSchemaDef::new())
        //                         .value()
        //                         .insert(version, Some(schema.clone()));
		//
		// 					// Update name index
		// 					if let Some(
		// 						pre_schema,
		// 					) = &change.pre
		// 					{
		// 						CatalogStore::schemas_by_name.remove(&pre_schema.name);
		// 					}
		// 					CatalogStore::schemas_by_name
        //                         .insert(schema.name.clone(), schema.id);
		// 				}
		// 			}
		// 			OperationType::Delete => {
		// 				if let Some(entry) = self
		// 					.catalog
		// 					.schemas
		// 					.get(id)
		// 				{
		// 					entry.value().insert(
		// 						version, None,
		// 					);
		// 				}
		// 				if let Some(schema) =
		// 					&change.pre
		// 				{
		// 					CatalogStore::schemas_by_name.remove(&schema.name);
		// 				}
		// 			}
		// 		}
		// 	}
		//
		// 	// Apply table changes
		// 	for (id, change) in changes.table_def() {
		// 		match change.op {
		// 			OperationType::Create
		// 			| OperationType::Update => {
		// 				if let Some(table) =
		// 					&change.post
		// 				{
		// 					CatalogStore::tables
        //                         .get_or_insert_with(*id, || crate::catalog::versioned::VersionedTableDef::new())
        //                         .value()
        //                         .insert(version, Some(table.clone()));
		//
		// 					// Update name index
		// 					if let Some(pre_table) =
		// 						&change.pre
		// 					{
		// 						CatalogStore::tables_by_name
        //                             .remove(&(pre_table.schema, pre_table.name.clone()));
		// 					}
		// 					CatalogStore::tables_by_name
        //                         .insert((table.schema, table.name.clone()), table.id);
		// 				}
		// 			}
		// 			OperationType::Delete => {
		// 				if let Some(entry) = self
		// 					.catalog
		// 					.tables
		// 					.get(id)
		// 				{
		// 					entry.value().insert(
		// 						version, None,
		// 					);
		// 				}
		// 				if let Some(table) = &change.pre
		// 				{
		// 					CatalogStore::tables_by_name
        //                         .remove(&(table.schema, table.name.clone()));
		// 				}
		// 			}
		// 		}
		// 	}
		//
		// 	// Apply view changes
		// 	for (id, change) in changes.view_def() {
		// 		match change.op {
		// 			OperationType::Create
		// 			| OperationType::Update => {
		// 				if let Some(view) = &change.post
		// 				{
		// 					CatalogStore::views
        //                         .get_or_insert_with(*id, || crate::catalog::versioned::VersionedViewDef::new())
        //                         .value()
        //                         .insert(version, Some(view.clone()));
		//
		// 					// Update name index
		// 					if let Some(pre_view) =
		// 						&change.pre
		// 					{
		// 						CatalogStore::views_by_name
        //                             .remove(&(pre_view.schema, pre_view.name.clone()));
		// 					}
		// 					CatalogStore::views_by_name
        //                         .insert((view.schema, view.name.clone()), view.id);
		// 				}
		// 			}
		// 			OperationType::Delete => {
		// 				if let Some(entry) = self
		// 					.catalog
		// 					.views
		// 					.get(id)
		// 				{
		// 					entry.value().insert(
		// 						version, None,
		// 					);
		// 				}
		// 				if let Some(view) = &change.pre
		// 				{
		// 					CatalogStore::views_by_name
        //                         .remove(&(view.schema, view.name.clone()));
		// 				}
		// 			}
		// 		}
		// 	}
		// }

		Ok(())
	}
}
