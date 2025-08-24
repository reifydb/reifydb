// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	interceptor::{
		PostCommitContext, PostCommitInterceptor


		,
	},
	interface::CommandTransaction
	,
};

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
		// if let Some(ref changes) = ctx.catalogchanges {
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
