// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod namespace;
mod source;
mod table;
mod view;

pub trait CatalogCommandTransaction:
	CatalogQueryTransaction
	+ CatalogNamespaceCommandOperations
	+ CatalogTableCommandOperations
	+ CatalogViewCommandOperations
{
}

pub trait CatalogTrackChangeOperations:
	CatalogTrackNamespaceChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackViewChangeOperations
{
}

pub trait CatalogQueryTransaction:
	CatalogNamespaceQueryOperations
	+ CatalogSourceQueryOperations
	+ CatalogTableQueryOperations
	+ CatalogViewQueryOperations
{
}

impl<T: QueryTransaction> CatalogQueryTransaction for T {}
impl<T: CommandTransaction> CatalogCommandTransaction for T {}

pub use namespace::{
	CatalogNamespaceCommandOperations, CatalogNamespaceQueryOperations,
	CatalogTrackNamespaceChangeOperations,
};
use reifydb_core::interface::{CommandTransaction, QueryTransaction};
pub use source::CatalogSourceQueryOperations;
pub use table::{
	CatalogTableCommandOperations, CatalogTableQueryOperations,
	CatalogTrackTableChangeOperations,
};
pub use view::{
	CatalogTrackViewChangeOperations, CatalogViewCommandOperations,
	CatalogViewQueryOperations,
};

// Extension trait for TransactionalChanges with catalog-specific helpers
// pub trait TransactionalChangesExt {
// 	fn find_namespace_by_name(&self, name: &str) -> Option<&NamespaceDef>;
//
// 	fn is_namespace_deleted_by_name(&self, name: &str) -> bool;
//
// 	fn find_table_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> Option<&TableDef>;
//
// 	fn is_table_deleted_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> bool;
//
// 	fn find_view_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> Option<&ViewDef>;
//
// 	fn is_view_deleted_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> bool;
// }
//
// impl TransactionalChangesExt for TransactionalChanges {
// 	fn find_namespace_by_name(&self, name: &str) -> Option<&NamespaceDef> {
// 		self.namespace_def.iter().rev().find_map(|change| {
// 			change.post.as_ref().filter(|s| s.name == name)
// 		})
// 	}
//
// 	fn is_namespace_deleted_by_name(&self, name: &str) -> bool {
// 		self.namespace_def.iter().rev().any(|change| {
// 			change.op == OperationType::Delete
// 				&& change.pre.as_ref().map(|s| s.name.as_str())
// 					== Some(name)
// 		})
// 	}
//
// 	fn find_table_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> Option<&TableDef> {
// 		self.table_def.iter().rev().find_map(|change| {
// 			change.post.as_ref().filter(|t| {
// 				t.namespace == namespace && t.name == name
// 			})
// 		})
// 	}
//
// 	fn is_table_deleted_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> bool {
// 		self.table_def.iter().rev().any(|change| {
// 			change.op == OperationType::Delete
// 				&& change
// 					.pre
// 					.as_ref()
// 					.map(|t| {
// 						t.namespace == namespace
// 							&& t.name == name
// 					})
// 					.unwrap_or(false)
// 		})
// 	}
//
// 	fn find_view_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> Option<&ViewDef> {
// 		self.view_def.iter().rev().find_map(|change| {
// 			change.post.as_ref().filter(|v| {
// 				v.namespace == namespace && v.name == name
// 			})
// 		})
// 	}
//
// 	fn is_view_deleted_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> bool {
// 		self.view_def.iter().rev().any(|change| {
// 			change.op == OperationType::Delete
// 				&& change
// 					.pre
// 					.as_ref()
// 					.map(|v| {
// 						v.namespace == namespace
// 							&& v.name == name
// 					})
// 					.unwrap_or(false)
// 		})
// 	}
// }
