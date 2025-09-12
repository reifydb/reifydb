// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion,
	interface::{
		NamespaceDef, NamespaceId, OperationType, SourceDef, SourceId,
		TableDef, TableId, TransactionalChanges, ViewDef, ViewId,
	},
};
use reifydb_type::IntoFragment;

use crate::MaterializedCatalog;

// Namespace query operations
pub trait CatalogNamespaceQueryOperations {
	fn find_namespace_by_name(
		&mut self,
		name: impl AsRef<str>,
	) -> crate::Result<Option<NamespaceDef>>;

	fn find_namespace(
		&mut self,
		id: NamespaceId,
	) -> crate::Result<Option<NamespaceDef>>;

	fn get_namespace(
		&mut self,
		id: NamespaceId,
	) -> crate::Result<NamespaceDef>;

	fn get_namespace_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a>,
	) -> crate::Result<NamespaceDef>;
}

// Source query operations
pub trait CatalogSourceQueryOperations {
	fn find_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		source: impl IntoFragment<'a>,
	) -> crate::Result<Option<SourceDef>>;

	fn find_source(
		&mut self,
		id: SourceId,
	) -> crate::Result<Option<SourceDef>>;

	fn get_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<SourceDef>;
}

// Table query operations
pub trait CatalogTableQueryOperations {
	fn find_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>>;

	fn find_table(
		&mut self,
		id: TableId,
	) -> crate::Result<Option<TableDef>>;

	fn get_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<TableDef>;
}

// View query operations
pub trait CatalogViewQueryOperations {
	fn find_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>>;

	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>>;

	fn get_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<ViewDef>;
}

// Combined catalog query transaction trait
pub trait CatalogQueryTransaction:
	CatalogNamespaceQueryOperations
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
	fn find_namespace_by_name(&self, name: &str) -> Option<&NamespaceDef>;

	fn is_namespace_deleted_by_name(&self, name: &str) -> bool;

	fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> Option<&TableDef>;

	fn is_table_deleted_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> bool;

	fn find_view_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> Option<&ViewDef>;

	fn is_view_deleted_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> bool;
}

impl TransactionalChangesExt for TransactionalChanges {
	fn find_namespace_by_name(&self, name: &str) -> Option<&NamespaceDef> {
		self.namespace_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|s| s.name == name)
		})
	}

	fn is_namespace_deleted_by_name(&self, name: &str) -> bool {
		self.namespace_def.iter().rev().any(|change| {
			change.op == OperationType::Delete
				&& change.pre.as_ref().map(|s| s.name.as_str())
					== Some(name)
		})
	}

	fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> Option<&TableDef> {
		self.table_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|t| {
				t.namespace == namespace && t.name == name
			})
		})
	}

	fn is_table_deleted_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> bool {
		self.table_def.iter().rev().any(|change| {
			change.op == OperationType::Delete
				&& change
					.pre
					.as_ref()
					.map(|t| {
						t.namespace == namespace
							&& t.name == name
					})
					.unwrap_or(false)
		})
	}

	fn find_view_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> Option<&ViewDef> {
		self.view_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|v| {
				v.namespace == namespace && v.name == name
			})
		})
	}

	fn is_view_deleted_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
	) -> bool {
		self.view_def.iter().rev().any(|change| {
			change.op == OperationType::Delete
				&& change
					.pre
					.as_ref()
					.map(|v| {
						v.namespace == namespace
							&& v.name == name
					})
					.unwrap_or(false)
		})
	}
}
