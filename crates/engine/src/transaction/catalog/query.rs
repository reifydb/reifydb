// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogNamespaceQueryOperations, CatalogQueryTransaction,
	CatalogTableQueryOperations, CatalogTransaction,
	CatalogViewQueryOperations, MaterializedCatalog,
	transaction::CatalogSourceQueryOperations,
};
use reifydb_core::{
	CommitVersion,
	interface::{
		NamespaceDef, NamespaceId, SourceDef, SourceId, TableDef,
		TableId, Transaction, VersionedQueryTransaction, ViewDef,
		ViewId,
	},
};
use reifydb_type::{
	IntoFragment, diagnostic::catalog::namespace_not_found, return_error,
};

use crate::StandardQueryTransaction;

// Implement CatalogQueryTransactionOperations for StandardQueryTransaction
impl<T: Transaction> CatalogTransaction for StandardQueryTransaction<T> {
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}

	fn version(&self) -> CommitVersion {
		self.versioned.version()
	}
}

impl<T: Transaction> CatalogNamespaceQueryOperations
	for StandardQueryTransaction<T>
{
	fn find_namespace_by_name(
		&mut self,
		name: impl AsRef<str>,
	) -> crate::Result<Option<NamespaceDef>> {
		let name = name.as_ref();

		Ok(self.catalog.find_namespace_by_name(
			name,
			VersionedQueryTransaction::version(self),
		))
	}

	fn find_namespace(
		&mut self,
		id: NamespaceId,
	) -> crate::Result<Option<NamespaceDef>> {
		Ok(self.catalog.find_namespace(
			id,
			VersionedQueryTransaction::version(self),
		))
	}

	fn get_namespace(
		&mut self,
		id: NamespaceId,
	) -> crate::Result<NamespaceDef> {
		use reifydb_type::{error, internal_error};

		self.find_namespace(id)?
			.ok_or_else(|| {
				error!(internal_error!(
					"Namespace with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
					id
				))
			})
	}

	fn get_namespace_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<NamespaceDef> {
		let name = name.into_fragment();

		if let Some(result) =
			self.find_namespace_by_name(name.text())?
		{
			return Ok(result);
		}

		let text = name.clone();
		let text = text.text();
		return_error!(namespace_not_found(name, text));
	}
}

impl<T: Transaction> CatalogSourceQueryOperations
	for StandardQueryTransaction<T>
{
	fn find_source(
		&mut self,
		_id: SourceId,
	) -> reifydb_core::Result<Option<SourceDef>> {
		todo!()
	}

	fn find_source_by_name<'a>(
		&mut self,
		_namespace: NamespaceId,
		_source: impl IntoFragment<'a>,
	) -> reifydb_core::Result<Option<SourceDef>> {
		todo!()
	}

	fn get_source_by_name<'a>(
		&mut self,
		_namespace: NamespaceId,
		_name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<SourceDef> {
		todo!()
	}
}

impl<T: Transaction> CatalogTableQueryOperations
	for StandardQueryTransaction<T>
{
	fn find_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>> {
		let name = name.as_ref();

		Ok(self.catalog.find_table_by_name(
			namespace,
			name,
			VersionedQueryTransaction::version(self),
		))
	}

	fn find_table(
		&mut self,
		id: TableId,
	) -> crate::Result<Option<TableDef>> {
		Ok(self.catalog.find_table(
			id,
			VersionedQueryTransaction::version(self),
		))
	}

	fn get_table_by_name(
		&mut self,
		_namespace: NamespaceId,
		_name: impl AsRef<str>,
	) -> reifydb_core::Result<TableDef> {
		todo!()
	}
}

impl<T: Transaction> CatalogViewQueryOperations
	for StandardQueryTransaction<T>
{
	fn find_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();

		Ok(self.catalog.find_view_by_name(
			namespace,
			name,
			VersionedQueryTransaction::version(self),
		))
	}

	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>> {
		Ok(self.catalog.find_view(
			id,
			VersionedQueryTransaction::version(self),
		))
	}

	fn get_view_by_name(
		&mut self,
		_namespace: NamespaceId,
		_name: impl AsRef<str>,
	) -> reifydb_core::Result<ViewDef> {
		todo!()
	}
}

impl<T: Transaction> CatalogQueryTransaction for StandardQueryTransaction<T> {}
