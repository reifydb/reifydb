// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::namespace_already_exists,
	interface::{
		CommandTransaction, NamespaceDef, NamespaceId, WithEventBus,
		interceptor::{NamespaceDefInterceptor, WithInterceptors},
	},
	log_warn, return_error,
};
use reifydb_type::{
	IntoFragment, diagnostic::catalog::namespace_not_found, internal_error,
};

use crate::{
	CatalogCommandTransactionOperations, CatalogNamespaceCommandOperations,
	CatalogNamespaceQueryOperations, CatalogStore, CatalogTransaction,
	TransactionalChangesExt, namespace::NamespaceToCreate,
};

// Query operations implementation
impl<T> CatalogNamespaceQueryOperations for T
where
	T: CommandTransaction
		+ CatalogCommandTransactionOperations
		+ TransactionalChangesExt,
{
	fn find_namespace_by_name(
		&mut self,
		name: impl AsRef<str>,
	) -> crate::Result<Option<NamespaceDef>> {
		let name = name.as_ref();

		// 1. Check transactional changes first
		if let Some(namespace) =
			self.get_changes().find_namespace_by_name(name)
		{
			return Ok(Some(namespace.clone()));
		}

		if self.get_changes().is_namespace_deleted_by_name(name) {
			return Ok(None);
		}

		// 2. Check MaterializedCatalog
		if let Some(namespace) = self.catalog().find_namespace_by_name(
			name,
			<T as CatalogTransaction>::version(self),
		) {
			return Ok(Some(namespace));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(namespace) =
			CatalogStore::find_namespace_by_name(self, name)?
		{
			log_warn!(
				"Namespace '{}' found in storage but not in MaterializedCatalog",
				name
			);
			return Ok(Some(namespace));
		}

		Ok(None)
	}

	fn find_namespace(
		&mut self,
		id: NamespaceId,
	) -> crate::Result<Option<NamespaceDef>> {
		// 1. Check transactional changes first
		if let Some(namespace) =
			self.get_changes().get_namespace_def(id)
		{
			return Ok(Some(namespace.clone()));
		}

		// 2. Check MaterializedCatalog
		if let Some(namespace) = self.catalog().find_namespace(
			id,
			<T as CatalogTransaction>::version(self),
		) {
			return Ok(Some(namespace));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(namespace) = CatalogStore::find_namespace(self, id)?
		{
			log_warn!(
				"Namespace with ID {:?} found in storage but not in MaterializedCatalog",
				id
			);
			return Ok(Some(namespace));
		}

		Ok(None)
	}

	fn get_namespace(
		&mut self,
		id: NamespaceId,
	) -> crate::Result<NamespaceDef> {
		use reifydb_core::error;

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
	) -> crate::Result<NamespaceDef> {
		let name = name.into_fragment();

		if let Some(result) =
			self.find_namespace_by_name(name.text())?
		{
			return Ok(result);
		}

		let binding = name.clone();
		let text = binding.text();
		return_error!(namespace_not_found(name, text))
	}
}

// Command operations implementation
impl<T> CatalogNamespaceCommandOperations for T
where
	T: CommandTransaction
		+ CatalogCommandTransactionOperations
		+ CatalogNamespaceQueryOperations
		+ WithInterceptors<T>
		+ WithEventBus
		+ NamespaceDefInterceptor<T>,
{
	fn create_namespace(
		&mut self,
		to_create: NamespaceToCreate,
	) -> crate::Result<NamespaceDef> {
		if let Some(namespace) =
			self.find_namespace_by_name(&to_create.name)?
		{
			return_error!(namespace_already_exists(
				to_create.namespace_fragment,
				&namespace.name
			));
		}
		let result = CatalogStore::create_namespace(self, to_create)?;
		self.track_namespace_def_created(result.clone())?;
		NamespaceDefInterceptor::post_create(self, &result)?;
		Ok(result)
	}
}
