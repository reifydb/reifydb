// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, NamespaceDef, NamespaceId, QueryTransaction, TransactionalChanges,
	TransactionalNamespaceChanges,
	interceptor::{NamespaceDefInterceptor, WithInterceptors},
};
use reifydb_type::{
	IntoFragment,
	diagnostic::catalog::{namespace_already_exists, namespace_not_found},
	error, internal, return_error,
};
use tracing::{instrument, warn};

use crate::{CatalogStore, store::namespace::NamespaceToCreate, transaction::MaterializedCatalogTransaction};

pub trait CatalogNamespaceCommandOperations {
	fn create_namespace(&mut self, to_create: NamespaceToCreate) -> crate::Result<NamespaceDef>;

	// TODO: Implement when update/delete are ready
	// fn update_namespace(&mut self, namespace_id: NamespaceId, updates:
	// NamespaceUpdates) -> crate::Result<NamespaceDef>; fn
	// delete_namespace(&mut self, namespace_id: NamespaceId) ->
	// crate::Result<()>;
}

pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_def_created(&mut self, namespace: NamespaceDef) -> crate::Result<()>;

	fn track_namespace_def_updated(&mut self, pre: NamespaceDef, post: NamespaceDef) -> crate::Result<()>;

	fn track_namespace_def_deleted(&mut self, namespace: NamespaceDef) -> crate::Result<()>;
}

pub trait CatalogNamespaceQueryOperations {
	fn find_namespace(&mut self, id: NamespaceId) -> crate::Result<Option<NamespaceDef>>;

	fn find_namespace_by_name<'a>(&mut self, name: impl IntoFragment<'a>) -> crate::Result<Option<NamespaceDef>>;

	fn get_namespace(&mut self, id: NamespaceId) -> crate::Result<NamespaceDef>;

	fn get_namespace_by_name<'a>(&mut self, name: impl IntoFragment<'a>) -> crate::Result<NamespaceDef>;
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackNamespaceChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogNamespaceCommandOperations for CT
{
	#[instrument(level = "debug", skip(self, to_create))]
	fn create_namespace(&mut self, to_create: NamespaceToCreate) -> reifydb_core::Result<NamespaceDef> {
		if let Some(namespace) = self.find_namespace_by_name(&to_create.name)? {
			return_error!(namespace_already_exists(to_create.namespace_fragment, &namespace.name));
		}
		let result = CatalogStore::create_namespace(self, to_create)?;
		self.track_namespace_def_created(result.clone())?;
		NamespaceDefInterceptor::post_create(self, &result)?;
		Ok(result)
	}
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges> CatalogNamespaceQueryOperations
	for QT
{
	#[instrument(level = "trace", skip(self))]
	fn find_namespace(&mut self, id: NamespaceId) -> reifydb_core::Result<Option<NamespaceDef>> {
		// 1. Check transactional changes first
		if let Some(namespace) = TransactionalNamespaceChanges::find_namespace(self, id) {
			return Ok(Some(namespace.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalNamespaceChanges::is_namespace_deleted(self, id) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(namespace) = self.catalog().find_namespace(id, self.version()) {
			return Ok(Some(namespace));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(namespace) = CatalogStore::find_namespace(self, id)? {
			warn!("Namespace with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(namespace));
		}

		Ok(None)
	}

	#[instrument(level = "trace", skip(self, name))]
	fn find_namespace_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<Option<NamespaceDef>> {
		let name = name.into_fragment();

		// 1. Check transactional changes first
		// nop for QueryTransaction
		if let Some(namespace) = TransactionalNamespaceChanges::find_namespace_by_name(self, name.as_borrowed())
		{
			return Ok(Some(namespace.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalNamespaceChanges::is_namespace_deleted_by_name(self, name.as_borrowed()) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(namespace) = self.catalog().find_namespace_by_name(name.text(), self.version()) {
			return Ok(Some(namespace));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(namespace) = CatalogStore::find_namespace_by_name(self, name.text())? {
			warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name.text());
			return Ok(Some(namespace));
		}

		Ok(None)
	}

	#[instrument(level = "trace", skip(self))]
	fn get_namespace(&mut self, id: NamespaceId) -> reifydb_core::Result<NamespaceDef> {
		self.find_namespace(id)?.ok_or_else(|| {
			error!(internal!(
				"Namespace with ID {} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(level = "trace", skip(self, name))]
	fn get_namespace_by_name<'a>(&mut self, name: impl IntoFragment<'a>) -> reifydb_core::Result<NamespaceDef> {
		let name = name.into_fragment();
		self.find_namespace_by_name(name.as_borrowed())?
			.ok_or_else(|| error!(namespace_not_found(name.as_borrowed(), name.text())))
	}
}
