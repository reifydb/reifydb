// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
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

#[async_trait(?Send)]
pub trait CatalogNamespaceCommandOperations: Send {
	async fn create_namespace(&mut self, to_create: NamespaceToCreate) -> crate::Result<NamespaceDef>;

	// TODO: Implement when update/delete are ready
	// async fn update_namespace(&mut self, namespace_id: NamespaceId, updates:
	// NamespaceUpdates) -> crate::Result<NamespaceDef>; async fn
	// delete_namespace(&mut self, namespace_id: NamespaceId) ->
	// crate::Result<()>;
}

pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_def_created(&mut self, namespace: NamespaceDef) -> crate::Result<()>;

	fn track_namespace_def_updated(&mut self, pre: NamespaceDef, post: NamespaceDef) -> crate::Result<()>;

	fn track_namespace_def_deleted(&mut self, namespace: NamespaceDef) -> crate::Result<()>;
}

#[async_trait(?Send)]
pub trait CatalogNamespaceQueryOperations: Send {
	async fn find_namespace(&mut self, id: NamespaceId) -> crate::Result<Option<NamespaceDef>>;

	async fn find_namespace_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a> + Send,
	) -> crate::Result<Option<NamespaceDef>>;

	async fn get_namespace(&mut self, id: NamespaceId) -> crate::Result<NamespaceDef>;

	async fn get_namespace_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a> + Send,
	) -> crate::Result<NamespaceDef>;
}

#[async_trait(?Send)]
impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackNamespaceChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges
		+ Send,
> CatalogNamespaceCommandOperations for CT
{
	#[instrument(name = "catalog::namespace::create", level = "debug", skip(self, to_create))]
	async fn create_namespace(&mut self, to_create: NamespaceToCreate) -> reifydb_core::Result<NamespaceDef> {
		if let Some(namespace) = self.find_namespace_by_name(&to_create.name).await? {
			return_error!(namespace_already_exists(to_create.namespace_fragment, &namespace.name));
		}
		let result = CatalogStore::create_namespace(self, to_create).await?;
		self.track_namespace_def_created(result.clone())?;
		NamespaceDefInterceptor::post_create(self, &result).await?;
		Ok(result)
	}
}

#[async_trait(?Send)]
impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges + Send>
	CatalogNamespaceQueryOperations for QT
{
	#[instrument(name = "catalog::namespace::find", level = "trace", skip(self))]
	async fn find_namespace(&mut self, id: NamespaceId) -> reifydb_core::Result<Option<NamespaceDef>> {
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
		if let Some(namespace) = CatalogStore::find_namespace(self, id).await? {
			warn!("Namespace with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(namespace));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::namespace::find_by_name", level = "trace", skip(self, name))]
	async fn find_namespace_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a> + Send,
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
		if let Some(namespace) = CatalogStore::find_namespace_by_name(self, name.text()).await? {
			warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name.text());
			return Ok(Some(namespace));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::namespace::get", level = "trace", skip(self))]
	async fn get_namespace(&mut self, id: NamespaceId) -> reifydb_core::Result<NamespaceDef> {
		self.find_namespace(id).await?.ok_or_else(|| {
			error!(internal!(
				"Namespace with ID {} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::namespace::get_by_name", level = "trace", skip(self, name))]
	async fn get_namespace_by_name<'a>(
		&mut self,
		name: impl IntoFragment<'a> + Send,
	) -> reifydb_core::Result<NamespaceDef> {
		let name = name.into_fragment();
		self.find_namespace_by_name(name.as_borrowed())
			.await?
			.ok_or_else(|| error!(namespace_not_found(name.as_borrowed(), name.text())))
	}
}
