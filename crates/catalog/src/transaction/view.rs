// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::interface::{
	CommandTransaction, NamespaceId, QueryTransaction, TransactionalChanges, TransactionalViewChanges, ViewDef,
	ViewId,
	interceptor::{ViewDefInterceptor, WithInterceptors},
};
use reifydb_type::{
	Fragment,
	diagnostic::catalog::{view_already_exists, view_not_found},
	error, internal, return_error,
};
use tracing::{instrument, warn};

use crate::{
	CatalogNamespaceQueryOperations, CatalogStore, store::view::ViewToCreate,
	transaction::MaterializedCatalogTransaction,
};

#[async_trait]
pub trait CatalogViewCommandOperations: Send {
	async fn create_view(&mut self, view: ViewToCreate) -> crate::Result<ViewDef>;

	// TODO: Implement when update/delete are ready
	// async fn update_view(&mut self, view_id: ViewId, updates: ViewUpdates) ->
	// crate::Result<ViewDef>; async fn delete_view(&mut self, view_id: ViewId)
	// -> crate::Result<()>;
}

pub trait CatalogTrackViewChangeOperations {
	fn track_view_def_created(&mut self, view: ViewDef) -> crate::Result<()>;

	fn track_view_def_updated(&mut self, pre: ViewDef, post: ViewDef) -> crate::Result<()>;

	fn track_view_def_deleted(&mut self, view: ViewDef) -> crate::Result<()>;
}

#[async_trait]
pub trait CatalogViewQueryOperations: CatalogNamespaceQueryOperations {
	async fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>>;

	async fn find_view_by_name(&mut self, namespace: NamespaceId, name: &str) -> crate::Result<Option<ViewDef>>;

	async fn get_view(&mut self, id: ViewId) -> crate::Result<ViewDef>;

	async fn get_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<ViewDef>;
}

#[async_trait]
impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackViewChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges
		+ Send
		+ 'static,
> CatalogViewCommandOperations for CT
{
	#[instrument(name = "catalog::view::create", level = "debug", skip(self, to_create))]
	async fn create_view(&mut self, to_create: ViewToCreate) -> reifydb_core::Result<ViewDef> {
		if let Some(view) = self.find_view_by_name(to_create.namespace, to_create.name.as_str()).await? {
			let namespace = self.get_namespace(to_create.namespace).await?;
			return_error!(view_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&view.name
			));
		}
		let result = CatalogStore::create_deferred_view(self, to_create).await?;
		self.track_view_def_created(result.clone())?;
		ViewDefInterceptor::post_create(self, &result).await?;
		Ok(result)
	}
}

#[async_trait]
impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges + Send + 'static>
	CatalogViewQueryOperations for QT
{
	#[instrument(name = "catalog::view::find", level = "trace", skip(self))]
	async fn find_view(&mut self, id: ViewId) -> reifydb_core::Result<Option<ViewDef>> {
		// 1. Check transactional changes first
		// nop for MultiVersionQueryTransaction
		if let Some(view) = TransactionalViewChanges::find_view(self, id) {
			return Ok(Some(view.clone()));
		}

		// 2. Check if deleted
		// nop for MultiVersionQueryTransaction
		if TransactionalViewChanges::is_view_deleted(self, id) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(view) = self.catalog().find_view(id, self.version()) {
			return Ok(Some(view));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(view) = CatalogStore::find_view(self, id).await? {
			warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(view));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::view::find_by_name", level = "trace", skip(self, name))]
	async fn find_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: &str,
	) -> reifydb_core::Result<Option<ViewDef>> {
		// 1. Check transactional changes first
		// nop for MultiVersionQueryTransaction
		if let Some(view) = TransactionalViewChanges::find_view_by_name(self, namespace, name) {
			return Ok(Some(view.clone()));
		}

		// 2. Check if deleted
		// nop for MultiVersionQueryTransaction
		if TransactionalViewChanges::is_view_deleted_by_name(self, namespace, name) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(view) = self.catalog().find_view_by_name(namespace, name, self.version()) {
			return Ok(Some(view));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(view) = CatalogStore::find_view_by_name(self, namespace, name).await? {
			warn!(
				"View '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
				name, namespace
			);
			return Ok(Some(view));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::view::get", level = "trace", skip(self))]
	async fn get_view(&mut self, id: ViewId) -> reifydb_core::Result<ViewDef> {
		self.find_view(id).await?.ok_or_else(|| {
			error!(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::view::get_by_name", level = "trace", skip(self, name))]
	async fn get_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> reifydb_core::Result<ViewDef> {
		let name = name.into();

		let namespace_name = self.get_namespace(namespace).await?.name;

		self.find_view_by_name(namespace, name.text())
			.await?
			.ok_or_else(|| error!(view_not_found(name.clone(), &namespace_name, name.text())))
	}
}
