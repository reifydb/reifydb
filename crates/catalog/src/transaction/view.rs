// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, NamespaceId, QueryTransaction, TransactionalChanges, TransactionalViewChanges, ViewDef,
	ViewId,
	interceptor::{ViewDefInterceptor, WithInterceptors},
};
use reifydb_type::{
	IntoFragment,
	diagnostic::catalog::{view_already_exists, view_not_found},
	error, internal, return_error,
};
use tracing::{instrument, warn};

use crate::{
	CatalogNamespaceQueryOperations, CatalogStore, store::view::ViewToCreate,
	transaction::MaterializedCatalogTransaction,
};

pub trait CatalogViewCommandOperations {
	fn create_view(&mut self, view: ViewToCreate) -> crate::Result<ViewDef>;

	// TODO: Implement when update/delete are ready
	// fn update_view(&mut self, view_id: ViewId, updates: ViewUpdates) ->
	// crate::Result<ViewDef>; fn delete_view(&mut self, view_id: ViewId)
	// -> crate::Result<()>;
}

pub trait CatalogTrackViewChangeOperations {
	fn track_view_def_created(&mut self, view: ViewDef) -> crate::Result<()>;

	fn track_view_def_updated(&mut self, pre: ViewDef, post: ViewDef) -> crate::Result<()>;

	fn track_view_def_deleted(&mut self, view: ViewDef) -> crate::Result<()>;
}

pub trait CatalogViewQueryOperations: CatalogNamespaceQueryOperations {
	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>>;

	fn find_view_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<ViewDef>>;

	fn get_view(&mut self, id: ViewId) -> crate::Result<ViewDef>;

	fn get_view_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<ViewDef>;
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackViewChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogViewCommandOperations for CT
{
	#[instrument(name = "catalog::view::create", level = "debug", skip(self, to_create))]
	fn create_view(&mut self, to_create: ViewToCreate) -> reifydb_core::Result<ViewDef> {
		if let Some(view) = self.find_view_by_name(to_create.namespace, &to_create.name)? {
			let namespace = self.get_namespace(to_create.namespace)?;
			return_error!(view_already_exists(to_create.fragment, &namespace.name, &view.name));
		}
		let result = CatalogStore::create_deferred_view(self, to_create)?;
		self.track_view_def_created(result.clone())?;
		ViewDefInterceptor::post_create(self, &result)?;
		Ok(result)
	}
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges> CatalogViewQueryOperations for QT {
	#[instrument(name = "catalog::view::find", level = "trace", skip(self))]
	fn find_view(&mut self, id: ViewId) -> reifydb_core::Result<Option<ViewDef>> {
		// 1. Check transactional changes first
		// nop for QueryTransaction
		if let Some(view) = TransactionalViewChanges::find_view(self, id) {
			return Ok(Some(view.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalViewChanges::is_view_deleted(self, id) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(view) = self.catalog().find_view(id, self.version()) {
			return Ok(Some(view));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(view) = CatalogStore::find_view(self, id)? {
			warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(view));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::view::find_by_name", level = "trace", skip(self, name))]
	fn find_view_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<Option<ViewDef>> {
		let name = name.into_fragment();

		// 1. Check transactional changes first
		// nop for QueryTransaction
		if let Some(view) = TransactionalViewChanges::find_view_by_name(self, namespace, name.as_borrowed()) {
			return Ok(Some(view.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalViewChanges::is_view_deleted_by_name(self, namespace, name.as_borrowed()) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(view) = self.catalog().find_view_by_name(namespace, name.text(), self.version()) {
			return Ok(Some(view));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(view) = CatalogStore::find_view_by_name(self, namespace, name.text())? {
			warn!(
				"View '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
				name.text(),
				namespace
			);
			return Ok(Some(view));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::view::get", level = "trace", skip(self))]
	fn get_view(&mut self, id: ViewId) -> reifydb_core::Result<ViewDef> {
		self.find_view(id)?.ok_or_else(|| {
			error!(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::view::get_by_name", level = "trace", skip(self, name))]
	fn get_view_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<ViewDef> {
		let name = name.into_fragment();

		let namespace_name = self.get_namespace(namespace)?.name;

		self.find_view_by_name(namespace, name.as_borrowed())?
			.ok_or_else(|| error!(view_not_found(name.as_borrowed(), &namespace_name, name.text())))
	}
}
