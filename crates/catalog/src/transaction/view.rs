// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, NamespaceId, QueryTransaction, ViewDef, ViewId,
};

use crate::view::ViewToCreate;

pub trait CatalogViewCommandOperations {
	fn create_view(&mut self, view: ViewToCreate)
	-> crate::Result<ViewDef>;

	// TODO: Implement when update/delete are ready
	// fn update_view(&mut self, view_id: ViewId, updates: ViewUpdates) ->
	// crate::Result<ViewDef>; fn delete_view(&mut self, view_id: ViewId)
	// -> crate::Result<()>;
}

pub trait CatalogTrackViewChangeOperations {
	fn track_view_def_created(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()>;

	fn track_view_def_updated(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()>;

	fn track_view_def_deleted(
		&mut self,
		view: ViewDef,
	) -> crate::Result<()>;
}

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

impl<T: CommandTransaction> CatalogViewCommandOperations for T {
	fn create_view(
		&mut self,
		view: ViewToCreate,
	) -> reifydb_core::Result<ViewDef> {
		todo!()
	}
}

impl<T: QueryTransaction> CatalogViewQueryOperations for T {
	fn find_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> reifydb_core::Result<Option<ViewDef>> {
		todo!()
	}

	fn find_view(
		&mut self,
		id: ViewId,
	) -> reifydb_core::Result<Option<ViewDef>> {
		todo!()
	}

	fn get_view_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> reifydb_core::Result<ViewDef> {
		todo!()
	}
}

// impl<T> CatalogViewCommandOperations for T
// where
// 	T: CommandTransaction
// 		+ CatalogTrackChangeOperations
// 		+ CatalogNamespaceQueryOperations
// 		+ CatalogViewQueryOperations
// 		+ WithInterceptors<T>
// 		+ WithEventBus
// 		+ ViewDefInterceptor<T>,
// {
// 	fn create_view(
// 		&mut self,
// 		to_create: ViewToCreate,
// 	) -> crate::Result<ViewDef> {
// 		if let Some(view) = self.find_view_by_name(
// 			to_create.namespace,
// 			&to_create.name,
// 		)? {
// 			let namespace =
// 				self.get_namespace(to_create.namespace)?;
// 			return_error!(view_already_exists(
// 				to_create.fragment,
// 				&namespace.name,
// 				&view.name
// 			));
// 		}
//
// 		let result =
// 			CatalogStore::create_deferred_view(self, to_create)?;
// 		self.track_view_def_created(result.clone())?;
// 		ViewDefInterceptor::post_create(self, &result)?;
//
// 		Ok(result)
// 	}
// }
//
// // Query operations implementation
// impl<T> CatalogViewQueryOperations for T
// where
// 	T: CommandTransaction
// 		+ CatalogTrackChangeOperations
// 		+ TransactionalChangesExt,
// {
// 	fn find_view_by_name(
// 		&mut self,
// 		namespace: NamespaceId,
// 		name: impl AsRef<str>,
// 	) -> crate::Result<Option<ViewDef>> {
// 		let name = name.as_ref();
//
// 		// 1. Check transactional changes first
// 		if let Some(view) =
// 			self.get_changes().find_view_by_name(namespace, name)
// 		{
// 			return Ok(Some(view.clone()));
// 		}
//
// 		if self.get_changes().is_view_deleted_by_name(namespace, name) {
// 			return Ok(None);
// 		}
//
// 		// 2. Check MaterializedCatalog
// 		if let Some(view) = self.catalog().find_view_by_name(
// 			namespace,
// 			name,
// 			<T as CatalogTransaction>::version(self),
// 		) {
// 			return Ok(Some(view));
// 		}
//
// 		// 3. Fall back to storage as defensive measure
// 		if let Some(view) =
// 			CatalogStore::find_view_by_name(self, namespace, name)?
// 		{
// 			log_warn!(
// 				"View '{}' in namespace {:?} found in storage but not in
// MaterializedCatalog", 				name,
// 				namespace
// 			);
// 			return Ok(Some(view));
// 		}
//
// 		Ok(None)
// 	}
//
// 	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>> {
// 		// 1. Check transactional changes first
// 		if let Some(view) = self.get_changes().get_view_def(id) {
// 			return Ok(Some(view.clone()));
// 		}
//
// 		// 2. Check MaterializedCatalog
// 		if let Some(view) = self
// 			.catalog()
// 			.find_view(id, <T as CatalogTransaction>::version(self))
// 		{
// 			return Ok(Some(view));
// 		}
//
// 		// 3. Fall back to storage as defensive measure
// 		if let Some(view) = CatalogStore::find_view(self, id)? {
// 			log_warn!(
// 				"View with ID {:?} found in storage but not in MaterializedCatalog",
// 				id
// 			);
// 			return Ok(Some(view));
// 		}
//
// 		Ok(None)
// 	}
//
// 	fn get_view_by_name(
// 		&mut self,
// 		_namespace: NamespaceId,
// 		_name: impl AsRef<str>,
// 	) -> reifydb_core::Result<ViewDef> {
// 		todo!()
// 	}
// }
//
// // TODO: Add CatalogViewQueryOperations implementation for query-only
// // transactions
