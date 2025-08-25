// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::view_already_exists,
	interface::{
		CommandTransaction, SchemaId, ViewDef, ViewId, WithHooks,
		interceptor::{ViewDefInterceptor, WithInterceptors},
	},
	log_warn, return_error,
};

use crate::{
	CatalogSchemaDefOperations, CatalogStore, CatalogTransactionOperations,
	CatalogViewDefOperations, TransactionalChangesExt, view::ViewToCreate,
};

impl<T> CatalogViewDefOperations for T
where
	T: CommandTransaction
		+ CatalogTransactionOperations
		+ CatalogSchemaDefOperations
		+ WithInterceptors<T>
		+ WithHooks
		+ ViewDefInterceptor<T>,
{
	fn create_view(
		&mut self,
		to_create: ViewToCreate,
	) -> crate::Result<ViewDef> {
		if let Some(view) = self
			.find_view_by_name(to_create.schema, &to_create.name)?
		{
			let schema = self.get_schema(to_create.schema)?;
			return_error!(view_already_exists(
				to_create.fragment,
				&schema.name,
				&view.name
			));
		}

		let result =
			CatalogStore::create_deferred_view(self, to_create)?;
		self.track_view_def_created(result.clone())?;
		ViewDefInterceptor::post_create(self, &result)?;

		Ok(result)
	}

	fn find_view_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();

		// 1. Check transactional changes first
		if let Some(view) =
			self.get_changes().find_view_by_name(schema, name)
		{
			return Ok(Some(view.clone()));
		}

		if self.get_changes().is_view_deleted_by_name(schema, name) {
			return Ok(None);
		}

		// 2. Check MaterializedCatalog
		if let Some(view) = self.catalog().find_view_by_name(
			schema,
			name,
			CatalogTransactionOperations::version(self),
		) {
			return Ok(Some(view));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(view) =
			CatalogStore::find_view_by_name(self, schema, name)?
		{
			log_warn!(
				"View '{}' in schema {:?} found in storage but not in MaterializedCatalog",
				name,
				schema
			);
			return Ok(Some(view));
		}

		Ok(None)
	}

	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>> {
		// 1. Check transactional changes first
		if let Some(change) = self.get_changes().view_def.get(&id) {
			return Ok(change.post.clone());
		}

		// 2. Check MaterializedCatalog
		if let Some(view) = self.catalog().find_view(
			id,
			CatalogTransactionOperations::version(self),
		) {
			return Ok(Some(view));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(view) = CatalogStore::find_view(self, id)? {
			log_warn!(
				"View with ID {:?} found in storage but not in MaterializedCatalog",
				id
			);
			return Ok(Some(view));
		}

		Ok(None)
	}
}
