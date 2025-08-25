// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::table_already_exists,
	interface::{
		CommandTransaction, SchemaId, TableDef, TableId, WithHooks,
		interceptor::{TableDefInterceptor, WithInterceptors},
	},
	log_warn, return_error,
};

use crate::{
	CatalogSchemaOperations, CatalogStore, CatalogTableOperations,
	CatalogTransactionContext, TransactionalChangesExt,
	table::TableToCreate,
};

impl<T> CatalogTableOperations for T
where
	T: CommandTransaction
		+ CatalogTransactionContext
		+ CatalogSchemaOperations
		+ WithInterceptors<T>
		+ WithHooks
		+ TableDefInterceptor<T>,
{
	fn create_table(
		&mut self,
		to_create: TableToCreate,
	) -> crate::Result<TableDef> {
		if let Some(table) = self.find_table_by_name(
			to_create.schema,
			&to_create.table,
		)? {
			let schema = self.get_schema(to_create.schema)?;

			return_error!(table_already_exists(
				to_create.fragment,
				&schema.name,
				&table.name
			));
		}

		let result = CatalogStore::create_table(self, to_create)?;
		self.track_table_created(result.clone())?;
		TableDefInterceptor::post_create(self, &result)?;

		Ok(result)
	}

	fn find_table_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>> {
		let name = name.as_ref();

		// 1. Check transactional changes first
		if let Some(table) =
			self.get_changes().find_table_by_name(schema, name)
		{
			return Ok(Some(table.clone()));
		}

		if self.get_changes().is_table_deleted_by_name(schema, name) {
			return Ok(None);
		}

		// 2. Check MaterializedCatalog
		if let Some(table) = self.catalog().find_table_by_name(
			schema,
			name,
			CatalogTransactionContext::version(self),
		) {
			return Ok(Some(table));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(table) =
			CatalogStore::find_table_by_name(self, schema, name)?
		{
			log_warn!(
				"Table '{}' in schema {:?} found in storage but not in MaterializedCatalog",
				name,
				schema
			);
			return Ok(Some(table));
		}

		Ok(None)
	}

	fn find_table(
		&mut self,
		id: TableId,
	) -> crate::Result<Option<TableDef>> {
		// 1. Check transactional changes first
		if let Some(change) = self.get_changes().table_def.get(&id) {
			return Ok(change.post.clone());
		}

		// 2. Check MaterializedCatalog
		if let Some(table) = self.catalog().find_table(
			id,
			CatalogTransactionContext::version(self),
		) {
			return Ok(Some(table));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(table) = CatalogStore::find_table(self, id)? {
			log_warn!(
				"Table with ID {:?} found in storage but not in MaterializedCatalog",
				id
			);
			return Ok(Some(table));
		}

		Ok(None)
	}
}
