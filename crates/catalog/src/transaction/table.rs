// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::table_already_exists,
	interface::{
		CommandTransaction, SchemaId, TableDef, TableId, WithEventBus,
		interceptor::{TableDefInterceptor, WithInterceptors},
	},
	log_warn, return_error,
};

use crate::{
	CatalogCommandTransactionOperations, CatalogSchemaQueryOperations,
	CatalogStore, CatalogTableCommandOperations,
	CatalogTableQueryOperations, CatalogTransaction,
	TransactionalChangesExt, table::TableToCreate,
};

impl<T> CatalogTableCommandOperations for T
where
	T: CommandTransaction
		+ CatalogCommandTransactionOperations
		+ CatalogSchemaQueryOperations
		+ CatalogTableQueryOperations
		+ WithInterceptors<T>
		+ WithEventBus
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
		self.track_table_def_created(result.clone())?;
		TableDefInterceptor::post_create(self, &result)?;

		Ok(result)
	}
}

// Query operations implementation
impl<T> CatalogTableQueryOperations for T
where
	T: CommandTransaction
		+ CatalogCommandTransactionOperations
		+ TransactionalChangesExt,
{
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
			<T as CatalogTransaction>::version(self),
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
		if let Some(table) = self.get_changes().get_table_def(id) {
			return Ok(Some(table.clone()));
		}

		// 2. Check MaterializedCatalog
		if let Some(table) = self.catalog().find_table(
			id,
			<T as CatalogTransaction>::version(self),
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

	fn get_table_by_name(
		&mut self,
		_schema: SchemaId,
		_name: impl AsRef<str>,
	) -> reifydb_core::Result<TableDef> {
		todo!()
	}
}

// TODO: Add CatalogTableQueryOperations implementation for query-only
// transactions
