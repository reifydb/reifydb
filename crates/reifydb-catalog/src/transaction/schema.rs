// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::schema_already_exists,
	interface::{
		CommandTransaction, SchemaDef, SchemaId, WithHooks,
		interceptor::{SchemaDefInterceptor, WithInterceptors},
	},
	log_warn, return_error,
};

use crate::{
	CatalogCommandTransactionOperations, CatalogQueryTransactionOperations,
	CatalogSchemaCommandOperations, CatalogSchemaQueryOperations,
	CatalogStore, TransactionalChangesExt, schema::SchemaToCreate,
};

// Query operations implementation
impl<T> CatalogSchemaQueryOperations for T
where
	T: CommandTransaction
		+ CatalogCommandTransactionOperations
		+ TransactionalChangesExt,
{
	fn find_schema_by_name(
		&mut self,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SchemaDef>> {
		let name = name.as_ref();

		// 1. Check transactional changes first
		if let Some(schema) =
			self.get_changes().find_schema_by_name(name)
		{
			return Ok(Some(schema.clone()));
		}

		if self.get_changes().is_schema_deleted_by_name(name) {
			return Ok(None);
		}

		// 2. Check MaterializedCatalog
		if let Some(schema) = self.catalog().find_schema_by_name(
			name,
			<T as CatalogQueryTransactionOperations>::version(self),
		) {
			return Ok(Some(schema));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(schema) =
			CatalogStore::find_schema_by_name(self, name)?
		{
			log_warn!(
				"Schema '{}' found in storage but not in MaterializedCatalog",
				name
			);
			return Ok(Some(schema));
		}

		Ok(None)
	}

	fn find_schema(
		&mut self,
		id: SchemaId,
	) -> crate::Result<Option<SchemaDef>> {
		// 1. Check transactional changes first
		if let Some(schema) = self.get_changes().get_schema_def(id) {
			return Ok(Some(schema.clone()));
		}

		// 2. Check MaterializedCatalog
		if let Some(schema) = self.catalog().find_schema(
			id,
			<T as CatalogQueryTransactionOperations>::version(self),
		) {
			return Ok(Some(schema));
		}

		// 3. Fall back to storage as defensive measure
		if let Some(schema) = CatalogStore::find_schema(self, id)? {
			log_warn!(
				"Schema with ID {:?} found in storage but not in MaterializedCatalog",
				id
			);
			return Ok(Some(schema));
		}

		Ok(None)
	}

	fn get_schema(&mut self, id: SchemaId) -> crate::Result<SchemaDef> {
		use reifydb_core::{error, internal_error};

		self.find_schema(id)?
			.ok_or_else(|| {
				error!(internal_error!(
					"Schema with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
					id
				))
			})
	}
}

// Command operations implementation
impl<T> CatalogSchemaCommandOperations for T
where
	T: CommandTransaction
		+ CatalogCommandTransactionOperations
		+ CatalogSchemaQueryOperations
		+ WithInterceptors<T>
		+ WithHooks
		+ SchemaDefInterceptor<T>,
{
	fn create_schema(
		&mut self,
		to_create: SchemaToCreate,
	) -> crate::Result<SchemaDef> {
		if let Some(schema) =
			self.find_schema_by_name(&to_create.name)?
		{
			return_error!(schema_already_exists(
				to_create.schema_fragment,
				&schema.name
			));
		}
		let result = CatalogStore::create_schema(self, to_create)?;
		self.track_schema_def_created(result.clone())?;
		SchemaDefInterceptor::post_create(self, &result)?;
		Ok(result)
	}
}
