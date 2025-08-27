// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogQueryTransaction, CatalogQueryTransactionOperations,
	CatalogSchemaQueryOperations, CatalogTableQueryOperations,
	CatalogViewQueryOperations, MaterializedCatalog,
};
use reifydb_core::{
	Version,
	interface::{
		SchemaDef, SchemaId, TableDef, TableId, Transaction,
		VersionedQueryTransaction, ViewDef, ViewId,
	},
};

use crate::StandardQueryTransaction;

// Implement CatalogQueryTransactionOperations for StandardQueryTransaction
impl<T: Transaction> CatalogQueryTransactionOperations
	for StandardQueryTransaction<T>
{
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}

	fn version(&self) -> Version {
		self.versioned.version()
	}
}

impl<T: Transaction> CatalogSchemaQueryOperations
	for StandardQueryTransaction<T>
{
	fn find_schema_by_name(
		&mut self,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SchemaDef>> {
		let name = name.as_ref();

		Ok(self.catalog.find_schema_by_name(
			name,
			VersionedQueryTransaction::version(self),
		))
	}

	fn find_schema(
		&mut self,
		id: SchemaId,
	) -> crate::Result<Option<SchemaDef>> {
		Ok(self.catalog.find_schema(
			id,
			VersionedQueryTransaction::version(self),
		))
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

impl<T: Transaction> CatalogTableQueryOperations
	for StandardQueryTransaction<T>
{
	fn find_table_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>> {
		let name = name.as_ref();

		Ok(self.catalog.find_table_by_name(
			schema,
			name,
			VersionedQueryTransaction::version(self),
		))
	}

	fn find_table(
		&mut self,
		id: TableId,
	) -> crate::Result<Option<TableDef>> {
		Ok(self.catalog.find_table(
			id,
			VersionedQueryTransaction::version(self),
		))
	}
}

impl<T: Transaction> CatalogViewQueryOperations
	for StandardQueryTransaction<T>
{
	fn find_view_by_name(
		&mut self,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();

		Ok(self.catalog.find_view_by_name(
			schema,
			name,
			VersionedQueryTransaction::version(self),
		))
	}

	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>> {
		Ok(self.catalog.find_view(
			id,
			VersionedQueryTransaction::version(self),
		))
	}
}

impl<T: Transaction> CatalogQueryTransaction for StandardQueryTransaction<T> {}
