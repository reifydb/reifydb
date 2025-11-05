// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		CommandTransaction, NamespaceId, QueryTransaction, TableDef, TableId, TransactionalChanges,
		TransactionalTableChanges,
		interceptor::{TableDefInterceptor, WithInterceptors},
	},
	log_warn,
};
use reifydb_type::{
	IntoFragment,
	diagnostic::catalog::{table_already_exists, table_not_found},
	error, internal, return_error,
};

use crate::{
	CatalogNamespaceQueryOperations, CatalogStore, store::table::TableToCreate,
	transaction::MaterializedCatalogTransaction,
};

pub trait CatalogTableCommandOperations {
	fn create_table(&mut self, table: TableToCreate) -> crate::Result<TableDef>;

	// TODO: Implement when update/delete are ready
	// fn update_table(&mut self, table_id: TableId, updates: TableUpdates)
	// -> crate::Result<TableDef>; fn delete_table(&mut self, table_id:
	// TableId) -> crate::Result<()>;
}

pub trait CatalogTrackTableChangeOperations {
	// Table tracking methods
	fn track_table_def_created(&mut self, table: TableDef) -> crate::Result<()>;

	fn track_table_def_updated(&mut self, pre: TableDef, post: TableDef) -> crate::Result<()>;

	fn track_table_def_deleted(&mut self, table: TableDef) -> crate::Result<()>;
}

pub trait CatalogTableQueryOperations: CatalogNamespaceQueryOperations {
	fn find_table(&mut self, id: TableId) -> crate::Result<Option<TableDef>>;

	fn find_table_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<TableDef>>;

	fn get_table(&mut self, id: TableId) -> crate::Result<TableDef>;

	fn get_table_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<TableDef>;
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackTableChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogTableCommandOperations for CT
{
	fn create_table(&mut self, to_create: TableToCreate) -> reifydb_core::Result<TableDef> {
		if let Some(table) = self.find_table_by_name(to_create.namespace, &to_create.table)? {
			let namespace = self.get_namespace(to_create.namespace)?;
			return_error!(table_already_exists(to_create.fragment, &namespace.name, &table.name));
		}
		let result = CatalogStore::create_table(self, to_create)?;
		self.track_table_def_created(result.clone())?;
		TableDefInterceptor::post_create(self, &result)?;
		Ok(result)
	}
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges> CatalogTableQueryOperations for QT {
	fn find_table(&mut self, id: TableId) -> reifydb_core::Result<Option<TableDef>> {
		// 1. Check transactional changes first
		// nop for QueryTransaction
		if let Some(table) = TransactionalTableChanges::find_table(self, id) {
			return Ok(Some(table.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalTableChanges::is_table_deleted(self, id) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(table) = self.catalog().find_table(id, self.version()) {
			return Ok(Some(table));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(table) = CatalogStore::find_table(self, id)? {
			log_warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(table));
		}

		Ok(None)
	}

	fn find_table_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<Option<TableDef>> {
		let name = name.into_fragment();

		// 1. Check transactional changes first
		// nop for QueryTransaction
		if let Some(table) = TransactionalTableChanges::find_table_by_name(self, namespace, name.as_borrowed())
		{
			return Ok(Some(table.clone()));
		}

		// 2. Check if deleted
		// nop for QueryTransaction
		if TransactionalTableChanges::is_table_deleted_by_name(self, namespace, name.as_borrowed()) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(table) = self.catalog().find_table_by_name(namespace, name.text(), self.version()) {
			return Ok(Some(table));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(table) = CatalogStore::find_table_by_name(self, namespace, name.text())? {
			log_warn!(
				"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
				name.text(),
				namespace
			);
			return Ok(Some(table));
		}

		Ok(None)
	}

	fn get_table(&mut self, id: TableId) -> reifydb_core::Result<TableDef> {
		self.find_table(id)?.ok_or_else(|| {
			error!(internal!(
				"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	fn get_table_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<TableDef> {
		let name = name.into_fragment();

		// Try to get the namespace name for the error message
		let namespace_name = self
			.find_namespace(namespace)?
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		self.find_table_by_name(namespace, name.as_borrowed())?
			.ok_or_else(|| error!(table_not_found(name.as_borrowed(), &namespace_name, name.text())))
	}
}
