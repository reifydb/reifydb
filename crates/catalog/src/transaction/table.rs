// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, NamespaceId, QueryTransaction, TableDef, TableId,
};

use crate::table::TableToCreate;

pub trait CatalogTableCommandOperations {
	fn create_table(
		&mut self,
		table: TableToCreate,
	) -> crate::Result<TableDef>;

	// TODO: Implement when update/delete are ready
	// fn update_table(&mut self, table_id: TableId, updates: TableUpdates)
	// -> crate::Result<TableDef>; fn delete_table(&mut self, table_id:
	// TableId) -> crate::Result<()>;
}

pub trait CatalogTrackTableChangeOperations {
	// Table tracking methods
	fn track_table_def_created(
		&mut self,
		table: TableDef,
	) -> crate::Result<()>;

	fn track_table_def_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> crate::Result<()>;

	fn track_table_def_deleted(
		&mut self,
		table: TableDef,
	) -> crate::Result<()>;
}

pub trait CatalogTableQueryOperations {
	fn find_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>>;

	fn find_table(
		&mut self,
		id: TableId,
	) -> crate::Result<Option<TableDef>>;

	fn get_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<TableDef>;
}

impl<T: CommandTransaction> CatalogTableCommandOperations for T {
	fn create_table(
		&mut self,
		table: TableToCreate,
	) -> reifydb_core::Result<TableDef> {
		todo!()
	}
}

impl<T: QueryTransaction> CatalogTableQueryOperations for T {
	fn find_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> reifydb_core::Result<Option<TableDef>> {
		todo!()
	}

	fn find_table(
		&mut self,
		id: TableId,
	) -> reifydb_core::Result<Option<TableDef>> {
		todo!()
	}

	fn get_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> reifydb_core::Result<TableDef> {
		todo!()
	}
}

// impl<T> CatalogTableCommandOperations for T
// where
// 	T: CommandTransaction
// 		+ CatalogTrackChangeOperations
// 		+ CatalogNamespaceQueryOperations
// 		+ CatalogTableQueryOperations
// 		+ WithInterceptors<T>
// 		+ WithEventBus
// 		+ TableDefInterceptor<T>,
// {
// 	fn create_table(
// 		&mut self,
// 		to_create: TableToCreate,
// 	) -> crate::Result<TableDef> {
// 		if let Some(table) = self.find_table_by_name(
// 			to_create.namespace,
// 			&to_create.table,
// 		)? {
// 			let namespace =
// 				self.get_namespace(to_create.namespace)?;
//
// 			return_error!(table_already_exists(
// 				to_create.fragment,
// 				&namespace.name,
// 				&table.name
// 			));
// 		}
//
// 		let result = CatalogStore::create_table(self, to_create)?;
// 		self.track_table_def_created(result.clone())?;
// 		TableDefInterceptor::post_create(self, &result)?;
//
// 		Ok(result)
// 	}
// }
//
// // Query operations implementation
// impl<T> CatalogTableQueryOperations for T
// where
// 	T: CommandTransaction
// 		+ CatalogTrackTableChangeOperations
// 		+ TransactionalChangesExt,
// {
// 	fn find_table_by_name(
// 		&mut self,
// 		namespace: NamespaceId,
// 		name: impl AsRef<str>,
// 	) -> crate::Result<Option<TableDef>> {
// 		let name = name.as_ref();
//
// 		// 1. Check transactional changes first
// 		if let Some(table) =
// 			self.get_changes().find_table_by_name(namespace, name)
// 		{
// 			return Ok(Some(table.clone()));
// 		}
//
// 		if self.get_changes().is_table_deleted_by_name(namespace, name)
// 		{
// 			return Ok(None);
// 		}
//
// 		// 2. Check MaterializedCatalog
// 		if let Some(table) = self.catalog().find_table_by_name(
// 			namespace,
// 			name,
// 			<T as CatalogTransaction>::version(self),
// 		) {
// 			return Ok(Some(table));
// 		}
//
// 		// 3. Fall back to storage as defensive measure
// 		if let Some(table) =
// 			CatalogStore::find_table_by_name(self, namespace, name)?
// 		{
// 			log_warn!(
// 				"Table '{}' in namespace {:?} found in storage but not in
// MaterializedCatalog", 				name,
// 				namespace
// 			);
// 			return Ok(Some(table));
// 		}
//
// 		Ok(None)
// 	}
//
// 	fn find_table(
// 		&mut self,
// 		id: TableId,
// 	) -> crate::Result<Option<TableDef>> {
// 		// 1. Check transactional changes first
// 		if let Some(table) = self.get_changes().get_table_def(id) {
// 			return Ok(Some(table.clone()));
// 		}
//
// 		// 2. Check MaterializedCatalog
// 		if let Some(table) = self.catalog().find_table(
// 			id,
// 			<T as CatalogTransaction>::version(self),
// 		) {
// 			return Ok(Some(table));
// 		}
//
// 		// 3. Fall back to storage as defensive measure
// 		if let Some(table) = CatalogStore::find_table(self, id)? {
// 			log_warn!(
// 				"Table with ID {:?} found in storage but not in MaterializedCatalog",
// 				id
// 			);
// 			return Ok(Some(table));
// 		}
//
// 		Ok(None)
// 	}
//
// 	fn get_table_by_name(
// 		&mut self,
// 		_namespace: NamespaceId,
// 		_name: impl AsRef<str>,
// 	) -> reifydb_core::Result<TableDef> {
// 		todo!()
// 	}
// }
//
// // TODO: Add CatalogTableQueryOperations implementation for query-only
// // transactions
