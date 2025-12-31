// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_core::interface::{
	CommandTransaction, NamespaceId, QueryTransaction, TableDef, TableId, TransactionalChanges,
	TransactionalTableChanges,
	interceptor::{TableDefInterceptor, WithInterceptors},
};
use reifydb_type::{
	Fragment,
	diagnostic::catalog::{table_already_exists, table_not_found},
	error, internal, return_error,
};
use tracing::{instrument, warn};

use crate::{
	CatalogNamespaceQueryOperations, CatalogStore, store::table::TableToCreate,
	transaction::MaterializedCatalogTransaction,
};

#[async_trait]
pub trait CatalogTableCommandOperations: Send {
	async fn create_table(&mut self, table: TableToCreate) -> crate::Result<TableDef>;

	// TODO: Implement when update/delete are ready
	// async fn update_table(&mut self, table_id: TableId, updates: TableUpdates)
	// -> crate::Result<TableDef>; async fn delete_table(&mut self, table_id:
	// TableId) -> crate::Result<()>;
}

pub trait CatalogTrackTableChangeOperations {
	fn track_table_def_created(&mut self, table: TableDef) -> crate::Result<()>;

	fn track_table_def_updated(&mut self, pre: TableDef, post: TableDef) -> crate::Result<()>;

	fn track_table_def_deleted(&mut self, table: TableDef) -> crate::Result<()>;
}

#[async_trait]
pub trait CatalogTableQueryOperations: CatalogNamespaceQueryOperations {
	async fn find_table(&mut self, id: TableId) -> crate::Result<Option<TableDef>>;

	async fn find_table_by_name(&mut self, namespace: NamespaceId, name: &str) -> crate::Result<Option<TableDef>>;

	async fn get_table(&mut self, id: TableId) -> crate::Result<TableDef>;

	async fn get_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<TableDef>;
}

#[async_trait]
impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackTableChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges
		+ Send
		+ 'static,
> CatalogTableCommandOperations for CT
{
	#[instrument(name = "catalog::table::create", level = "debug", skip(self, to_create))]
	async fn create_table(&mut self, to_create: TableToCreate) -> reifydb_core::Result<TableDef> {
		if let Some(table) = self.find_table_by_name(to_create.namespace, to_create.table.as_str()).await? {
			let namespace = self.get_namespace(to_create.namespace).await?;
			return_error!(table_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&table.name
			));
		}
		let result = CatalogStore::create_table(self, to_create).await?;
		self.track_table_def_created(result.clone())?;
		TableDefInterceptor::post_create(self, &result).await?;
		Ok(result)
	}
}

#[async_trait]
impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges + Send + 'static>
	CatalogTableQueryOperations for QT
{
	#[instrument(name = "catalog::table::find", level = "trace", skip(self))]
	async fn find_table(&mut self, id: TableId) -> reifydb_core::Result<Option<TableDef>> {
		// 1. Check transactional changes first
		// nop for MultiVersionQueryTransaction
		if let Some(table) = TransactionalTableChanges::find_table(self, id) {
			return Ok(Some(table.clone()));
		}

		// 2. Check if deleted
		// nop for MultiVersionQueryTransaction
		if TransactionalTableChanges::is_table_deleted(self, id) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(table) = self.catalog().find_table(id, self.version()) {
			return Ok(Some(table));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(table) = CatalogStore::find_table(self, id).await? {
			warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
			return Ok(Some(table));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::table::find_by_name", level = "trace", skip(self, name))]
	async fn find_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: &str,
	) -> reifydb_core::Result<Option<TableDef>> {
		// 1. Check transactional changes first
		// nop for MultiVersionQueryTransaction
		if let Some(table) = TransactionalTableChanges::find_table_by_name(self, namespace, name) {
			return Ok(Some(table.clone()));
		}

		// 2. Check if deleted
		// nop for MultiVersionQueryTransaction
		if TransactionalTableChanges::is_table_deleted_by_name(self, namespace, name) {
			return Ok(None);
		}

		// 3. Check MaterializedCatalog
		if let Some(table) = self.catalog().find_table_by_name(namespace, name, self.version()) {
			return Ok(Some(table));
		}

		// 4. Fall back to storage as defensive measure
		if let Some(table) = CatalogStore::find_table_by_name(self, namespace, name).await? {
			warn!(
				"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
				name, namespace
			);
			return Ok(Some(table));
		}

		Ok(None)
	}

	#[instrument(name = "catalog::table::get", level = "trace", skip(self))]
	async fn get_table(&mut self, id: TableId) -> reifydb_core::Result<TableDef> {
		self.find_table(id).await?.ok_or_else(|| {
			error!(internal!(
				"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::table::get_by_name", level = "trace", skip(self, name))]
	async fn get_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> reifydb_core::Result<TableDef> {
		let name = name.into();

		// Try to get the namespace name for the error message
		let namespace_name = self
			.find_namespace(namespace)
			.await?
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		self.find_table_by_name(namespace, name.text())
			.await?
			.ok_or_else(|| error!(table_not_found(name.clone(), &namespace_name, name.text())))
	}
}
