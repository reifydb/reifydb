// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{NamespaceId, TableDef, TableId};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction, change::TransactionalTableChanges};
use reifydb_type::{Fragment, diagnostic::catalog::table_not_found, error, internal};
use tracing::{instrument, warn};

use crate::{Catalog, CatalogStore};

impl Catalog {
	#[instrument(name = "catalog::table::find", level = "trace", skip(self, txn))]
	pub async fn find_table<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: TableId,
	) -> crate::Result<Option<TableDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(table) = TransactionalTableChanges::find_table(cmd, id) {
					return Ok(Some(table.clone()));
				}

				// 2. Check if deleted
				if TransactionalTableChanges::is_table_deleted(cmd, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(table) = self.materialized.find_table(id, cmd.version()) {
					return Ok(Some(table));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table(cmd, id).await? {
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(table) = self.materialized.find_table(id, qry.version()) {
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table(qry, id).await? {
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::table::find_by_name", level = "trace", skip(self, txn, name))]
	pub async fn find_table_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<TableDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(table) = TransactionalTableChanges::find_table_by_name(cmd, namespace, name)
				{
					return Ok(Some(table.clone()));
				}

				// 2. Check if deleted
				if TransactionalTableChanges::is_table_deleted_by_name(cmd, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(table) =
					self.materialized.find_table_by_name(namespace, name, cmd.version())
				{
					return Ok(Some(table));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table_by_name(cmd, namespace, name).await? {
					warn!(
						"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(table));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(table) =
					self.materialized.find_table_by_name(namespace, name, qry.version())
				{
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table_by_name(qry, namespace, name).await? {
					warn!(
						"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(table));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::table::get", level = "trace", skip(self, txn))]
	pub async fn get_table<T: IntoStandardTransaction>(&self, txn: &mut T, id: TableId) -> crate::Result<TableDef> {
		self.find_table(txn, id).await?.ok_or_else(|| {
			error!(internal!(
				"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::table::get_by_name", level = "trace", skip(self, txn, name))]
	pub async fn get_table_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<TableDef> {
		let name = name.into();

		// Try to get the namespace name for the error message
		let namespace_name = self
			.find_namespace(txn, namespace)
			.await?
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		self.find_table_by_name(txn, namespace, name.text())
			.await?
			.ok_or_else(|| error!(table_not_found(name.clone(), &namespace_name, name.text())))
	}
}
