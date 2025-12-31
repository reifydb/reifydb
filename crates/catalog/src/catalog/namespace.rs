// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{NamespaceDef, NamespaceId, TransactionalNamespaceChanges};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction};
use reifydb_type::{Fragment, diagnostic::catalog::namespace_not_found, error, internal};
use tracing::{instrument, warn};

use crate::{Catalog, CatalogStore};

impl Catalog {
	#[instrument(name = "catalog::namespace::find", level = "trace", skip(self, txn))]
	pub async fn find_namespace<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: NamespaceId,
	) -> crate::Result<Option<NamespaceDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(namespace) = TransactionalNamespaceChanges::find_namespace(cmd, id) {
					return Ok(Some(namespace.clone()));
				}

				// 2. Check if deleted
				if TransactionalNamespaceChanges::is_namespace_deleted(cmd, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(namespace) = self.materialized.find_namespace(id, cmd.version()) {
					return Ok(Some(namespace));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace(cmd, id).await? {
					warn!(
						"Namespace with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(namespace) = self.materialized.find_namespace(id, qry.version()) {
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace(qry, id).await? {
					warn!(
						"Namespace with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::namespace::find_by_name", level = "trace", skip(self, txn, name))]
	pub async fn find_namespace_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		name: &str,
	) -> crate::Result<Option<NamespaceDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(namespace) =
					TransactionalNamespaceChanges::find_namespace_by_name(cmd, name)
				{
					return Ok(Some(namespace.clone()));
				}

				// 2. Check if deleted
				if TransactionalNamespaceChanges::is_namespace_deleted_by_name(cmd, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(namespace) = self.materialized.find_namespace_by_name(name, cmd.version()) {
					return Ok(Some(namespace));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace_by_name(cmd, name).await? {
					warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(namespace) = self.materialized.find_namespace_by_name(name, qry.version()) {
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace_by_name(qry, name).await? {
					warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::namespace::get", level = "trace", skip(self, txn))]
	pub async fn get_namespace<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: NamespaceId,
	) -> crate::Result<NamespaceDef> {
		self.find_namespace(txn, id).await?.ok_or_else(|| {
			error!(internal!(
				"Namespace with ID {} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::namespace::get_by_name", level = "trace", skip(self, txn, name))]
	pub async fn get_namespace_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<NamespaceDef> {
		let name = name.into();
		self.find_namespace_by_name(txn, name.text())
			.await?
			.ok_or_else(|| error!(namespace_not_found(name.clone(), name.text())))
	}
}
