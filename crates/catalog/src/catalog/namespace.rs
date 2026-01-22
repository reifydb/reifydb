// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::namespace_not_found,
	interface::catalog::{change::CatalogTrackNamespaceChangeOperations, id::NamespaceId, namespace::NamespaceDef},
	internal,
};
use reifydb_transaction::{
	change::TransactionalNamespaceChanges,
	standard::{IntoStandardTransaction, StandardTransaction, command::StandardCommandTransaction},
};
use reifydb_type::{error, fragment::Fragment};
use tracing::{instrument, warn};

use crate::{CatalogStore, catalog::Catalog, store::namespace::create::NamespaceToCreate as StoreNamespaceToCreate};

/// Namespace creation specification for the Catalog API.
#[derive(Debug, Clone)]
pub struct NamespaceToCreate {
	pub namespace_fragment: Option<Fragment>,
	pub name: String,
}

impl From<NamespaceToCreate> for StoreNamespaceToCreate {
	fn from(to_create: NamespaceToCreate) -> Self {
		StoreNamespaceToCreate {
			namespace_fragment: to_create.namespace_fragment,
			name: to_create.name,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::namespace::find", level = "trace", skip(self, txn))]
	pub fn find_namespace<T: IntoStandardTransaction>(
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
				if let Some(namespace) = self.materialized.find_namespace_at(id, cmd.version()) {
					return Ok(Some(namespace));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace(cmd, id)? {
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
				if let Some(namespace) = self.materialized.find_namespace_at(id, qry.version()) {
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace(qry, id)? {
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
	pub fn find_namespace_by_name<T: IntoStandardTransaction>(
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
				if let Some(namespace) =
					self.materialized.find_namespace_by_name_at(name, cmd.version())
				{
					return Ok(Some(namespace));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace_by_name(cmd, name)? {
					warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(namespace) =
					self.materialized.find_namespace_by_name_at(name, qry.version())
				{
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace_by_name(qry, name)? {
					warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::namespace::get", level = "trace", skip(self, txn))]
	pub fn get_namespace<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: NamespaceId,
	) -> crate::Result<NamespaceDef> {
		self.find_namespace(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Namespace with ID {} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::namespace::get_by_name", level = "trace", skip(self, txn, name))]
	pub fn get_namespace_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<NamespaceDef> {
		let name = name.into();
		self.find_namespace_by_name(txn, name.text())?
			.ok_or_else(|| error!(namespace_not_found(name.clone(), name.text())))
	}

	#[instrument(name = "catalog::namespace::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_namespace(
		&self,
		txn: &mut StandardCommandTransaction,
		to_create: NamespaceToCreate,
	) -> crate::Result<NamespaceDef> {
		let namespace = CatalogStore::create_namespace(txn, to_create.into())?;
		txn.track_namespace_def_created(namespace.clone())?;
		Ok(namespace)
	}

	#[instrument(name = "catalog::namespace::delete", level = "debug", skip(self, txn))]
	pub fn delete_namespace(
		&self,
		txn: &mut StandardCommandTransaction,
		namespace: NamespaceDef,
	) -> crate::Result<()> {
		CatalogStore::delete_namespace(txn, namespace.id)?;
		txn.track_namespace_def_deleted(namespace)?;
		Ok(())
	}

	#[instrument(name = "catalog::namespace::list_all", level = "debug", skip(self, txn))]
	pub fn list_namespaces_all<T: IntoStandardTransaction>(&self, txn: &mut T) -> crate::Result<Vec<NamespaceDef>> {
		CatalogStore::list_namespaces_all(txn)
	}
}
