// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackNamespaceChangeOperations, id::NamespaceId, namespace::Namespace},
	internal,
};
use reifydb_transaction::{
	change::TransactionalNamespaceChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{error, fragment::Fragment};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
	store::namespace::create::NamespaceToCreate as StoreNamespaceToCreate,
};

/// Namespace creation specification for the Catalog API.
#[derive(Debug, Clone)]
pub struct NamespaceToCreate {
	pub namespace_fragment: Option<Fragment>,
	pub name: String,
	pub local_name: String,
	pub parent_id: NamespaceId,
	pub grpc: Option<String>,
}

impl From<NamespaceToCreate> for StoreNamespaceToCreate {
	fn from(to_create: NamespaceToCreate) -> Self {
		StoreNamespaceToCreate {
			namespace_fragment: to_create.namespace_fragment,
			name: to_create.name,
			local_name: to_create.local_name,
			parent_id: to_create.parent_id,
			grpc: to_create.grpc,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::namespace::find", level = "trace", skip(self, txn))]
	pub fn find_namespace(&self, txn: &mut Transaction<'_>, id: NamespaceId) -> Result<Option<Namespace>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(namespace) = self.materialized.find_namespace_at(id, cmd.version()) {
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) =
					CatalogStore::find_namespace(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!(
						"Namespace with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(namespace) = TransactionalNamespaceChanges::find_namespace(admin, id) {
					return Ok(Some(namespace.clone()));
				}

				// 2. Check if deleted
				if TransactionalNamespaceChanges::is_namespace_deleted(admin, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(namespace) = self.materialized.find_namespace_at(id, admin.version()) {
					return Ok(Some(namespace));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(namespace) =
					CatalogStore::find_namespace(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!(
						"Namespace with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(namespace) = self.materialized.find_namespace_at(id, qry.version()) {
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) =
					CatalogStore::find_namespace(&mut Transaction::Query(&mut *qry), id)?
				{
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
	pub fn find_namespace_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<Option<Namespace>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(namespace) =
					self.materialized.find_namespace_by_name_at(name, cmd.version())
				{
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace_by_name(
					&mut Transaction::Command(&mut *cmd),
					name,
				)? {
					warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(namespace) =
					TransactionalNamespaceChanges::find_namespace_by_name(admin, name)
				{
					return Ok(Some(namespace.clone()));
				}

				// 2. Check if deleted
				if TransactionalNamespaceChanges::is_namespace_deleted_by_name(admin, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(namespace) =
					self.materialized.find_namespace_by_name_at(name, admin.version())
				{
					return Ok(Some(namespace));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(namespace) = CatalogStore::find_namespace_by_name(
					&mut Transaction::Admin(&mut *admin),
					name,
				)? {
					warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(namespace) =
					self.materialized.find_namespace_by_name_at(name, qry.version())
				{
					return Ok(Some(namespace));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(namespace) =
					CatalogStore::find_namespace_by_name(&mut Transaction::Query(&mut *qry), name)?
				{
					warn!("Namespace '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(namespace));
				}

				Ok(None)
			}
		}
	}

	fn find_child_namespace(
		&self,
		txn: &mut Transaction<'_>,
		parent_id: NamespaceId,
		name: &str,
	) -> Result<Option<Namespace>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(ns) =
					self.materialized.find_child_namespace_at(parent_id, name, cmd.version())
				{
					return Ok(Some(ns));
				}
				let all = CatalogStore::list_namespaces_all(&mut Transaction::Command(&mut *cmd))?;
				Ok(all.into_iter().find(|ns| ns.local_name() == name && ns.parent_id() == parent_id))
			}
			Transaction::Admin(admin) => {
				if let Some(ns) = admin.changes.namespace.iter().rev().find_map(|change| {
					change.post
						.as_ref()
						.filter(|ns| ns.local_name() == name && ns.parent_id() == parent_id)
				}) {
					return Ok(Some(ns.clone()));
				}
				if let Some(ns) =
					self.materialized.find_child_namespace_at(parent_id, name, admin.version())
				{
					return Ok(Some(ns));
				}
				let all = CatalogStore::list_namespaces_all(&mut Transaction::Admin(&mut *admin))?;
				Ok(all.into_iter().find(|ns| ns.local_name() == name && ns.parent_id() == parent_id))
			}
			Transaction::Query(qry) => {
				if let Some(ns) =
					self.materialized.find_child_namespace_at(parent_id, name, qry.version())
				{
					return Ok(Some(ns));
				}
				let all = CatalogStore::list_namespaces_all(&mut Transaction::Query(&mut *qry))?;
				Ok(all.into_iter().find(|ns| ns.local_name() == name && ns.parent_id() == parent_id))
			}
		}
	}

	/// Resolve namespace from path segments (e.g. `["system", "config"]`).
	/// Returns the "default" namespace when segments is empty.
	pub fn find_namespace_by_segments(
		&self,
		txn: &mut Transaction<'_>,
		segments: &[&str],
	) -> Result<Option<Namespace>> {
		if segments.is_empty() {
			return self.find_namespace_by_name(txn, "default");
		}

		let mut current = match self.find_namespace_by_name(txn, segments[0])? {
			Some(ns) => ns,
			None => return Ok(None),
		};

		for &segment in &segments[1..] {
			match self.find_child_namespace(txn, current.id(), segment)? {
				Some(ns) => current = ns,
				None => return Ok(None),
			}
		}

		Ok(Some(current))
	}

	/// Resolve a `::` separated path (e.g. `"system::config"`) to a `Namespace` by walking
	/// parent → child hierarchically.
	pub fn find_namespace_by_path(&self, txn: &mut Transaction<'_>, path: &str) -> Result<Option<Namespace>> {
		let segments: Vec<&str> = path.split("::").collect();
		self.find_namespace_by_segments(txn, &segments)
	}

	#[instrument(name = "catalog::namespace::get", level = "trace", skip(self, txn))]
	pub fn get_namespace(&self, txn: &mut Transaction<'_>, id: NamespaceId) -> Result<Namespace> {
		self.find_namespace(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Namespace with ID {} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::namespace::get_by_name", level = "trace", skip(self, txn, name))]
	pub fn get_namespace_by_name(
		&self,
		txn: &mut Transaction<'_>,
		name: impl Into<Fragment> + Send,
	) -> Result<Namespace> {
		let name = name.into();
		self.find_namespace_by_name(txn, name.text())?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: name.text().to_string(),
				name: name.text().to_string(),
				fragment: name.clone(),
			}
			.into()
		})
	}

	#[instrument(name = "catalog::namespace::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_namespace(&self, txn: &mut AdminTransaction, to_create: NamespaceToCreate) -> Result<Namespace> {
		let namespace = CatalogStore::create_namespace(txn, to_create.into())?;
		txn.track_namespace_created(namespace.clone())?;
		Ok(namespace)
	}

	#[instrument(name = "catalog::namespace::drop", level = "debug", skip(self, txn))]
	pub fn drop_namespace(&self, txn: &mut AdminTransaction, namespace: Namespace) -> Result<()> {
		CatalogStore::drop_namespace(txn, namespace.id())?;
		txn.track_namespace_deleted(namespace)?;
		Ok(())
	}

	#[instrument(name = "catalog::namespace::list_all", level = "debug", skip(self, txn))]
	pub fn list_namespaces_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Namespace>> {
		CatalogStore::list_namespaces_all(txn)
	}

	#[instrument(name = "catalog::namespace::update_grpc", level = "debug", skip(self, txn))]
	pub fn update_namespace_grpc(
		&self,
		txn: &mut AdminTransaction,
		namespace_id: NamespaceId,
		grpc: Option<String>,
	) -> Result<()> {
		CatalogStore::update_namespace_grpc(txn, namespace_id, grpc)?;
		// Re-read the updated namespace and track the change
		let updated = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		txn.track_namespace_created(updated)?;
		Ok(())
	}
}
