// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{change::CatalogTrackSourceChangeOperations, id::NamespaceId, source::Source};
use reifydb_transaction::{
	change::TransactionalSourceChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog, store::source::create::SourceToCreate as StoreSourceToCreate};

#[derive(Debug, Clone)]
pub struct SourceToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub connector: String,
	pub config: Vec<(String, String)>,
	pub target_namespace: NamespaceId,
	pub target_name: String,
}

impl From<SourceToCreate> for StoreSourceToCreate {
	fn from(to_create: SourceToCreate) -> Self {
		StoreSourceToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			connector: to_create.connector,
			config: to_create.config,
			target_namespace: to_create.target_namespace,
			target_name: to_create.target_name,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::source::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_source_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Source>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(source) =
					self.materialized.find_source_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(source));
				}

				if let Some(source) = CatalogStore::find_source_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"Source '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(source));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(source) =
					TransactionalSourceChanges::find_source_by_name(admin, namespace, name)
				{
					return Ok(Some(source.clone()));
				}

				if TransactionalSourceChanges::is_source_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				if let Some(source) =
					self.materialized.find_source_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(source));
				}

				if let Some(source) = CatalogStore::find_source_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"Source '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(source));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(source) =
					self.materialized.find_source_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(source));
				}

				if let Some(source) = CatalogStore::find_source_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"Source '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(source));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(source) =
					TransactionalSourceChanges::find_source_by_name(t.inner, namespace, name)
				{
					return Ok(Some(source.clone()));
				}

				if TransactionalSourceChanges::is_source_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}

				if let Some(source) = CatalogStore::find_source_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					namespace,
					name,
				)? {
					return Ok(Some(source));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(source) =
					self.materialized.find_source_by_name_at(namespace, name, rep.version())
				{
					return Ok(Some(source));
				}

				if let Some(source) = CatalogStore::find_source_by_name(
					&mut Transaction::Replica(&mut *rep),
					namespace,
					name,
				)? {
					warn!(
						"Source '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(source));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::source::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_source(&self, txn: &mut AdminTransaction, to_create: SourceToCreate) -> Result<Source> {
		let source = CatalogStore::create_source(txn, to_create.into())?;
		txn.track_source_created(source.clone())?;
		Ok(source)
	}

	#[instrument(name = "catalog::source::drop", level = "debug", skip(self, txn))]
	pub fn drop_source(&self, txn: &mut AdminTransaction, source: Source) -> Result<()> {
		CatalogStore::drop_source(txn, source.id)?;
		txn.track_source_deleted(source)?;
		Ok(())
	}

	#[instrument(name = "catalog::source::list_all", level = "debug", skip(self, txn))]
	pub fn list_sources_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Source>> {
		CatalogStore::list_sources_all(txn)
	}
}
