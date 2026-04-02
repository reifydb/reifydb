// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{change::CatalogTrackSinkChangeOperations, id::NamespaceId, sink::Sink};
use reifydb_transaction::{
	change::TransactionalSinkChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog, store::sink::create::SinkToCreate as StoreSinkToCreate};

#[derive(Debug, Clone)]
pub struct SinkToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub source_namespace: NamespaceId,
	pub source_name: String,
	pub connector: String,
	pub config: Vec<(String, String)>,
}

impl From<SinkToCreate> for StoreSinkToCreate {
	fn from(to_create: SinkToCreate) -> Self {
		StoreSinkToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			source_namespace: to_create.source_namespace,
			source_name: to_create.source_name,
			connector: to_create.connector,
			config: to_create.config,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::sink::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_sink_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Sink>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(sink) =
					self.materialized.find_sink_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(sink));
				}
				if let Some(sink) = CatalogStore::find_sink_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"Sink '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(sink));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(sink) = TransactionalSinkChanges::find_sink_by_name(admin, namespace, name)
				{
					return Ok(Some(sink.clone()));
				}
				if TransactionalSinkChanges::is_sink_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}
				if let Some(sink) =
					self.materialized.find_sink_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(sink));
				}
				if let Some(sink) = CatalogStore::find_sink_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"Sink '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(sink));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(sink) =
					self.materialized.find_sink_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(sink));
				}
				if let Some(sink) = CatalogStore::find_sink_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"Sink '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(sink));
				}
				Ok(None)
			}
			Transaction::Subscription(sub) => {
				if let Some(sink) = TransactionalSinkChanges::find_sink_by_name(sub, namespace, name) {
					return Ok(Some(sink.clone()));
				}
				if TransactionalSinkChanges::is_sink_deleted_by_name(sub, namespace, name) {
					return Ok(None);
				}
				if let Some(sink) =
					self.materialized.find_sink_by_name_at(namespace, name, sub.version())
				{
					return Ok(Some(sink));
				}
				if let Some(sink) = CatalogStore::find_sink_by_name(
					&mut Transaction::Subscription(&mut *sub),
					namespace,
					name,
				)? {
					warn!(
						"Sink '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(sink));
				}
				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(sink) =
					TransactionalSinkChanges::find_sink_by_name(t.inner, namespace, name)
				{
					return Ok(Some(sink.clone()));
				}
				if TransactionalSinkChanges::is_sink_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(sink) = CatalogStore::find_sink_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					namespace,
					name,
				)? {
					return Ok(Some(sink));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(sink) =
					self.materialized.find_sink_by_name_at(namespace, name, rep.version())
				{
					return Ok(Some(sink));
				}
				if let Some(sink) = CatalogStore::find_sink_by_name(
					&mut Transaction::Replica(&mut *rep),
					namespace,
					name,
				)? {
					warn!(
						"Sink '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(sink));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::sink::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_sink(&self, txn: &mut AdminTransaction, to_create: SinkToCreate) -> Result<Sink> {
		let sink = CatalogStore::create_sink(txn, to_create.into())?;
		txn.track_sink_created(sink.clone())?;
		Ok(sink)
	}

	#[instrument(name = "catalog::sink::drop", level = "debug", skip(self, txn))]
	pub fn drop_sink(&self, txn: &mut AdminTransaction, sink: Sink) -> Result<()> {
		CatalogStore::drop_sink(txn, sink.id)?;
		txn.track_sink_deleted(sink)?;
		Ok(())
	}

	#[instrument(name = "catalog::sink::list_all", level = "debug", skip(self, txn))]
	pub fn list_sinks_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Sink>> {
		CatalogStore::list_sinks_all(txn)
	}
}
