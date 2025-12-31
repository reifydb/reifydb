// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{FlowDef, FlowId, NamespaceId, TransactionalFlowChanges};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction};
use reifydb_type::{error, internal};
use tracing::{instrument, warn};

use crate::{Catalog, CatalogStore};

impl Catalog {
	#[instrument(name = "catalog::flow::find", level = "trace", skip(self, txn))]
	pub async fn find_flow<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: FlowId,
	) -> crate::Result<Option<FlowDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(flow) = TransactionalFlowChanges::find_flow(cmd, id) {
					return Ok(Some(flow.clone()));
				}

				// 2. Check if deleted
				if TransactionalFlowChanges::is_flow_deleted(cmd, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(flow) = self.materialized.find_flow(id, cmd.version()) {
					return Ok(Some(flow));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow(cmd, id).await? {
					warn!("Flow with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(flow) = self.materialized.find_flow(id, qry.version()) {
					return Ok(Some(flow));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow(qry, id).await? {
					warn!("Flow with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(flow));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::flow::find_by_name", level = "trace", skip(self, txn, name))]
	pub async fn find_flow_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<FlowDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(flow) = TransactionalFlowChanges::find_flow_by_name(cmd, namespace, name) {
					return Ok(Some(flow.clone()));
				}

				// 2. Check if deleted
				if TransactionalFlowChanges::is_flow_deleted_by_name(cmd, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(flow) = self.materialized.find_flow_by_name(namespace, name, cmd.version())
				{
					return Ok(Some(flow));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow_by_name(cmd, namespace, name).await? {
					warn!(
						"Flow '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(flow) = self.materialized.find_flow_by_name(namespace, name, qry.version())
				{
					return Ok(Some(flow));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow_by_name(qry, namespace, name).await? {
					warn!(
						"Flow '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(flow));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::flow::get", level = "trace", skip(self, txn))]
	pub async fn get_flow<T: IntoStandardTransaction>(&self, txn: &mut T, id: FlowId) -> crate::Result<FlowDef> {
		self.find_flow(txn, id).await?.ok_or_else(|| {
			error!(internal!(
				"Flow with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}
}
