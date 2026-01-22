// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		change::CatalogTrackFlowChangeOperations,
		flow::{FlowDef, FlowId, FlowStatus},
		id::NamespaceId,
	},
	internal,
};
use reifydb_transaction::{
	change::TransactionalFlowChanges,
	standard::{IntoStandardTransaction, StandardTransaction, command::StandardCommandTransaction},
};
use reifydb_type::{error, fragment::Fragment};
use tracing::{instrument, warn};

use crate::{
	CatalogStore,
	catalog::Catalog,
	store::{flow::create::FlowToCreate as StoreFlowToCreate, sequence::flow as flow_sequence},
};

#[derive(Debug, Clone)]
pub struct FlowToCreate {
	pub fragment: Option<Fragment>,
	pub name: String,
	pub namespace: NamespaceId,
	pub status: FlowStatus,
}

impl From<FlowToCreate> for StoreFlowToCreate {
	fn from(to_create: FlowToCreate) -> Self {
		StoreFlowToCreate {
			fragment: to_create.fragment,
			name: to_create.name,
			namespace: to_create.namespace,
			status: to_create.status,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::flow::find", level = "trace", skip(self, txn))]
	pub fn find_flow<T: IntoStandardTransaction>(&self, txn: &mut T, id: FlowId) -> crate::Result<Option<FlowDef>> {
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
				if let Some(flow) = self.materialized.find_flow_at(id, cmd.version()) {
					return Ok(Some(flow));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow(cmd, id)? {
					warn!("Flow with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(flow) = self.materialized.find_flow_at(id, qry.version()) {
					return Ok(Some(flow));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow(qry, id)? {
					warn!("Flow with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(flow));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::flow::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_flow_by_name<T: IntoStandardTransaction>(
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
				if let Some(flow) =
					self.materialized.find_flow_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(flow));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow_by_name(cmd, namespace, name)? {
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
				if let Some(flow) =
					self.materialized.find_flow_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(flow));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(flow) = CatalogStore::find_flow_by_name(qry, namespace, name)? {
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
	pub fn get_flow<T: IntoStandardTransaction>(&self, txn: &mut T, id: FlowId) -> crate::Result<FlowDef> {
		self.find_flow(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Flow with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::flow::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_flow(
		&self,
		txn: &mut StandardCommandTransaction,
		to_create: FlowToCreate,
	) -> crate::Result<FlowDef> {
		let flow = CatalogStore::create_flow(txn, to_create.into())?;
		txn.track_flow_def_created(flow.clone())?;
		Ok(flow)
	}

	/// Create a flow with a specific ID (for subscription flows where FlowId == SubscriptionId).
	/// This skips the name uniqueness check since the ID is guaranteed unique by the sequence.
	#[instrument(name = "catalog::flow::create_with_id", level = "debug", skip(self, txn, to_create))]
	pub fn create_flow_with_id(
		&self,
		txn: &mut StandardCommandTransaction,
		flow_id: FlowId,
		to_create: FlowToCreate,
	) -> crate::Result<FlowDef> {
		let flow = CatalogStore::create_flow_with_id(txn, flow_id, to_create.into())?;
		txn.track_flow_def_created(flow.clone())?;
		Ok(flow)
	}

	#[instrument(name = "catalog::flow::delete", level = "debug", skip(self, txn))]
	pub fn delete_flow(&self, txn: &mut StandardCommandTransaction, flow: FlowDef) -> crate::Result<()> {
		CatalogStore::delete_flow(txn, flow.id)?;
		txn.track_flow_def_deleted(flow)?;
		Ok(())
	}

	#[instrument(name = "catalog::flow::list_all", level = "debug", skip(self, txn))]
	pub fn list_flows_all<T: IntoStandardTransaction>(&self, txn: &mut T) -> crate::Result<Vec<FlowDef>> {
		CatalogStore::list_flows_all(txn)
	}

	#[instrument(name = "catalog::flow::update_name", level = "debug", skip(self, txn))]
	pub fn update_flow_name(
		&self,
		txn: &mut StandardCommandTransaction,
		flow_id: FlowId,
		new_name: String,
	) -> crate::Result<()> {
		CatalogStore::update_flow_name(txn, flow_id, new_name)
	}

	#[instrument(name = "catalog::flow::update_status", level = "debug", skip(self, txn))]
	pub fn update_flow_status(
		&self,
		txn: &mut StandardCommandTransaction,
		flow_id: FlowId,
		status: FlowStatus,
	) -> crate::Result<()> {
		CatalogStore::update_flow_status(txn, flow_id, status)
	}

	#[instrument(name = "catalog::flow::next_id", level = "trace", skip(self, txn))]
	pub fn next_flow_id(&self, txn: &mut StandardCommandTransaction) -> crate::Result<FlowId> {
		flow_sequence::next_flow_id(txn)
	}

	#[instrument(name = "catalog::flow::next_node_id", level = "trace", skip(self, txn))]
	pub fn next_flow_node_id(
		&self,
		txn: &mut StandardCommandTransaction,
	) -> crate::Result<reifydb_core::interface::catalog::flow::FlowNodeId> {
		flow_sequence::next_flow_node_id(txn)
	}

	#[instrument(name = "catalog::flow::next_edge_id", level = "trace", skip(self, txn))]
	pub fn next_flow_edge_id(
		&self,
		txn: &mut StandardCommandTransaction,
	) -> crate::Result<reifydb_core::interface::catalog::flow::FlowEdgeId> {
		flow_sequence::next_flow_edge_id(txn)
	}
}
