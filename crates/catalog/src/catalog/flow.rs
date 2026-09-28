// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	flow::dag::FlowDag,
	interface::catalog::{
		change::CatalogTrackFlowChangeOperations,
		flow::{Flow, FlowEdgeId, FlowEntry, FlowId, FlowStatus, OperatorId},
		id::NamespaceId,
	},
	internal,
};
use reifydb_transaction::{
	change::TransactionalFlowChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{error, fragment::Fragment};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::{flow::create::FlowToCreate as StoreFlowToCreate, sequence::flow as flow_sequence},
};

#[derive(Debug, Clone)]
pub struct FlowToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub status: FlowStatus,
}

impl From<FlowToCreate> for StoreFlowToCreate {
	fn from(to_create: FlowToCreate) -> Self {
		StoreFlowToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			status: to_create.status,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::flow::find", level = "trace", skip(self, txn))]
	pub fn find_flow(&self, txn: &mut Transaction<'_>, id: FlowId) -> Result<Option<Flow>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(flow) = self.cache.find_flow_at(id, cmd.version()) {
					return Ok(Some(flow));
				}

				if let Some(flow) = CatalogStore::find_flow(&mut Transaction::Command(&mut *cmd), id)? {
					warn!("Flow with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(entry) = TransactionalFlowChanges::find_flow(admin, id) {
					return Ok(Some(entry.flow.clone()));
				}

				if TransactionalFlowChanges::is_flow_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(flow) = self.cache.find_flow_at(id, admin.version()) {
					return Ok(Some(flow));
				}

				if let Some(flow) = CatalogStore::find_flow(&mut Transaction::Admin(&mut *admin), id)? {
					warn!("Flow with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(flow) = self.cache.find_flow_at(id, qry.version()) {
					return Ok(Some(flow));
				}

				if let Some(flow) = CatalogStore::find_flow(&mut Transaction::Query(&mut *qry), id)? {
					warn!("Flow with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(entry) = TransactionalFlowChanges::find_flow(t.inner, id) {
					return Ok(Some(entry.flow.clone()));
				}
				if TransactionalFlowChanges::is_flow_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(flow) = self.cache.find_flow_at(id, t.inner.version()) {
					return Ok(Some(flow));
				}
				if let Some(flow) =
					CatalogStore::find_flow(&mut Transaction::Test(Box::new(t.reborrow())), id)?
				{
					return Ok(Some(flow));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::flow::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_flow_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Flow>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(flow) = self.cache.find_flow_by_name_at(namespace, name, cmd.version()) {
					return Ok(Some(flow));
				}

				if let Some(flow) = CatalogStore::find_flow_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"Flow '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(entry) = TransactionalFlowChanges::find_flow_by_name(admin, namespace, name)
				{
					return Ok(Some(entry.flow.clone()));
				}

				if TransactionalFlowChanges::is_flow_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				if let Some(flow) = self.cache.find_flow_by_name_at(namespace, name, admin.version()) {
					return Ok(Some(flow));
				}

				if let Some(flow) = CatalogStore::find_flow_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"Flow '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(flow) = self.cache.find_flow_by_name_at(namespace, name, qry.version()) {
					return Ok(Some(flow));
				}

				if let Some(flow) = CatalogStore::find_flow_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"Flow '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(flow));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(entry) =
					TransactionalFlowChanges::find_flow_by_name(t.inner, namespace, name)
				{
					return Ok(Some(entry.flow.clone()));
				}
				if TransactionalFlowChanges::is_flow_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(flow) = CatalogStore::find_flow_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					namespace,
					name,
				)? {
					return Ok(Some(flow));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::flow::get", level = "trace", skip(self, txn))]
	pub fn get_flow(&self, txn: &mut Transaction<'_>, id: FlowId) -> Result<Flow> {
		self.find_flow(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Flow with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::flow::find_dag", level = "trace", skip(self, txn))]
	pub fn find_flow_dag(&self, txn: &mut Transaction<'_>, id: FlowId) -> Result<Option<FlowDag>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(entry) = self.cache.find_flow_entry_at(id, cmd.version()) {
					return Ok(Some(entry.dag));
				}

				let mut txn = Transaction::Command(&mut *cmd);
				if CatalogStore::find_flow(&mut txn, id)?.is_some() {
					return Err(error!(internal!(
						"Flow DAG with ID {:?} found in storage but not in CatalogCache",
						id
					)));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(entry) = TransactionalFlowChanges::find_flow(admin, id) {
					return Ok(Some(entry.dag.clone()));
				}

				if TransactionalFlowChanges::is_flow_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(entry) = self.cache.find_flow_entry_at(id, admin.version()) {
					return Ok(Some(entry.dag));
				}

				let mut txn = Transaction::Admin(&mut *admin);
				if CatalogStore::find_flow(&mut txn, id)?.is_some() {
					return Err(error!(internal!(
						"Flow DAG with ID {:?} found in storage but not in CatalogCache",
						id
					)));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(entry) = self.cache.find_flow_entry_at(id, qry.version()) {
					return Ok(Some(entry.dag));
				}

				let mut txn = Transaction::Query(&mut *qry);
				if CatalogStore::find_flow(&mut txn, id)?.is_some() {
					return Err(error!(internal!(
						"Flow DAG with ID {:?} found in storage but not in CatalogCache",
						id
					)));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(entry) = TransactionalFlowChanges::find_flow(t.inner, id) {
					return Ok(Some(entry.dag.clone()));
				}
				if TransactionalFlowChanges::is_flow_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(entry) = self.cache.find_flow_entry_at(id, t.inner.version()) {
					return Ok(Some(entry.dag));
				}
				let mut txn = Transaction::Test(Box::new(t.reborrow()));
				if CatalogStore::find_flow(&mut txn, id)?.is_some() {
					return Err(error!(internal!(
						"Flow DAG with ID {:?} found in storage but not in CatalogCache",
						id
					)));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::flow::get_dag", level = "trace", skip(self, txn))]
	pub fn get_flow_dag(&self, txn: &mut Transaction<'_>, id: FlowId) -> Result<FlowDag> {
		self.find_flow_dag(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Flow DAG with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::flow::create", level = "info", skip(self, txn, to_create, dag))]
	pub fn create_flow(&self, txn: &mut AdminTransaction, to_create: FlowToCreate, dag: FlowDag) -> Result<Flow> {
		let flow = CatalogStore::create_flow(txn, dag.id(), to_create.into())?;
		for (_, node) in dag.graph.nodes() {
			CatalogStore::create_operator(txn, flow.id, node)?;
		}
		for edge in dag.graph.edges() {
			CatalogStore::create_flow_edge(txn, edge)?;
		}
		txn.track_flow_created(FlowEntry {
			flow: flow.clone(),
			dag,
		})?;
		Ok(flow)
	}

	#[instrument(name = "catalog::flow::drop", level = "info", skip(self, txn))]
	pub fn drop_flow(&self, txn: &mut AdminTransaction, flow: Flow) -> Result<()> {
		let dag = self.get_flow_dag(&mut Transaction::Admin(&mut *txn), flow.id)?;
		CatalogStore::drop_flow(txn, flow.id)?;
		txn.track_flow_deleted(FlowEntry {
			flow,
			dag,
		})?;
		Ok(())
	}

	#[instrument(name = "catalog::flow::list_all", level = "trace", skip(self, txn))]
	pub fn list_flows_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Flow>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self.cache.list_all_flows_at(cmd.version())),
			Transaction::Admin(admin) => {
				let mut flows = self.cache.list_all_flows_at(admin.version());
				for change in &admin.changes.flow {
					if let Some(entry) = &change.post
						&& !flows.iter().any(|existing| existing.id == entry.flow.id)
					{
						flows.push(entry.flow.clone());
					}
				}

				flows.retain(|f| !admin.is_flow_deleted(f.id));
				Ok(flows)
			}
			Transaction::Query(qry) => Ok(self.cache.list_all_flows_at(qry.version())),
			Transaction::Test(t) => {
				let mut flows = self.cache.list_all_flows_at(t.inner.version());
				for change in &t.inner.changes.flow {
					if let Some(entry) = &change.post
						&& !flows.iter().any(|existing| existing.id == entry.flow.id)
					{
						flows.push(entry.flow.clone());
					}
				}

				flows.retain(|flw| !t.inner.is_flow_deleted(flw.id));
				Ok(flows)
			}
		}
	}

	#[instrument(name = "catalog::flow::list_dags_asc", level = "trace", skip(self, txn))]
	pub fn list_flow_dags_asc(&self, txn: &mut Transaction<'_>) -> Result<Vec<FlowDag>> {
		let mut entries = match txn.reborrow() {
			Transaction::Command(cmd) => self.cache.list_flow_entries_at(cmd.version()),
			Transaction::Admin(admin) => {
				let mut entries = self.cache.list_flow_entries_at(admin.version());
				for change in &admin.changes.flow {
					if let Some(entry) = &change.post
						&& !entries.iter().any(|existing| existing.flow.id == entry.flow.id)
					{
						entries.push(entry.clone());
					}
				}

				entries.retain(|entry| !admin.is_flow_deleted(entry.flow.id));
				entries
			}
			Transaction::Query(qry) => self.cache.list_flow_entries_at(qry.version()),
			Transaction::Test(t) => {
				let mut entries = self.cache.list_flow_entries_at(t.inner.version());
				for change in &t.inner.changes.flow {
					if let Some(entry) = &change.post
						&& !entries.iter().any(|existing| existing.flow.id == entry.flow.id)
					{
						entries.push(entry.clone());
					}
				}

				entries.retain(|entry| !t.inner.is_flow_deleted(entry.flow.id));
				entries
			}
		};
		entries.sort_by_key(|entry| entry.flow.id);
		Ok(entries.into_iter().map(|entry| entry.dag).collect())
	}

	#[instrument(name = "catalog::flow::update_name", level = "debug", skip(self, txn))]
	pub fn update_flow_name(&self, txn: &mut AdminTransaction, flow_id: FlowId, new_name: String) -> Result<()> {
		CatalogStore::update_flow_name(txn, flow_id, new_name)
	}

	#[instrument(name = "catalog::flow::update_status", level = "debug", skip(self, txn))]
	pub fn update_flow_status(
		&self,
		txn: &mut AdminTransaction,
		flow_id: FlowId,
		status: FlowStatus,
	) -> Result<()> {
		CatalogStore::update_flow_status(txn, flow_id, status)
	}

	#[instrument(name = "catalog::flow::next_id", level = "trace", skip(self, txn))]
	pub fn next_flow_id(&self, txn: &mut AdminTransaction) -> Result<FlowId> {
		flow_sequence::next_flow_id(txn)
	}

	#[instrument(name = "catalog::flow::next_node_id", level = "trace", skip(self, txn))]
	pub fn next_operator_id(&self, txn: &mut AdminTransaction) -> Result<OperatorId> {
		flow_sequence::next_operator_id(txn)
	}

	#[instrument(name = "catalog::flow::next_edge_id", level = "trace", skip(self, txn))]
	pub fn next_flow_edge_id(&self, txn: &mut AdminTransaction) -> Result<FlowEdgeId> {
		flow_sequence::next_flow_edge_id(txn)
	}
}
