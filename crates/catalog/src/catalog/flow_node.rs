// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowId, FlowNode, FlowNodeId};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::flow_node::create", level = "debug", skip(self, txn, node_def))]
	pub fn create_flow_node(&self, txn: &mut AdminTransaction, node_def: &FlowNode) -> Result<()> {
		CatalogStore::create_flow_node(txn, node_def)
	}

	#[instrument(name = "catalog::flow_node::drop", level = "debug", skip(self, txn))]
	pub fn drop_flow_node(&self, txn: &mut AdminTransaction, node_id: FlowNodeId) -> Result<()> {
		CatalogStore::drop_flow_node(txn, node_id)
	}

	#[instrument(name = "catalog::flow_node::find", level = "trace", skip(self, txn))]
	pub fn find_flow_node(&self, txn: &mut Transaction<'_>, node_id: FlowNodeId) -> Result<Option<FlowNode>> {
		if let Some(node) = self.cache.find_flow_node_at(node_id, txn.version()) {
			return Ok(Some(node));
		}
		if let Some(node) = CatalogStore::find_flow_node(txn, node_id)? {
			warn!("flow node {:?} found in storage but not in CatalogCache", node_id);
			return Ok(Some(node));
		}
		Ok(None)
	}

	#[instrument(name = "catalog::flow_node::get", level = "trace", skip(self, txn))]
	pub fn get_flow_node(&self, txn: &mut Transaction<'_>, node_id: FlowNodeId) -> Result<FlowNode> {
		CatalogStore::get_flow_node(txn, node_id)
	}

	#[instrument(name = "catalog::flow_node::list_by_flow", level = "debug", skip(self, txn))]
	pub fn list_flow_nodes_by_flow(&self, txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<Vec<FlowNode>> {
		if let Some(nodes) = self.cache.list_flow_nodes_by_flow_at(flow_id, txn.version()) {
			return Ok(nodes);
		}
		let nodes = CatalogStore::list_flow_nodes_by_flow(txn, flow_id)?;
		if !nodes.is_empty() {
			warn!("flow nodes for flow {:?} found in storage but not in CatalogCache", flow_id);
		}
		Ok(nodes)
	}

	#[instrument(name = "catalog::flow_node::list_all", level = "debug", skip(self, txn))]
	pub fn list_flow_nodes_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<FlowNode>> {
		CatalogStore::list_flow_nodes_all(txn)
	}
}
