// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowEdge, FlowEdgeId, FlowId};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::flow_edge::create", level = "info", skip(self, txn, edge_def))]
	pub fn create_flow_edge(&self, txn: &mut AdminTransaction, edge_def: &FlowEdge) -> Result<()> {
		CatalogStore::create_flow_edge(txn, edge_def)
	}

	#[instrument(name = "catalog::flow_edge::drop", level = "info", skip(self, txn))]
	pub fn drop_flow_edge(&self, txn: &mut AdminTransaction, edge_id: FlowEdgeId) -> Result<()> {
		CatalogStore::drop_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::find", level = "trace", skip(self, txn))]
	pub fn find_flow_edge(&self, txn: &mut Transaction<'_>, edge_id: FlowEdgeId) -> Result<Option<FlowEdge>> {
		if let Some(edge) = self.cache.find_flow_edge_at(edge_id, txn.version()) {
			return Ok(Some(edge));
		}
		if let Some(edge) = CatalogStore::find_flow_edge(txn, edge_id)? {
			warn!("flow edge {:?} found in storage but not in CatalogCache", edge_id);
			return Ok(Some(edge));
		}
		Ok(None)
	}

	#[instrument(name = "catalog::flow_edge::get", level = "trace", skip(self, txn))]
	pub fn get_flow_edge(&self, txn: &mut Transaction<'_>, edge_id: FlowEdgeId) -> Result<FlowEdge> {
		CatalogStore::get_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::list_by_flow", level = "trace", skip(self, txn))]
	pub fn list_flow_edges_by_flow(&self, txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<Vec<FlowEdge>> {
		if let Some(edges) = self.cache.list_flow_edges_by_flow_at(flow_id, txn.version()) {
			return Ok(edges);
		}
		let edges = CatalogStore::list_flow_edges_by_flow(txn, flow_id)?;
		if !edges.is_empty() {
			warn!("flow edges for flow {:?} found in storage but not in CatalogCache", flow_id);
		}
		Ok(edges)
	}

	#[instrument(name = "catalog::flow_edge::list_all", level = "trace", skip(self, txn))]
	pub fn list_flow_edges_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<FlowEdge>> {
		CatalogStore::list_flow_edges_all(txn)
	}
}
