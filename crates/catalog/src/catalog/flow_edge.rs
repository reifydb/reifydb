// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowEdge, FlowEdgeId, FlowId};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::flow_edge::create", level = "debug", skip(self, txn, edge_def))]
	pub fn create_flow_edge(&self, txn: &mut AdminTransaction, edge_def: &FlowEdge) -> Result<()> {
		CatalogStore::create_flow_edge(txn, edge_def)
	}

	#[instrument(name = "catalog::flow_edge::drop", level = "debug", skip(self, txn))]
	pub fn drop_flow_edge(&self, txn: &mut AdminTransaction, edge_id: FlowEdgeId) -> Result<()> {
		CatalogStore::drop_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::find", level = "trace", skip(self, txn))]
	pub fn find_flow_edge(&self, txn: &mut Transaction<'_>, edge_id: FlowEdgeId) -> Result<Option<FlowEdge>> {
		CatalogStore::find_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::get", level = "trace", skip(self, txn))]
	pub fn get_flow_edge(&self, txn: &mut Transaction<'_>, edge_id: FlowEdgeId) -> Result<FlowEdge> {
		CatalogStore::get_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::list_by_flow", level = "debug", skip(self, txn))]
	pub fn list_flow_edges_by_flow(&self, txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<Vec<FlowEdge>> {
		CatalogStore::list_flow_edges_by_flow(txn, flow_id)
	}

	#[instrument(name = "catalog::flow_edge::list_all", level = "debug", skip(self, txn))]
	pub fn list_flow_edges_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<FlowEdge>> {
		CatalogStore::list_flow_edges_all(txn)
	}
}
