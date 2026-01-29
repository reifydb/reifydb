// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowEdgeDef, FlowEdgeId, FlowId};
use reifydb_transaction::transaction::{AsTransaction, admin::AdminTransaction};
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::flow_edge::create", level = "debug", skip(self, txn, edge_def))]
	pub fn create_flow_edge(&self, txn: &mut AdminTransaction, edge_def: &FlowEdgeDef) -> crate::Result<()> {
		CatalogStore::create_flow_edge(txn, edge_def)
	}

	#[instrument(name = "catalog::flow_edge::delete", level = "debug", skip(self, txn))]
	pub fn delete_flow_edge(&self, txn: &mut AdminTransaction, edge_id: FlowEdgeId) -> crate::Result<()> {
		CatalogStore::delete_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::find", level = "trace", skip(self, txn))]
	pub fn find_flow_edge<T: AsTransaction>(
		&self,
		txn: &mut T,
		edge_id: FlowEdgeId,
	) -> crate::Result<Option<FlowEdgeDef>> {
		CatalogStore::find_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::get", level = "trace", skip(self, txn))]
	pub fn get_flow_edge<T: AsTransaction>(&self, txn: &mut T, edge_id: FlowEdgeId) -> crate::Result<FlowEdgeDef> {
		CatalogStore::get_flow_edge(txn, edge_id)
	}

	#[instrument(name = "catalog::flow_edge::list_by_flow", level = "debug", skip(self, txn))]
	pub fn list_flow_edges_by_flow<T: AsTransaction>(
		&self,
		txn: &mut T,
		flow_id: FlowId,
	) -> crate::Result<Vec<FlowEdgeDef>> {
		CatalogStore::list_flow_edges_by_flow(txn, flow_id)
	}

	#[instrument(name = "catalog::flow_edge::list_all", level = "debug", skip(self, txn))]
	pub fn list_flow_edges_all<T: AsTransaction>(&self, txn: &mut T) -> crate::Result<Vec<FlowEdgeDef>> {
		CatalogStore::list_flow_edges_all(txn)
	}
}
