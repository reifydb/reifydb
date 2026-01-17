// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowId, FlowNodeDef, FlowNodeId};
use reifydb_transaction::standard::{IntoStandardTransaction, command::StandardCommandTransaction};
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::flow_node::create", level = "debug", skip(self, txn, node_def))]
	pub fn create_flow_node(
		&self,
		txn: &mut StandardCommandTransaction,
		node_def: &FlowNodeDef,
	) -> crate::Result<()> {
		CatalogStore::create_flow_node(txn, node_def)
	}

	#[instrument(name = "catalog::flow_node::delete", level = "debug", skip(self, txn))]
	pub fn delete_flow_node(&self, txn: &mut StandardCommandTransaction, node_id: FlowNodeId) -> crate::Result<()> {
		CatalogStore::delete_flow_node(txn, node_id)
	}

	#[instrument(name = "catalog::flow_node::find", level = "trace", skip(self, txn))]
	pub fn find_flow_node<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		node_id: FlowNodeId,
	) -> crate::Result<Option<FlowNodeDef>> {
		CatalogStore::find_flow_node(txn, node_id)
	}

	#[instrument(name = "catalog::flow_node::get", level = "trace", skip(self, txn))]
	pub fn get_flow_node<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		node_id: FlowNodeId,
	) -> crate::Result<FlowNodeDef> {
		CatalogStore::get_flow_node(txn, node_id)
	}

	#[instrument(name = "catalog::flow_node::list_by_flow", level = "debug", skip(self, txn))]
	pub fn list_flow_nodes_by_flow<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		flow_id: FlowId,
	) -> crate::Result<Vec<FlowNodeDef>> {
		CatalogStore::list_flow_nodes_by_flow(txn, flow_id)
	}

	#[instrument(name = "catalog::flow_node::list_all", level = "debug", skip(self, txn))]
	pub fn list_flow_nodes_all<T: IntoStandardTransaction>(&self, txn: &mut T) -> crate::Result<Vec<FlowNodeDef>> {
		CatalogStore::list_flow_nodes_all(txn)
	}
}
