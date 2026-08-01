// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowId, Operator, OperatorId};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::operator::create", level = "info", skip(self, txn, node_def))]
	pub fn create_operator(&self, txn: &mut AdminTransaction, node_def: &Operator) -> Result<()> {
		CatalogStore::create_operator(txn, node_def)
	}

	#[instrument(name = "catalog::operator::drop", level = "info", skip(self, txn))]
	pub fn drop_operator(&self, txn: &mut AdminTransaction, node_id: OperatorId) -> Result<()> {
		CatalogStore::drop_operator(txn, node_id)
	}

	#[instrument(name = "catalog::operator::find", level = "trace", skip(self, txn))]
	pub fn find_operator(&self, txn: &mut Transaction<'_>, node_id: OperatorId) -> Result<Option<Operator>> {
		if let Some(node) = self.cache.find_operator_at(node_id, txn.version()) {
			return Ok(Some(node));
		}
		if let Some(node) = CatalogStore::find_operator(txn, node_id)? {
			warn!("flow node {:?} found in storage but not in CatalogCache", node_id);
			return Ok(Some(node));
		}
		Ok(None)
	}

	#[instrument(name = "catalog::operator::get", level = "trace", skip(self, txn))]
	pub fn get_operator(&self, txn: &mut Transaction<'_>, node_id: OperatorId) -> Result<Operator> {
		CatalogStore::get_operator(txn, node_id)
	}

	#[instrument(name = "catalog::operator::list_by_flow", level = "trace", skip(self, txn))]
	pub fn list_operators_by_flow(&self, txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<Vec<Operator>> {
		if let Some(nodes) = self.cache.list_operators_by_flow_at(flow_id, txn.version()) {
			return Ok(nodes);
		}
		let nodes = CatalogStore::list_operators_by_flow(txn, flow_id)?;
		if !nodes.is_empty() {
			warn!("flow nodes for flow {:?} found in storage but not in CatalogCache", flow_id);
		}
		Ok(nodes)
	}

	#[instrument(name = "catalog::operator::list_all", level = "trace", skip(self, txn))]
	pub fn list_operators_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Operator>> {
		CatalogStore::list_operators_all(txn)
	}
}
