// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{flow::FlowId, view::ViewKind},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{flow::FlowDag, loader::load_flow_dag, node::FlowNodeType};
use reifydb_transaction::transaction::{Transaction, query::QueryTransaction};
use reifydb_value::{Result, error::Error, value::identity::IdentityId};

use crate::{engine::FlowEngine, error::FlowGraphError, lineage::FlowLineageTracker};

pub struct TransactionalFlowRegistry {
	pub flow_engine: FlowEngine,
	pub engine: StandardEngine,
	pub catalog: Catalog,
	pub lineage: FlowLineageTracker,
}

impl TransactionalFlowRegistry {
	pub fn try_register(&self, flow: FlowDag, query: &mut QueryTransaction) -> Result<bool> {
		self.lineage.add(flow.clone());
		self.try_register_with_query(flow, query)
	}

	pub fn try_register_by_id_at_version(&self, flow_id: FlowId, version: CommitVersion) -> Result<()> {
		let lease = self.engine.acquire_version_lease(version)?;
		let mut query = self.engine.begin_query_at_version(&lease, IdentityId::system())?;
		let flow = load_flow_dag(&mut Transaction::Query(&mut query), flow_id)?;
		self.lineage.add(flow.clone());
		self.try_register_with_query(flow, &mut query)?;
		Ok(())
	}

	fn try_register_with_query(&self, flow: FlowDag, query: &mut QueryTransaction) -> Result<bool> {
		if !self.is_transactional_view_flow(&flow, query)? {
			return Ok(false);
		}

		let mut engine = self.flow_engine.write();

		if engine.flows.contains_key(&flow.id) {
			return Ok(true);
		}

		let mut cmd = self.engine.begin_command(IdentityId::system())?;
		engine.register(&mut cmd, flow)?;

		Ok(true)
	}

	fn is_transactional_view_flow(&self, flow: &FlowDag, query: &mut QueryTransaction) -> Result<bool> {
		for node_id in flow.get_node_ids() {
			let Some(node) = flow.get_node(&node_id) else {
				continue;
			};
			let view = match &node.ty {
				FlowNodeType::SinkTableView {
					view,
					..
				}
				| FlowNodeType::SinkRingBufferView {
					view,
					..
				}
				| FlowNodeType::SinkSeriesView {
					view,
					..
				} => view,
				_ => continue,
			};
			let Some(def) = self.catalog.find_view(&mut Transaction::Query(query), *view)? else {
				return Err(Error::from(FlowGraphError::SinkViewNotVisibleAtRegistration {
					flow_id: flow.id.0,
					view_id: view.0,
				}));
			};
			if def.kind() == ViewKind::Transactional {
				return Ok(true);
			}
		}

		Ok(false)
	}
}
