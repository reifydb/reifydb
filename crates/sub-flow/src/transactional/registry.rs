// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, RwLock};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::{flow::FlowId, view::ViewKind};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{flow::FlowDag, loader::load_flow_dag, node::FlowNodeType};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, value::identity::IdentityId};

use crate::engine::FlowEngine;

pub struct TransactionalFlowRegistry {
	pub flow_engine: Arc<RwLock<FlowEngine>>,
	pub engine: StandardEngine,
	pub catalog: Catalog,
}

impl TransactionalFlowRegistry {
	pub fn try_register(&self, flow: FlowDag) -> Result<bool> {
		if !self.is_transactional_view_flow(&flow) {
			return Ok(false);
		}

		let mut engine = self.flow_engine.write().unwrap();

		if engine.flows.contains_key(&flow.id) {
			return Ok(true);
		}

		let mut cmd = self.engine.begin_command(IdentityId::system())?;
		engine.register(&mut cmd, flow)?;

		Ok(true)
	}

	pub fn try_register_by_id(&self, flow_id: FlowId) -> Result<()> {
		let mut query = self.engine.begin_query(IdentityId::system())?;
		let flow = load_flow_dag(&self.catalog, &mut Transaction::Query(&mut query), flow_id)?;
		self.try_register(flow)?;
		Ok(())
	}

	fn is_transactional_view_flow(&self, flow: &FlowDag) -> bool {
		let mut query = match self.engine.begin_query(IdentityId::system()) {
			Ok(q) => q,
			Err(_) => return false,
		};

		for node_id in flow.get_node_ids() {
			if let Some(node) = flow.get_node(&node_id) {
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
					} => Some(view),
					_ => None,
				};
				if let Some(view) = view
					&& let Ok(Some(def)) =
						self.catalog.find_view(&mut Transaction::Query(&mut query), *view)
					&& def.kind() == ViewKind::Transactional
				{
					return true;
				}
			}
		}

		false
	}
}
