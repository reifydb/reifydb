// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Registrar for transactional view flows.
//!
//! Detects whether a newly-discovered [`FlowDag`] is a transactional view flow,
//! and if so registers it in the transactional [`FlowEngine`].

use std::sync::{Arc, RwLock};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::{flow::FlowId, view::ViewKind};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{flow::FlowDag, loader::load_flow_dag, node::FlowNodeType};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

use crate::engine::FlowEngine;

/// Detects whether a newly-discovered `FlowDag` is a transactional view flow,
/// and if so registers it in the transactional `FlowEngine`.
pub struct TransactionalFlowRegistrar {
	pub flow_engine: Arc<RwLock<FlowEngine>>,
	pub engine: StandardEngine,
	pub catalog: Catalog,
}

impl TransactionalFlowRegistrar {
	/// Try to register a flow as a transactional view flow.
	///
	/// Returns `true` if the flow was transactional and was registered,
	/// `false` if it is a deferred (or other) flow and should be handled elsewhere.
	pub fn try_register(&self, flow: FlowDag) -> Result<bool> {
		if !self.is_transactional_view_flow(&flow) {
			return Ok(false);
		}

		let mut engine = self.flow_engine.write().unwrap();

		// Already registered (e.g. post-commit interceptor raced with CDC dispatcher).
		if engine.flows.contains_key(&flow.id) {
			return Ok(true);
		}

		let mut cmd = self.engine.begin_command()?;
		engine.register(&mut cmd, flow)?;
		// Registration performs only catalog reads — no writes were made to cmd.
		// Dropping cmd without commit is safe (auto-rollback is a no-op).
		Ok(true)
	}

	/// Load a flow by ID from the catalog and register it if it is transactional.
	///
	/// Used by the post-commit interceptor to eagerly register transactional flows
	/// at commit time, before CDC polling picks them up.
	pub fn try_register_by_id(&self, flow_id: FlowId) -> Result<()> {
		let mut query = self.engine.begin_query()?;
		let flow = load_flow_dag(&self.catalog, &mut Transaction::Query(&mut query), flow_id)?;
		self.try_register(flow)?;
		Ok(())
	}

	/// Returns `true` if any `SinkView` node in the flow writes to a transactional view.
	fn is_transactional_view_flow(&self, flow: &FlowDag) -> bool {
		let mut query = match self.engine.begin_query() {
			Ok(q) => q,
			Err(_) => return false,
		};

		for node_id in flow.get_node_ids() {
			if let Some(node) = flow.get_node(&node_id) {
				if let FlowNodeType::SinkView {
					view,
				} = &node.ty
				{
					if let Ok(Some(def)) =
						self.catalog.find_view(&mut Transaction::Query(&mut query), *view)
					{
						if def.kind == ViewKind::Transactional {
							return true;
						}
					}
				}
			}
		}

		false
	}
}
