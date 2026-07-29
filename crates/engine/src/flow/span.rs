// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_catalog::{catalog::Catalog, vtable::system::operator_store::OperatorStore};
use reifydb_core::error::diagnostic::flow::{flow_span_on_unageable_node, flow_span_without_reclaim};
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error};

pub fn check_declared_spans(
	catalog: &Catalog,
	operators: &OperatorStore,
	txn: &mut Transaction<'_>,
	flow: &FlowDag,
) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);

	for node_id in flow.topological_order()? {
		let node = flow.get_node(&node_id).unwrap();

		let Some(settings) = catalog.find_operator_settings(txn, node.id)? else {
			continue;
		};
		if settings.ttl.is_none() && settings.join.is_none() {
			continue;
		}
		if !node.ty.consults_declared_span() {
			return Err(Error(Box::new(flow_span_on_unageable_node(&flow_name, &node.ty.label()))));
		}

		let FlowNodeType::Apply {
			operator,
			..
		} = &node.ty
		else {
			continue;
		};
		let reclaims = operators
			.get(operator)
			.is_some_and(|info| info.capabilities & OperatorCapability::Reclaim.bit() != 0);
		if !reclaims {
			return Err(Error(Box::new(flow_span_without_reclaim(&flow_name, &node.ty.label()))));
		}
	}

	Ok(())
}
