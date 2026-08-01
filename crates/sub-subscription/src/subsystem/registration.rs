// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_engine::subscription::SubscriptionContext;
use reifydb_rql::flow::{flow::FlowDag, operator::OperatorDef};
use reifydb_sub_flow::{
	context::FlowContext,
	engine::FlowEngineInner,
	operator::{OperatorCell, apply::ApplyOperator},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::sink::{DeliveryBuffer, operator::EphemeralSinkSubscriptionOperator};

pub(crate) fn register_ephemeral_flow(
	engine: &mut FlowEngineInner,
	txn: &mut Transaction<'_>,
	flow: FlowDag,
	ctx: &SubscriptionContext,
	delivery: Arc<DeliveryBuffer>,
) -> Result<()> {
	let flow_ctx = Arc::new(FlowContext {
		identity: ctx.identity,
		symbols: ctx.symbols.clone(),
		params: ctx.params.clone(),
	});
	for node_id in flow.topological_order()? {
		let node = flow.get_node(&node_id).unwrap();
		match &node.ty {
			OperatorDef::SinkSubscription {
				..
			} => {
				let parent = engine.operator(node.inputs[0]).expect("Parent operator not found");
				let op = EphemeralSinkSubscriptionOperator::new(
					parent.clone(),
					node_id,
					ctx.id,
					delivery.clone(),
				);
				engine.insert_operator(
					node_id,
					OperatorCell::new(ApplyOperator::new(parent, node_id, Box::new(op), None)),
				);
			}
			_ => {
				engine.add(txn, &flow, node, &flow_ctx)?;
			}
		}
	}
	engine.register_flow_dag(flow);
	Ok(())
}
