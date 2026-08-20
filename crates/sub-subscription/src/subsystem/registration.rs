// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_engine::subscription::SubscriptionContext;
use reifydb_flow::{context::FlowContext, engine::FlowEngineInner, operator::apply::ApplyOperator};
use reifydb_rql::flow::{flow::FlowDag, operator::OperatorDef};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::delivery::{DeliveryBuffer, sink::EphemeralSinkSubscriptionOperator};

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
	for operator_id in flow.topological_order() {
		let operator = flow.get_operator(operator_id).unwrap();
		match &operator.ty {
			OperatorDef::SinkSubscription {
				..
			} => {
				let parent_schema = engine
					.operator(operator.inputs[0])
					.expect("Parent operator not found")
					.output_schema();
				let op = EphemeralSinkSubscriptionOperator::new(*operator_id, ctx.id, delivery.clone());
				engine.insert_operator(
					*operator_id,
					Box::new(ApplyOperator::new(parent_schema, *operator_id, Box::new(op), None)),
				);
			}
			_ => {
				engine.add_core(txn, &flow, operator, &flow_ctx)?;
			}
		}
	}
	engine.register_flow_dag(flow);
	Ok(())
}
