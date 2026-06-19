// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_sub_flow::{
	engine::FlowEngineInner,
	operator::{OperatorCell, Operators},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::sink::{DeliveryBuffer, operator::EphemeralSinkSubscriptionOperator};

pub(crate) fn register_ephemeral_flow(
	engine: &mut FlowEngineInner,
	txn: &mut Transaction<'_>,
	flow: FlowDag,
	subscription_id: SubscriptionId,
	delivery: Arc<DeliveryBuffer>,
) -> Result<()> {
	for node_id in flow.topological_order()? {
		let node = flow.get_node(&node_id).unwrap();
		match &node.ty {
			FlowNodeType::SinkSubscription {
				..
			} => {
				let parent = engine.operator(node.inputs[0]).expect("Parent operator not found");
				let op = EphemeralSinkSubscriptionOperator::new(
					parent,
					node_id,
					subscription_id,
					delivery.clone(),
				);
				engine.insert_operator(node_id, OperatorCell::new(Operators::Custom(Box::new(op))));
			}
			_ => {
				engine.add(txn, &flow, node)?;
			}
		}
	}
	engine.register_flow_dag(flow);
	Ok(())
}
