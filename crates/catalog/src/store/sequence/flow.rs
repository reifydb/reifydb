// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{CommandTransaction, FlowEdgeId, FlowId, FlowNodeId};

use crate::store::sequence::{
	generator::u64::GeneratorU64,
	system::{FLOW_EDGE_KEY, FLOW_KEY, FLOW_NODE_KEY},
};

pub async fn next_flow_id(txn: &mut impl CommandTransaction) -> crate::Result<FlowId> {
	GeneratorU64::next(txn, &FLOW_KEY, None).await.map(FlowId)
}

pub async fn next_flow_node_id(txn: &mut impl CommandTransaction) -> crate::Result<FlowNodeId> {
	GeneratorU64::next(txn, &FLOW_NODE_KEY, None).await.map(FlowNodeId)
}

pub async fn next_flow_edge_id(txn: &mut impl CommandTransaction) -> crate::Result<FlowEdgeId> {
	GeneratorU64::next(txn, &FLOW_EDGE_KEY, None).await.map(FlowEdgeId)
}
