// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	FlowEdgeId, FlowId, FlowNodeId, UnderlyingCommandTransaction
	,
};

use crate::sequence::{
	generator::u64::GeneratorU64,
	system::{FLOW_EDGE_KEY, FLOW_KEY, FLOW_NODE_KEY},
};

pub fn next_flow_id(
	txn: &mut impl UnderlyingCommandTransaction,
) -> crate::Result<FlowId> {
	GeneratorU64::next(txn, &FLOW_KEY, None).map(FlowId)
}

pub fn next_flow_node_id(
	txn: &mut impl UnderlyingCommandTransaction,
) -> crate::Result<FlowNodeId> {
	GeneratorU64::next(txn, &FLOW_NODE_KEY, None).map(FlowNodeId)
}

pub fn next_flow_edge_id(
	txn: &mut impl UnderlyingCommandTransaction,
) -> crate::Result<FlowEdgeId> {
	GeneratorU64::next(txn, &FLOW_EDGE_KEY, None).map(FlowEdgeId)
}
