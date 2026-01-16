// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowEdgeId, FlowId, FlowNodeId};
use reifydb_transaction::standard::command::StandardCommandTransaction;

use crate::store::sequence::{
	generator::u64::GeneratorU64,
	system::{FLOW_EDGE_KEY, FLOW_KEY, FLOW_NODE_KEY},
};

pub fn next_flow_id(txn: &mut StandardCommandTransaction) -> crate::Result<FlowId> {
	GeneratorU64::next(txn, &FLOW_KEY, None).map(FlowId)
}

pub fn next_flow_node_id(txn: &mut StandardCommandTransaction) -> crate::Result<FlowNodeId> {
	GeneratorU64::next(txn, &FLOW_NODE_KEY, None).map(FlowNodeId)
}

pub fn next_flow_edge_id(txn: &mut StandardCommandTransaction) -> crate::Result<FlowEdgeId> {
	GeneratorU64::next(txn, &FLOW_EDGE_KEY, None).map(FlowEdgeId)
}
