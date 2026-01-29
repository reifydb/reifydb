// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowEdgeId, FlowId, FlowNodeId};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::store::sequence::{
	generator::u64::GeneratorU64,
	system::{FLOW_EDGE_KEY, FLOW_KEY, FLOW_NODE_KEY},
};

pub(crate) fn next_flow_id(txn: &mut AdminTransaction) -> crate::Result<FlowId> {
	GeneratorU64::next(txn, &FLOW_KEY, None).map(FlowId)
}

pub(crate) fn next_flow_node_id(txn: &mut AdminTransaction) -> crate::Result<FlowNodeId> {
	GeneratorU64::next(txn, &FLOW_NODE_KEY, None).map(FlowNodeId)
}

pub(crate) fn next_flow_edge_id(txn: &mut AdminTransaction) -> crate::Result<FlowEdgeId> {
	GeneratorU64::next(txn, &FLOW_EDGE_KEY, None).map(FlowEdgeId)
}
