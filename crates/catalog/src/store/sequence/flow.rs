// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowEdgeId, FlowId, OperatorId};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::error::ValueError;

use crate::store::sequence::{
	generator::u64::GeneratorU64,
	system::{FLOW_EDGE_KEY, FLOW_KEY, FLOW_NODE_KEY},
};

pub(crate) fn next_flow_id(txn: &mut AdminTransaction) -> Result<FlowId, ValueError> {
	GeneratorU64::next(txn, &FLOW_KEY, None).map(FlowId)
}

pub(crate) fn next_operator_id(txn: &mut AdminTransaction) -> Result<OperatorId, ValueError> {
	GeneratorU64::next(txn, &FLOW_NODE_KEY, None).map(OperatorId)
}

pub(crate) fn next_flow_edge_id(txn: &mut AdminTransaction) -> Result<FlowEdgeId, ValueError> {
	GeneratorU64::next(txn, &FLOW_EDGE_KEY, None).map(FlowEdgeId)
}
