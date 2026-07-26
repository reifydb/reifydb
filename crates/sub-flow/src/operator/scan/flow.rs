// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::interface::{
	catalog::flow::{Flow, FlowNodeId},
	change::Change,
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_value::Result;

pub struct SourceFlowOperator {
	node: FlowNodeId,
	#[allow(dead_code)]
	flow: Flow,
}

impl SourceFlowOperator {
	pub fn new(node: FlowNodeId, flow: Flow) -> Self {
		Self {
			node,
			flow,
		}
	}
}

impl Operator for SourceFlowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		Ok(change)
	}
}
