// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::{Flow, FlowNodeId},
		change::Change,
	},
	value::column::columns::Columns,
};
use reifydb_type::{Result, value::row_number::RowNumber};

use crate::{Operator, transaction::FlowTransaction};

pub struct PrimitiveFlowOperator {
	node: FlowNodeId,
	#[allow(dead_code)]
	flow: Flow,
}

impl PrimitiveFlowOperator {
	pub fn new(node: FlowNodeId, flow: Flow) -> Self {
		Self {
			node,
			flow,
		}
	}
}

impl Operator for PrimitiveFlowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		Ok(change)
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		// TODO: Implement flow pull
		unimplemented!()
	}
}
