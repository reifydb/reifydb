// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::{FlowDef, FlowNodeId},
		change::Change,
	},
	value::column::columns::Columns,
};
use reifydb_type::{Result, value::row_number::RowNumber};

use crate::{Operator, transaction::FlowTransaction};

pub struct PrimitiveFlowOperator {
	node: FlowNodeId,
	#[allow(dead_code)]
	flow: FlowDef,
}

impl PrimitiveFlowOperator {
	pub fn new(node: FlowNodeId, flow: FlowDef) -> Self {
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
