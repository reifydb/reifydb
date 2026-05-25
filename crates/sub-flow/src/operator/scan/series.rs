// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::interface::{catalog::flow::FlowNodeId, change::Change};
use reifydb_type::Result;

use crate::{Operator, transaction::FlowTransaction};

pub struct PrimitiveSeriesOperator {
	node: FlowNodeId,
}

impl PrimitiveSeriesOperator {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}
}

impl Operator for PrimitiveSeriesOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		Ok(Change::from_flow(self.node, change.version, change.diffs, change.changed_at))
	}
}
