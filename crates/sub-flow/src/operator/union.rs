use reifydb_core::interface::FlowNodeId;
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::{flow::FlowChange, operator::Operator};

pub struct UnionOperator {
	node: FlowNodeId,
}

impl UnionOperator {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}
}

impl Operator for UnionOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded union processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}
}
