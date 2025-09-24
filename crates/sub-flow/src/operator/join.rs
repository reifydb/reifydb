use reifydb_core::{
	flow::FlowChange,
	interface::{FlowNodeId, Transaction},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::operator::Operator;

pub struct JoinOperator {
	node: FlowNodeId,
}

impl JoinOperator {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}
}

impl<T: Transaction> Operator<T> for JoinOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row join processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.diffs))
	}
}
