use reifydb_core::{
	flow::FlowChange,
	interface::{FlowNodeId, Transaction},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::Operator;

pub struct TakeOperator {
	node: FlowNodeId,
	limit: usize,
}

impl TakeOperator {
	pub fn new(node: FlowNodeId, limit: usize) -> Self {
		Self {
			node,
			limit,
		}
	}
}

impl<T: Transaction> Operator<T> for TakeOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row take processing
		// For now, just pass through all changes
		Ok(change)
	}
}
