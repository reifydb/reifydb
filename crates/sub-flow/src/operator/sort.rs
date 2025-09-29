use reifydb_core::interface::{FlowNodeId, Transaction, expression::Expression};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::{flow::FlowChange, operator::Operator};

pub struct SortOperator {
	node: FlowNodeId,
	_expressions: Vec<Expression<'static>>,
}

impl SortOperator {
	pub fn new(node: FlowNodeId, _expressions: Vec<Expression<'static>>) -> Self {
		Self {
			node,
			_expressions,
		}
	}
}

impl<T: Transaction> Operator<T> for SortOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row sort processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}
}
