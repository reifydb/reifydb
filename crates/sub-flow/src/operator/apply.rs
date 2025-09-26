use reifydb_core::{
	flow::FlowChange,
	interface::{FlowNodeId, Transaction},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::operator::Operator;

pub struct ApplyOperator<T: Transaction> {
	node: FlowNodeId,
	inner: Box<dyn Operator<T>>,
}

impl<T: Transaction> ApplyOperator<T> {
	pub fn new(node: FlowNodeId, inner: Box<dyn Operator<T>>) -> Self {
		Self {
			node,
			inner,
		}
	}
}

impl<T: Transaction> Operator<T> for ApplyOperator<T> {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		self.inner.apply(txn, change, evaluator)
	}
}
