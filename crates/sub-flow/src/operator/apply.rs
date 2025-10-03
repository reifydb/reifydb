use reifydb_core::interface::FlowNodeId;
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::{flow::FlowChange, operator::Operator};

pub struct ApplyOperator {
	node: FlowNodeId,
	inner: Box<dyn Operator>,
}

impl ApplyOperator {
	pub fn new(node: FlowNodeId, inner: Box<dyn Operator>) -> Self {
		Self {
			node,
			inner,
		}
	}
}

impl Operator for ApplyOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		self.inner.apply(txn, change, evaluator)
	}
}
