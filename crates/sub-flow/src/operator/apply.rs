use std::marker::PhantomData;

use reifydb_core::{
	flow::FlowChange,
	interface::{FlowNodeId, Transaction},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::operator::Operator;

pub struct ApplyOperator<T: Transaction> {
	node: FlowNodeId,
	_marker: PhantomData<T>,
}

impl<T: Transaction> ApplyOperator<T> {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
			_marker: PhantomData,
		}
	}
}

impl<T: Transaction> Operator<T> for ApplyOperator<T> {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row apply processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.diffs))
	}
}
