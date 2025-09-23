use std::marker::PhantomData;

use reifydb_core::{flow::FlowChange, interface::Transaction};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::Operator;

pub struct ApplyOperator<T: Transaction> {
	_marker: PhantomData<T>,
}

impl<T: Transaction> ApplyOperator<T> {
	pub fn new() -> Self {
		Self {
			_marker: PhantomData,
		}
	}
}

impl<T: Transaction> Operator<T> for ApplyOperator<T> {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row apply processing
		// For now, just pass through all changes
		Ok(change)
	}
}
