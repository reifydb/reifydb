use reifydb_core::{
	flow::FlowChange,
	interface::{Transaction, expression::Expression},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::operator::Operator;

pub struct SortOperator {
	_expressions: Vec<Expression<'static>>,
}

impl SortOperator {
	pub fn new(_expressions: Vec<Expression<'static>>) -> Self {
		Self {
			_expressions,
		}
	}
}

impl<T: Transaction> Operator<T> for SortOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row sort processing
		// For now, just pass through all changes
		Ok(change)
	}
}
