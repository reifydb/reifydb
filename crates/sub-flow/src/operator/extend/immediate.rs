use reifydb_core::{
	flow::FlowChange,
	interface::{Transaction, expression::Expression},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::Operator;

pub struct ExtendOperator {
	pub(crate) projections: Vec<Expression<'static>>,
}

impl ExtendOperator {
	pub fn new(projections: Vec<Expression<'static>>) -> Self {
		Self {
			projections,
		}
	}
}

impl<T: Transaction> Operator<T> for ExtendOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row extend processing
		// For now, just pass through all changes
		Ok(change)
	}
}
