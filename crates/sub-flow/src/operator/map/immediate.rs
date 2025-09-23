use reifydb_core::{
	flow::FlowChange,
	interface::{Transaction, expression::Expression},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::operator::Operator;

pub struct MapOperator {
	pub(crate) projections: Vec<Expression<'static>>,
}

impl MapOperator {
	pub fn new(projections: Vec<Expression<'static>>) -> Self {
		Self {
			projections,
		}
	}
}

impl<T: Transaction> Operator<T> for MapOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row map processing
		// For now, just pass through all changes
		Ok(change)
	}
}
