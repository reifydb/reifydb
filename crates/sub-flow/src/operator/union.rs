use reifydb_core::{flow::FlowChange, interface::Transaction};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::operator::Operator;

pub struct UnionOperator;

impl<T: Transaction> Operator<T> for UnionOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row union processing
		// For now, just pass through all changes
		Ok(change)
	}
}
