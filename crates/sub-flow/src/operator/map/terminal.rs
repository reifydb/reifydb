use reifydb_core::{
	flow::FlowChange,
	interface::{Transaction, ViewDef, expression::Expression},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::Operator;

pub struct MapTerminalOperator {
	projections: Vec<Expression<'static>>,
	sink_def: ViewDef,
}

impl MapTerminalOperator {
	pub fn new(projections: Vec<Expression<'static>>, sink_def: ViewDef) -> Self {
		Self {
			projections,
			sink_def,
		}
	}
}

impl<T: Transaction> Operator<T> for MapTerminalOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row map terminal processing
		// For now, just pass through all changes
		Ok(change)
	}
}
