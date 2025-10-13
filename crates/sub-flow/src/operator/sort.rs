use reifydb_core::{CommitVersion, Row, interface::FlowNodeId};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_rql::expression::Expression;
use reifydb_type::RowNumber;

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

impl Operator for SortOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded sort processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}

	fn get_rows(
		&self,
		txn: &mut StandardCommandTransaction,
		rows: &[RowNumber],
		version: CommitVersion,
	) -> crate::Result<Vec<Option<Row>>> {
		unimplemented!()
	}
}
