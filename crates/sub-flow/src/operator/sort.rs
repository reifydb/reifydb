use std::sync::Arc;

use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_engine::StandardRowEvaluator;
use reifydb_flow_operator_sdk::FlowChange;
use reifydb_rql::expression::Expression;
use reifydb_type::RowNumber;

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

pub struct SortOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	_expressions: Vec<Expression<'static>>,
}

impl SortOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, _expressions: Vec<Expression<'static>>) -> Self {
		Self {
			parent,
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
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded sort processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		self.parent.get_rows(txn, rows)
	}
}
