use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_engine::StandardRowEvaluator;
use reifydb_type::RowNumber;

use crate::{flow::FlowChange, operator::Operator, transaction::FlowTransaction};

pub struct UnionOperator {
	node: FlowNodeId,
}

impl UnionOperator {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}
}

impl Operator for UnionOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded union processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		unimplemented!()
	}
}
