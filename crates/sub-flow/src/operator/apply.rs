use reifydb_core::{CommitVersion, Row, interface::FlowNodeId};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::RowNumber;

use crate::{flow::FlowChange, operator::Operator};

pub struct ApplyOperator {
	node: FlowNodeId,
	inner: Box<dyn Operator>,
}

impl ApplyOperator {
	pub fn new(node: FlowNodeId, inner: Box<dyn Operator>) -> Self {
		Self {
			node,
			inner,
		}
	}
}

impl Operator for ApplyOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		self.inner.apply(txn, change, evaluator)
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
