use std::sync::Arc;

use reifydb_core::{CommitVersion, Row, interface::FlowNodeId};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::RowNumber;

use crate::{
	flow::FlowChange,
	operator::{Operator, Operators},
};

pub struct ApplyOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	inner: Box<dyn Operator>,
}

impl ApplyOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, inner: Box<dyn Operator>) -> Self {
		Self {
			parent,
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
		self.parent.get_rows(txn, rows, version)
	}
}
