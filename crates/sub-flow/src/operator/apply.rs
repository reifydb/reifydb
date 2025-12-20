use std::sync::Arc;

use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_engine::StandardRowEvaluator;
use reifydb_flow_operator_sdk::FlowChange;
use reifydb_type::RowNumber;

use crate::{
	operator::{BoxedOperator, Operator, Operators},
	transaction::FlowTransaction,
};

pub struct ApplyOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	inner: BoxedOperator,
}

impl ApplyOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, inner: BoxedOperator) -> Self {
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
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		self.inner.apply(txn, change, evaluator)
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		self.parent.get_rows(txn, rows)
	}
}
