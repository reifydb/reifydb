// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{FlowNodeId, Transaction, expression::Expression};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::{Operator, flow::FlowChange};

pub struct ExtendOperator {
	node: FlowNodeId,
	expressions: Vec<Expression<'static>>,
}

impl ExtendOperator {
	pub fn new(node: FlowNodeId, expressions: Vec<Expression<'static>>) -> Self {
		Self {
			node,
			expressions,
		}
	}
}

impl<T: Transaction> Operator<T> for ExtendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded extend processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}
}
