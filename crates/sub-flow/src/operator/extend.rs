// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, Row, interface::FlowNodeId};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_rql::expression::Expression;
use reifydb_type::RowNumber;

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

impl Operator for ExtendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded extend processing
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
