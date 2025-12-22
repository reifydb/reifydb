// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_engine::StandardRowEvaluator;
use reifydb_flow_operator_sdk::FlowChange;
use reifydb_rql::expression::Expression;
use reifydb_type::RowNumber;

use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct ExtendOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	expressions: Vec<Expression>,
}

impl ExtendOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, expressions: Vec<Expression>) -> Self {
		Self {
			parent,
			node,
			expressions,
		}
	}
}

#[async_trait]
impl Operator for ExtendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded extend processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		self.parent.get_rows(txn, rows)
	}
}
