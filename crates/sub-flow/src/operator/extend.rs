// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_rql::expression::Expression;
use reifydb_sdk::FlowChange;
use reifydb_type::RowNumber;

use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct ExtendOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	#[allow(dead_code)]
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
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-encoded extend processing
		// For now, just pass through all changes with updated from
		Ok(FlowChange::internal(self.node, change.version, change.diffs))
	}

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		self.parent.pull(txn, rows).await
	}
}
