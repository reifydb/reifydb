// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	value::column::columns::Columns,
};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_rql::expression::Expression;
use reifydb_type::value::row_number::RowNumber;

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

pub struct SortOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	_expressions: Vec<Expression>,
}

impl SortOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, _expressions: Vec<Expression>) -> Self {
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
		change: Change,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<Change> {
		// TODO: Implement single-encoded sort processing
		// For now, just pass through all changes with updated from
		Ok(Change::from_flow(self.node, change.version, change.diffs))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
