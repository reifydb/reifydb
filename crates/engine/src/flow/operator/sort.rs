// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::flow::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change},
	value::column::columns::Columns,
};
use reifydb_rql::expression::Expression;
use reifydb_value::Result;

use crate::flow::{
	operator::{Operator, OperatorCell},
	transaction::FlowTransaction,
};

pub struct SortOperator {
	parent: OperatorCell,
	node: OperatorId,
	_expressions: Vec<Expression>,
}

impl SortOperator {
	pub fn new(parent: OperatorCell, node: OperatorId, _expressions: Vec<Expression>) -> Self {
		Self {
			parent,
			node,
			_expressions,
		}
	}
}

impl SortOperator {
	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}
}

impl Operator for SortOperator {
	fn id(&self) -> OperatorId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		// TODO: Implement single-encoded sort processing

		Ok(Change::from_flow(self.node, change.version, change.diffs, change.changed_at))
	}
}
