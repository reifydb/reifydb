// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	value::column::columns::Columns,
};
use reifydb_rql::expression::Expression;
use reifydb_value::Result;

use crate::{
	operator::{Operator, OperatorCell},
	transaction::FlowTransaction,
};

pub struct SortOperator {
	parent: OperatorCell,
	node: FlowNodeId,
	_expressions: Vec<Expression>,
}

impl SortOperator {
	pub fn new(parent: OperatorCell, node: FlowNodeId, _expressions: Vec<Expression>) -> Self {
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
	fn id(&self) -> FlowNodeId {
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
