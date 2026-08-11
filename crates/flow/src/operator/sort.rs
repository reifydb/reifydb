// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change},
	value::column::columns::Columns,
};
use reifydb_rql::expression::Expression;
use reifydb_value::Result;

use crate::{
	operator::{Operator, OperatorCell},
	transaction::DepFlowTransaction,
};

pub struct SortOperator {
	parent: OperatorCell,
	operator: OperatorId,
	_expressions: Vec<Expression>,
}

impl SortOperator {
	pub fn new(parent: OperatorCell, operator: OperatorId, _expressions: Vec<Expression>) -> Self {
		Self {
			parent,
			operator,
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
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, _txn: &mut DepFlowTransaction, change: Change) -> Result<Change> {
		// TODO: Implement single-encoded sort processing

		Ok(Change::from_flow(self.operator, change.version, change.diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}
