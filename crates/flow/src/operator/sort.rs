// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	value::column::columns::Columns,
};
use reifydb_rql::expression::Expression;
use reifydb_value::Result;

use crate::{operator::Operator, transaction::FlowTransaction};

pub struct SortOperator {
	parent_schema: Option<Columns>,
	operator: OperatorId,
	_expressions: Vec<Expression>,
}

impl SortOperator {
	pub fn new(parent_schema: Option<Columns>, operator: OperatorId, _expressions: Vec<Expression>) -> Self {
		Self {
			parent_schema,
			operator,
			_expressions,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent_schema.clone()
	}
}

impl<T: FlowTransaction> Operator<T> for SortOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, _txn: &mut T, change: Change) -> Result<Change> {
		// TODO: Implement single-encoded sort processing

		Ok(Change::from_flow(self.operator, change.version, change.diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}
