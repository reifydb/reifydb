// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_rql::expression::VariableExpression;
use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	error::EngineError,
	vm::{
		stack::Variable,
		volcano::query::{QueryContext, QueryNode},
	},
};

pub(crate) struct VariableNode {
	variable_expr: VariableExpression,
	context: Option<Arc<QueryContext>>,
	executed: bool,
}

impl VariableNode {
	pub fn new(variable_expr: VariableExpression) -> Self {
		Self {
			variable_expr,
			context: None,
			executed: false,
		}
	}
}

impl QueryNode for VariableNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "VariableNode::next() called before initialize()");

		if self.executed {
			return Ok(None);
		}

		let variable_name = self.variable_expr.name();

		match ctx.symbols.get(variable_name) {
			Some(Variable::Columns {
				columns,
			}) => {
				self.executed = true;
				Ok(Some(columns.clone()))
			}
			Some(Variable::ForIterator {
				columns,
				..
			}) => {
				self.executed = true;

				Ok(Some(columns.clone()))
			}
			Some(Variable::Closure(_)) => Err(EngineError::VariableNotFound {
				name: variable_name.to_string(),
			}
			.into()),
			None => Err(EngineError::VariableNotFound {
				name: variable_name.to_string(),
			}
			.into()),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
