// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_rql::expression::VariableExpression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
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
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "VariableNode::next() called before initialize()");

		// Variables execute once and return their data
		if self.executed {
			return Ok(None);
		}

		let variable_name = self.variable_expr.name();

		// Look up the variable in the stack
		match ctx.stack.get(variable_name) {
			Some(Variable::Scalar(columns)) => {
				let mut columns = columns.clone();
				columns[0].name = Fragment::internal(variable_name);
				self.executed = true;
				Ok(Some(columns))
			}
			Some(Variable::Columns(frame_columns)) => {
				// Return the frame directly
				self.executed = true;

				Ok(Some(frame_columns.clone()))
			}
			Some(Variable::ForIterator {
				columns,
				..
			}) => {
				// Return the iterator's columns
				self.executed = true;

				Ok(Some(columns.clone()))
			}
			Some(Variable::Closure(_)) => {
				// Closures cannot be used as data sources in queries
				return Err(EngineError::VariableNotFound {
					name: variable_name.to_string(),
				}
				.into());
			}
			None => {
				// Variable not found - return error
				return Err(EngineError::VariableNotFound {
					name: variable_name.to_string(),
				}
				.into());
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		// Variable headers depend on the variable type, can't determine ahead of time
		None
	}
}
