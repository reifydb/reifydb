// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, ColumnData, Columns, headers::ColumnHeaders};
use reifydb_rql::expression::VariableExpression;
use reifydb_type::{Fragment, diagnostic::runtime::variable_not_found, return_error};

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
	stack::Variable,
};

pub(crate) struct VariableNode {
	variable_expr: VariableExpression,
	context: Option<Arc<ExecutionContext>>,
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
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "VariableNode::next() called before initialize()");

		// Variables execute once and return their data
		if self.executed {
			return Ok(None);
		}

		let variable_name = self.variable_expr.name();

		// Look up the variable in the stack
		match ctx.stack.get(variable_name) {
			Some(Variable::Scalar(value)) => {
				// Convert scalar to single-column, single-row dataframe
				let value_type = value.get_type();
				let mut data = ColumnData::with_capacity(value_type, 1);
				data.push_value(value.clone());

				let column = Column {
					name: Fragment::internal(variable_name),
					data,
				};

				let columns = Columns::new(vec![column]);

				self.executed = true;

				Ok(Some(Batch {
					columns,
				}))
			}
			Some(Variable::Frame(frame_columns)) => {
				// Return the frame directly
				self.executed = true;

				Ok(Some(Batch {
					columns: frame_columns.clone(),
				}))
			}
			None => {
				// Variable not found - return error
				return_error!(variable_not_found(variable_name));
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		// Variable headers depend on the variable type, can't determine ahead of time
		None
	}
}
