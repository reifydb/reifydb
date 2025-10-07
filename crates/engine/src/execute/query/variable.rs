// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	interface::evaluate::expression::VariableExpression,
	stack::Variable,
	value::column::{Column, ColumnData, Columns, headers::ColumnHeaders},
};
use reifydb_type::{Fragment, diagnostic::runtime::variable_not_found, return_error};

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
};

pub(crate) struct VariableNode<'a> {
	variable_expr: VariableExpression<'a>,
	context: Option<Arc<ExecutionContext<'a>>>,
	executed: bool,
}

impl<'a> VariableNode<'a> {
	pub fn new(variable_expr: VariableExpression<'a>) -> Self {
		Self {
			variable_expr,
			context: None,
			executed: false,
		}
	}
}

impl<'a> QueryNode<'a> for VariableNode<'a> {
	fn initialize(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
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
					name: Fragment::owned_internal(variable_name),
					data,
				};

				let columns = Columns::new(vec![column]);

				self.executed = true;

				// Transmute the columns to extend their lifetime
				// SAFETY: The columns are created here and genuinely have lifetime 'a
				let columns = unsafe { std::mem::transmute::<Columns<'_>, Columns<'a>>(columns) };

				Ok(Some(Batch {
					columns,
				}))
			}
			Some(Variable::Frame(frame_columns)) => {
				// Return the frame directly
				self.executed = true;

				// Clone the columns and transmute to extend lifetime
				// SAFETY: The columns come from the stack which has lifetime 'a
				let columns = frame_columns.clone();
				let columns = unsafe { std::mem::transmute::<Columns<'static>, Columns<'a>>(columns) };

				Ok(Some(Batch {
					columns,
				}))
			}
			None => {
				// Variable not found - return error
				return_error!(variable_not_found(variable_name));
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		// Variable headers depend on the variable type, can't determine ahead of time
		None
	}
}
