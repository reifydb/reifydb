// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::value::column::{Columns, headers::ColumnHeaders};
use reifydb_rql::{expression::Expression, plan::physical};

use crate::{
	StandardTransaction,
	evaluate::column::{ColumnEvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, QueryNode},
	stack::Variable,
};

pub(crate) struct LetNode<'a> {
	name: String,
	value: Expression<'a>,
	mutable: bool,
	context: Option<Arc<ExecutionContext<'a>>>,
	executed: bool,
}

impl<'a> LetNode<'a> {
	pub fn new(physical_node: physical::LetNode<'a>) -> Self {
		let name_text = physical_node.name.text();
		// Strip the '$' prefix if present
		let clean_name = if name_text.starts_with('$') {
			name_text[1..].to_string()
		} else {
			name_text.to_string()
		};

		Self {
			name: clean_name,
			value: physical_node.value,
			mutable: physical_node.mutable,
			context: None,
			executed: false,
		}
	}
}

impl<'a> QueryNode<'a> for LetNode<'a> {
	fn initialize(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "LetNode::next() called before initialize()");

		// Let statements execute once and return no data
		if self.executed {
			return Ok(None);
		}

		let stored_ctx = self.context.as_ref().unwrap();

		// Evaluate the expression to get the value
		let evaluation_context = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1, // Single value evaluation
			take: None,
			params: &stored_ctx.params,
			stack: &stored_ctx.stack,
		};

		let result_column = evaluate(&evaluation_context, &self.value)?;
		let result_columns = Columns::new(vec![result_column]);

		// Determine if this should be stored as a Scalar or Frame variable
		let variable = if result_columns.len() == 1 && result_columns.row_count() == 1 {
			// Single column, single row -> check if we should store as scalar
			if let Some(first_column) = result_columns.iter().next() {
				if let Some(first_value) = first_column.data().iter().next() {
					Variable::scalar(first_value)
				} else {
					// Empty column -> store as frame
					Variable::frame(unsafe {
						std::mem::transmute::<Columns<'_>, Columns<'static>>(
							result_columns.clone(),
						)
					})
				}
			} else {
				// No columns -> store as frame
				Variable::frame(unsafe {
					std::mem::transmute::<Columns<'_>, Columns<'static>>(result_columns.clone())
				})
			}
		} else {
			// Multiple columns or rows -> store as frame
			Variable::frame(unsafe {
				std::mem::transmute::<Columns<'_>, Columns<'static>>(result_columns.clone())
			})
		};

		// Store the variable in the stack with mutable access
		ctx.stack.set(self.name.clone(), variable, self.mutable)?;

		self.executed = true;

		// Transmute the columns to extend their lifetime
		// SAFETY: The columns come from evaluate() which returns Column<'a>
		// so they genuinely have lifetime 'a through the query execution
		let result_columns = unsafe { std::mem::transmute::<Columns<'_>, Columns<'a>>(result_columns) };

		// Return the result as a single batch for debugging/inspection
		Ok(Some(Batch {
			columns: result_columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		// Let statements don't produce meaningful column headers
		None
	}
}
