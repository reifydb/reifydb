// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	interface::evaluate::expression::Expression,
	value::column::{Columns, headers::ColumnHeaders},
};
use reifydb_rql::plan::physical;
use reifydb_type::Params;

use crate::{
	StandardTransaction,
	evaluate::column::{ColumnEvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, QueryNode},
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
		Self {
			name: physical_node.name.text().to_string(),
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

	fn next(&mut self, rx: &mut StandardTransaction<'a>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "LetNode::next() called before initialize()");

		// Let statements execute once and return no data
		if self.executed {
			return Ok(None);
		}

		let ctx = self.context.as_ref().unwrap();

		// Evaluate the expression to get the value
		let evaluation_context = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1, // Single value evaluation
			take: None,
			params: unsafe { std::mem::transmute::<&Params, &'a Params>(&ctx.params) },
		};

		let result_column = evaluate(&evaluation_context, &self.value)?;
		let result_columns = Columns::new(vec![result_column]);

		// Store the variable in the stack
		// Note: We need to get a mutable reference to the execution context's stack
		// This is a limitation of the current design - we need to modify this
		// For now, we'll return an error indicating this needs to be handled at a higher level

		self.executed = true;

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
