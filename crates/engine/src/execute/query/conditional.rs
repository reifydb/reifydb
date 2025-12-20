// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::value::column::{Columns, headers::ColumnHeaders};
use reifydb_rql::{
	expression::Expression,
	plan::physical::{self, PhysicalPlan},
};

use crate::{
	StandardTransaction,
	evaluate::column::{ColumnEvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, QueryNode, query::compile::compile},
};

pub(crate) struct ConditionalNode<'a> {
	condition: Expression<'a>,
	then_branch_plan: PhysicalPlan<'a>,
	else_ifs: Vec<ElseIfBranch<'a>>,
	else_branch_plan: Option<PhysicalPlan<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
	executed: bool,
}

pub(crate) struct ElseIfBranch<'a> {
	condition: Expression<'a>,
	then_branch_plan: PhysicalPlan<'a>,
}

impl<'a> ConditionalNode<'a> {
	pub fn new(physical_node: physical::ConditionalNode<'a>) -> Self {
		// Store the physical plans for lazy compilation
		let mut else_ifs = Vec::new();
		for physical_else_if in physical_node.else_ifs {
			else_ifs.push(ElseIfBranch {
				condition: physical_else_if.condition,
				then_branch_plan: *physical_else_if.then_branch,
			});
		}

		Self {
			condition: physical_node.condition,
			then_branch_plan: *physical_node.then_branch,
			else_ifs,
			else_branch_plan: physical_node.else_branch.map(|plan| *plan),
			context: None,
			executed: false,
		}
	}

	fn evaluate_condition(&self, condition: &Expression<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<bool> {
		// Create evaluation context for the condition
		let evaluation_context = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1, // Single value evaluation
			take: None,
			params: &ctx.params,
			stack: &ctx.stack,
			is_aggregate_context: false,
		};

		// Evaluate the condition expression
		let result_column = evaluate(&evaluation_context, condition)?;

		// Extract the boolean value from the result
		if let Some(first_value) = result_column.data().iter().next() {
			use reifydb_type::Value;
			match first_value {
				Value::Boolean(true) => Ok(true),
				Value::Boolean(false) => Ok(false),
				Value::Undefined => Ok(false),
				// For numeric values, treat zero as false, non-zero as true
				Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => {
					Ok(false)
				}
				Value::Uint1(0)
				| Value::Uint2(0)
				| Value::Uint4(0)
				| Value::Uint8(0)
				| Value::Uint16(0) => Ok(false),
				Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => {
					Ok(true)
				}
				Value::Uint1(_)
				| Value::Uint2(_)
				| Value::Uint4(_)
				| Value::Uint8(_)
				| Value::Uint16(_) => Ok(true),
				// For strings, treat empty as false, non-empty as true
				Value::Utf8(s) => Ok(!s.is_empty()),
				// For other values, treat as truthy (we'll handle floats separately if needed)
				_ => Ok(true),
			}
		} else {
			// Empty result is false
			Ok(false)
		}
	}
}

impl<'a> QueryNode<'a> for ConditionalNode<'a> {
	fn initialize(&mut self, _rx: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "ConditionalNode::next() called before initialize()");

		// Conditional statements execute once
		if self.executed {
			return Ok(None);
		}

		let stored_ctx = self.context.as_ref().unwrap();

		// Evaluate the main condition
		if self.evaluate_condition(&self.condition, stored_ctx)? {
			// Compile and execute then branch
			self.executed = true;
			let mut then_node = compile(self.then_branch_plan.clone(), rx, stored_ctx.clone());
			then_node.initialize(rx, stored_ctx)?;
			return then_node.next(rx, ctx);
		}

		// Check else if conditions
		for else_if in &self.else_ifs {
			if self.evaluate_condition(&else_if.condition, stored_ctx)? {
				// Compile and execute this else if branch
				self.executed = true;
				let mut else_if_node =
					compile(else_if.then_branch_plan.clone(), rx, stored_ctx.clone());
				else_if_node.initialize(rx, stored_ctx)?;
				return else_if_node.next(rx, ctx);
			}
		}

		// Execute else branch if present
		if let Some(else_branch_plan) = &self.else_branch_plan {
			self.executed = true;
			let mut else_node = compile(else_branch_plan.clone(), rx, stored_ctx.clone());
			else_node.initialize(rx, stored_ctx)?;
			return else_node.next(rx, ctx);
		}

		// No conditions matched and no else branch
		self.executed = true;
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		// Conditionals don't produce meaningful column headers
		// The actual headers depend on which branch is executed
		None
	}
}
