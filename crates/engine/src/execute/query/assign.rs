// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::value::column::{Columns, headers::ColumnHeaders};
use reifydb_rql::plan::{physical, physical::AssignValue};

use crate::{
	StandardTransaction,
	evaluate::column::{ColumnEvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, QueryNode, query::compile::compile},
	stack::Variable,
};

pub(crate) struct AssignNode {
	name: String,
	value: AssignValue,
	context: Option<Arc<ExecutionContext>>,
	executed: bool,
}

impl AssignNode {
	pub fn new(physical_node: physical::AssignNode) -> Self {
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
			context: None,
			executed: false,
		}
	}
}

#[async_trait]
impl QueryNode for AssignNode {
	async fn initialize<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "AssignNode::next() called before initialize()");

		// Assignment statements execute once and return no data
		if self.executed {
			return Ok(None);
		}

		let stored_ctx = self.context.as_ref().unwrap();

		// Handle both expression and statement values
		let result_columns = match &self.value {
			AssignValue::Expression(expr) => {
				// Evaluate the expression to get the value
				let evaluation_context = ColumnEvaluationContext {
					target: None,
					columns: Columns::empty(),
					row_count: 1, // Single value evaluation
					take: None,
					params: &stored_ctx.params,
					stack: &stored_ctx.stack,
					is_aggregate_context: false,
				};

				let result_column = evaluate(&evaluation_context, expr)?;
				Columns::new(vec![result_column])
			}
			AssignValue::Statement(physical_plans) => {
				// Execute the pipeline of physical plans
				self.execute_statement_pipeline(rx, ctx, physical_plans).await?
			}
		};

		// Determine if this should be stored as a Scalar or Frame variable
		let variable = if result_columns.len() == 1 && result_columns.row_count() == 1 {
			// Single column, single row -> check if we should store as scalar
			if let Some(first_column) = result_columns.iter().next() {
				if let Some(first_value) = first_column.data().iter().next() {
					Variable::scalar(first_value)
				} else {
					// Empty column -> store as frame
					Variable::frame(result_columns.clone())
				}
			} else {
				// No columns -> store as frame
				Variable::frame(result_columns.clone())
			}
		} else {
			// Multiple columns or rows -> store as frame
			Variable::frame(result_columns.clone())
		};

		// Reassign the variable using the new reassign method
		ctx.stack.reassign(self.name.clone(), variable)?;

		self.executed = true;

		// Return the result as a single batch for debugging/inspection
		Ok(Some(Batch {
			columns: result_columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		// Assignment statements don't produce meaningful column headers
		None
	}
}

impl<'a> AssignNode {
	/// Execute a pipeline of physical plans and return the final result
	async fn execute_statement_pipeline(
		&self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
		physical_plans: &[physical::PhysicalPlan],
	) -> crate::Result<Columns> {
		if physical_plans.is_empty() {
			return Ok(Columns::empty());
		}

		// For a pipeline, we need to execute each plan in sequence
		// The last plan in the pipeline produces the final result
		let last_plan = physical_plans.last().unwrap();

		// For now, execute just the last plan as a simple implementation
		// TODO: Implement proper pipeline chaining for complex cases
		let execution_context = Arc::new(ctx.clone());
		let mut node = compile(last_plan.clone(), rx, execution_context.clone()).await;

		// Initialize the operator before execution
		node.initialize(rx, &execution_context).await?;

		let mut result: Option<Columns> = None;
		let mut mutable_context = (*execution_context).clone();

		while let Some(crate::execute::Batch {
			columns,
		}) = node.next(rx, &mut mutable_context).await?
		{
			if let Some(mut result_columns) = result.take() {
				result_columns.append_columns(columns)?;
				result = Some(result_columns);
			} else {
				result = Some(columns);
			}
		}

		Ok(result.unwrap_or_else(Columns::empty))
	}
}
