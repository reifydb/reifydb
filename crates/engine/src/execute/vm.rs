// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::plan::physical::{ForPhysicalNode, LoopPhysicalNode, PhysicalPlan, WhilePhysicalNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{params::Params, value::frame::frame::Frame};

use crate::{
	execute::Executor,
	stack::{ControlFlow, ScopeType, Stack, Variable},
};

const MAX_ITERATIONS: usize = 10_000;

impl Executor {
	/// Execute the body of a loop construct.
	/// Returns the control flow signal after executing all plans in the body.
	pub(crate) fn vm_execute_body(
		&self,
		rx: &mut Transaction<'_>,
		body: &[PhysicalPlan],
		params: Params,
		stack: &mut Stack,
		result: &mut Vec<Frame>,
	) -> crate::Result<ControlFlow> {
		for plan in body {
			match plan {
				PhysicalPlan::Break => return Ok(ControlFlow::Break),
				PhysicalPlan::Continue => return Ok(ControlFlow::Continue),
				PhysicalPlan::Loop(node) => {
					self.vm_loop(rx, node, params.clone(), stack, result)?;
				}
				PhysicalPlan::While(node) => {
					self.vm_while(rx, node, params.clone(), stack, result)?;
				}
				PhysicalPlan::For(node) => {
					self.vm_for(rx, node, params.clone(), stack, result)?;
				}
				PhysicalPlan::Declare(_) | PhysicalPlan::Assign(_) | PhysicalPlan::Conditional(_) => {
					// Execute but suppress output for declarations, assignments, and conditionals.
					// Inside loop bodies these are control-flow / side-effect statements,
					// not data-producing operations.
					self.query(rx, plan.clone(), params.clone(), stack)?;
					// Check for control flow signals from nested IF branches
					if stack.control_flow != ControlFlow::Normal {
						let cf = stack.control_flow.clone();
						stack.control_flow = ControlFlow::Normal;
						return Ok(cf);
					}
				}
				other => {
					if let Some(cols) = self.query(rx, other.clone(), params.clone(), stack)? {
						result.push(Frame::from(cols));
					}
					// Check for control flow signals from nested IF branches
					if stack.control_flow != ControlFlow::Normal {
						let cf = stack.control_flow.clone();
						stack.control_flow = ControlFlow::Normal;
						return Ok(cf);
					}
				}
			}
		}
		Ok(ControlFlow::Normal)
	}

	/// Execute a LOOP { ... } block
	pub(crate) fn vm_loop(
		&self,
		rx: &mut Transaction<'_>,
		node: &LoopPhysicalNode,
		params: Params,
		stack: &mut Stack,
		result: &mut Vec<Frame>,
	) -> crate::Result<()> {
		for _iteration in 0..MAX_ITERATIONS {
			stack.enter_scope(ScopeType::Loop);
			let cf = self.vm_execute_body(rx, &node.body, params.clone(), stack, result)?;
			stack.exit_scope()?;

			match cf {
				ControlFlow::Break => return Ok(()),
				ControlFlow::Continue => continue,
				ControlFlow::Normal => {}
			}
		}

		Err(reifydb_type::error::Error(reifydb_type::error::diagnostic::runtime::max_iterations_exceeded(
			MAX_ITERATIONS,
		)))
	}

	/// Execute a WHILE condition { ... } block
	pub(crate) fn vm_while(
		&self,
		rx: &mut Transaction<'_>,
		node: &WhilePhysicalNode,
		params: Params,
		stack: &mut Stack,
		result: &mut Vec<Frame>,
	) -> crate::Result<()> {
		for _iteration in 0..MAX_ITERATIONS {
			// Evaluate condition
			if !self.evaluate_loop_condition(&node.condition, &params, stack)? {
				return Ok(());
			}

			stack.enter_scope(ScopeType::Loop);
			let cf = self.vm_execute_body(rx, &node.body, params.clone(), stack, result)?;
			stack.exit_scope()?;

			match cf {
				ControlFlow::Break => return Ok(()),
				ControlFlow::Continue => continue,
				ControlFlow::Normal => {}
			}
		}

		Err(reifydb_type::error::Error(reifydb_type::error::diagnostic::runtime::max_iterations_exceeded(
			MAX_ITERATIONS,
		)))
	}

	/// Execute a FOR $var IN expr { ... } block
	pub(crate) fn vm_for(
		&self,
		rx: &mut Transaction<'_>,
		node: &ForPhysicalNode,
		params: Params,
		stack: &mut Stack,
		result: &mut Vec<Frame>,
	) -> crate::Result<()> {
		// Execute the iterable to get a frame
		let iterable_cols = self.query(rx, *node.iterable.clone(), params.clone(), stack)?;

		let Some(columns) = iterable_cols else {
			return Ok(());
		};

		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(());
		}

		let var_name = node.variable_name.text();
		let clean_name = if var_name.starts_with('$') {
			&var_name[1..]
		} else {
			var_name
		};

		// Iterate over each row
		for row_idx in 0..row_count {
			if row_idx >= MAX_ITERATIONS {
				return Err(reifydb_type::error::Error(
					reifydb_type::error::diagnostic::runtime::max_iterations_exceeded(
						MAX_ITERATIONS,
					),
				));
			}

			stack.enter_scope(ScopeType::Loop);

			// If single column, bind the scalar value; otherwise bind a single-row frame
			if columns.len() == 1 {
				let value = columns.columns[0].data.get_value(row_idx);
				stack.set(clean_name.to_string(), Variable::Scalar(value), true)?;
			} else {
				// Create a single-row frame from this row
				let mut row_columns = Vec::new();
				for col in columns.columns.iter() {
					let value = col.data.get_value(row_idx);
					let mut data = reifydb_core::value::column::data::ColumnData::undefined(0);
					data.push_value(value);
					row_columns
						.push(reifydb_core::value::column::Column::new(col.name.clone(), data));
				}
				let row_frame = Columns::new(row_columns);
				stack.set(clean_name.to_string(), Variable::Frame(row_frame), true)?;
			}

			let cf = self.vm_execute_body(rx, &node.body, params.clone(), stack, result)?;
			stack.exit_scope()?;

			match cf {
				ControlFlow::Break => return Ok(()),
				ControlFlow::Continue => continue,
				ControlFlow::Normal => {}
			}
		}

		Ok(())
	}

	/// Evaluate a boolean condition for WHILE loops.
	/// Reuses the same evaluation logic as ConditionalNode.
	fn evaluate_loop_condition(
		&self,
		condition: &reifydb_rql::expression::Expression,
		params: &Params,
		stack: &Stack,
	) -> crate::Result<bool> {
		use crate::evaluate::{ColumnEvaluationContext, column::evaluate};

		let evaluation_context = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params,
			stack,
			is_aggregate_context: false,
		};

		let result_column = evaluate(&evaluation_context, condition, &self.functions)?;

		if let Some(first_value) = result_column.data().iter().next() {
			use reifydb_type::value::Value;
			match first_value {
				Value::Boolean(true) => Ok(true),
				Value::Boolean(false) => Ok(false),
				Value::Undefined => Ok(false),
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
				Value::Utf8(s) => Ok(!s.is_empty()),
				_ => Ok(true),
			}
		} else {
			Ok(false)
		}
	}
}
