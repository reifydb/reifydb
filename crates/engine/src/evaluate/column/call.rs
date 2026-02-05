// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData, view::group_by::GroupByView};
use reifydb_function::{AggregateFunction, AggregateFunctionContext, ScalarFunctionContext};
use reifydb_rql::{
	expression::{CallExpression, Expression},
	plan::physical::{DefineFunctionNode, PhysicalPlan},
};
use reifydb_type::{error, error::diagnostic::function, fragment::Fragment, params::Params, value::Value};

use super::StandardColumnEvaluator;
use crate::{
	evaluate::ColumnEvaluationContext,
	vm::{
		instruction::{Instruction, compile::compile as compile_instructions},
		stack::{ScopeType, SymbolTable, Variable},
	},
};

/// Strip the leading `$` from a variable name if present
fn strip_dollar_prefix(name: &str) -> String {
	if name.starts_with('$') {
		name[1..].to_string()
	} else {
		name.to_string()
	}
}

/// Convert a slice of Values into ColumnData
fn column_data_from_values(values: &[Value]) -> ColumnData {
	if values.is_empty() {
		return ColumnData::undefined(0);
	}

	let mut data = ColumnData::undefined(0);
	for value in values {
		data.push_value(value.clone());
	}
	data
}

impl StandardColumnEvaluator {
	pub(crate) fn call<'a>(&self, ctx: &ColumnEvaluationContext, call: &CallExpression) -> crate::Result<Column> {
		let function_name = call.func.0.text();

		// Check if we're in aggregation context and if function exists as aggregate
		// FIXME this is a quick hack - this should be derived from a call stack
		if ctx.is_aggregate_context {
			if let Some(aggregate_fn) = self.functions.get_aggregate(function_name) {
				return self.handle_aggregate_function(ctx, call, aggregate_fn);
			}
		}

		// Evaluate arguments first (needed for both user-defined and built-in functions)
		let arguments = self.evaluate_arguments(ctx, &call.args)?;

		// Try user-defined function from symbol table first
		if let Some(func_def) = ctx.symbol_table.get_function(function_name) {
			return self.call_user_defined_function(ctx, call, func_def.clone(), &arguments);
		}

		// Fall back to built-in scalar function handling
		let functor = self
			.functions
			.get_scalar(function_name)
			.ok_or(error!(function::unknown_function(call.func.0.clone())))?;

		let row_count = ctx.row_count;
		Ok(Column {
			name: call.full_fragment_owned(),
			data: functor.scalar(ScalarFunctionContext {
				fragment: call.func.0.clone(),
				columns: &arguments,
				row_count,
			})?,
		})
	}

	/// Execute a user-defined function for each row, returning a column of results
	fn call_user_defined_function(
		&self,
		ctx: &ColumnEvaluationContext,
		call: &CallExpression,
		func_def: DefineFunctionNode,
		arguments: &Columns,
	) -> crate::Result<Column> {
		let row_count = ctx.row_count;
		let mut results: Vec<Value> = Vec::with_capacity(row_count);

		// Compile function body once
		let body_instructions = compile_instructions(func_def.body.clone())?;

		// For each row, execute the function
		for row_idx in 0..row_count {
			// Clone symbol table for this execution
			let mut func_symbol_table = ctx.symbol_table.clone();
			func_symbol_table.enter_scope(ScopeType::Function);

			// Bind arguments to parameters
			for (param, arg_col) in func_def.parameters.iter().zip(arguments.iter()) {
				let param_name = strip_dollar_prefix(param.name.text());
				let value = arg_col.data().get_value(row_idx);
				func_symbol_table.set(param_name, Variable::Scalar(value), true)?;
			}

			// Execute function body instructions and get result
			let result = self.execute_function_body_for_scalar(
				&body_instructions,
				&mut func_symbol_table,
				ctx.params,
			)?;

			results.push(result);
		}

		// Convert results to ColumnData
		let data = column_data_from_values(&results);
		Ok(Column {
			name: call.full_fragment_owned(),
			data,
		})
	}

	/// Execute function body instructions and return a scalar result
	fn execute_function_body_for_scalar(
		&self,
		instructions: &[Instruction],
		symbol_table: &mut SymbolTable,
		params: &Params,
	) -> crate::Result<Value> {
		let mut ip = 0;
		let mut last_value = Value::Undefined;

		while ip < instructions.len() {
			match &instructions[ip] {
				Instruction::Halt => break,
				Instruction::Nop => {}

				Instruction::Return(ret_node) => {
					if let Some(ref expr) = ret_node.value {
						let evaluation_context = ColumnEvaluationContext {
							target: None,
							columns: Columns::empty(),
							row_count: 1,
							take: None,
							params,
							symbol_table,
							is_aggregate_context: false,
						};
						let result_column = self.evaluate(&evaluation_context, expr)?;
						if result_column.data.len() > 0 {
							return Ok(result_column.data.get_value(0));
						}
					}
					return Ok(Value::Undefined);
				}

				Instruction::Query(plan) => match plan {
					PhysicalPlan::Map(map_node) => {
						if map_node.input.is_none() && !map_node.map.is_empty() {
							let evaluation_context = ColumnEvaluationContext {
								target: None,
								columns: Columns::empty(),
								row_count: 1,
								take: None,
								params,
								symbol_table,
								is_aggregate_context: false,
							};
							let result_column =
								self.evaluate(&evaluation_context, &map_node.map[0])?;
							if result_column.data.len() > 0 {
								last_value = result_column.data.get_value(0);
							}
						}
					}
					_ => {
						unreachable!("Other plan types would need full VM execution");
					}
				},

				Instruction::Emit => {
					// Emit is handled - the last computed value is what we return
				}

				Instruction::EvalCondition(expr) => {
					let evaluation_context = ColumnEvaluationContext {
						target: None,
						columns: Columns::empty(),
						row_count: 1,
						take: None,
						params,
						symbol_table,
						is_aggregate_context: false,
					};
					let result_column = self.evaluate(&evaluation_context, expr)?;
					if result_column.data.len() > 0 {
						last_value = result_column.data.get_value(0);
					}
				}

				Instruction::JumpIfFalsePop(addr) => {
					let is_false = match &last_value {
						Value::Boolean(false) => true,
						Value::Boolean(true) => false,
						_ => true,
					};
					if is_false {
						ip = *addr;
						continue;
					}
				}

				Instruction::Jump(addr) => {
					ip = *addr;
					continue;
				}

				Instruction::EnterScope(scope_type) => {
					symbol_table.enter_scope(scope_type.clone());
				}

				Instruction::ExitScope => {
					let _ = symbol_table.exit_scope();
				}

				_ => {
					// Handle other instructions as needed
				}
			}
			ip += 1;
		}

		Ok(last_value)
	}

	fn handle_aggregate_function<'a>(
		&self,
		ctx: &ColumnEvaluationContext,
		call: &CallExpression,
		mut aggregate_fn: Box<dyn AggregateFunction>,
	) -> crate::Result<Column> {
		// Create a single group containing all row indices for aggregation
		let mut group_view = GroupByView::new();
		let all_indices: Vec<usize> = (0..ctx.row_count).collect();
		group_view.insert(Vec::<Value>::new(), all_indices); // Empty group key for single group

		// Determine which column to aggregate over
		let column = if call.args.is_empty() {
			// For count() with no arguments, create a dummy column
			Column {
				name: Fragment::internal("dummy"),
				data: ColumnData::int4_with_capacity(ctx.row_count),
			}
		} else {
			// For functions with arguments like sum(amount), use the first argument column
			let arguments = self.evaluate_arguments(ctx, &call.args)?;
			arguments[0].clone()
		};

		// Call the aggregate function
		aggregate_fn.aggregate(AggregateFunctionContext {
			fragment: call.func.0.clone(),
			column: &column,
			groups: &group_view,
		})?;

		// Finalize and get results
		let (_keys, result_data) = aggregate_fn.finalize()?;

		Ok(Column {
			name: call.full_fragment_owned(),
			data: result_data,
		})
	}

	fn evaluate_arguments<'a>(
		&self,
		ctx: &ColumnEvaluationContext,
		expressions: &Vec<Expression>,
	) -> crate::Result<Columns> {
		let mut result: Vec<Column> = Vec::with_capacity(expressions.len());

		for expression in expressions {
			result.push(self.evaluate(ctx, expression)?)
		}

		Ok(Columns::new(result))
	}
}
