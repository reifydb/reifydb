// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData, view::group_by::GroupByView};
use reifydb_function::{AggregateFunction, AggregateFunctionContext, ScalarFunctionContext};
use reifydb_rql::{
	expression::{CallExpression, Expression},
	instruction::{CompiledFunctionDef, Instruction, ScopeType},
	query::QueryPlan,
};
use reifydb_type::{error, error::diagnostic::function, fragment::Fragment, params::Params, value::Value};

use super::StandardColumnEvaluator;
use crate::{
	evaluate::ColumnEvaluationContext,
	vm::{
		scalar,
		stack::{SymbolTable, Variable},
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
				clock: &self.clock,
			})?,
		})
	}

	/// Execute a user-defined function for each row, returning a column of results
	fn call_user_defined_function(
		&self,
		ctx: &ColumnEvaluationContext,
		call: &CallExpression,
		func_def: CompiledFunctionDef,
		arguments: &Columns,
	) -> crate::Result<Column> {
		let row_count = ctx.row_count;
		let mut results: Vec<Value> = Vec::with_capacity(row_count);

		// Function body is already pre-compiled
		let body_instructions = &func_def.body;

		let mut func_symbol_table = ctx.symbol_table.clone();

		// For each row, execute the function
		for row_idx in 0..row_count {
			let base_depth = func_symbol_table.scope_depth();
			func_symbol_table.enter_scope(ScopeType::Function);

			// Bind arguments to parameters
			for (param, arg_col) in func_def.parameters.iter().zip(arguments.iter()) {
				let param_name = strip_dollar_prefix(param.name.text());
				let value = arg_col.data().get_value(row_idx);
				func_symbol_table.set(param_name, Variable::scalar(value), true)?;
			}

			// Execute function body instructions and get result
			let result = self.execute_function_body_for_scalar(
				&body_instructions,
				&mut func_symbol_table,
				ctx.params,
			)?;

			while func_symbol_table.scope_depth() > base_depth {
				let _ = func_symbol_table.exit_scope();
			}

			results.push(result);
		}

		// Convert results to ColumnData
		let data = column_data_from_values(&results);
		Ok(Column {
			name: call.full_fragment_owned(),
			data,
		})
	}

	/// Execute function body instructions and return a scalar result.
	/// Uses a simple stack-based interpreter matching the new bytecode ISA.
	fn execute_function_body_for_scalar(
		&self,
		instructions: &[Instruction],
		symbol_table: &mut SymbolTable,
		params: &Params,
	) -> crate::Result<Value> {
		let mut ip = 0;
		let mut stack: Vec<Value> = Vec::new();

		while ip < instructions.len() {
			match &instructions[ip] {
				Instruction::Halt => break,
				Instruction::Nop => {}

				// === Stack ===
				Instruction::PushConst(v) => stack.push(v.clone()),
				Instruction::PushUndefined => stack.push(Value::Undefined),
				Instruction::Pop => {
					stack.pop();
				}
				Instruction::Dup => {
					if let Some(v) = stack.last() {
						stack.push(v.clone());
					}
				}

				// === Variables ===
				Instruction::LoadVar(name) => {
					let var_name = strip_dollar_prefix(name.text());
					let val = symbol_table
						.get(&var_name)
						.map(|v| match v {
							Variable::Scalar(c) => c.scalar_value(),
							_ => Value::Undefined,
						})
						.unwrap_or(Value::Undefined);
					stack.push(val);
				}
				Instruction::StoreVar(name) => {
					let val = stack.pop().unwrap_or(Value::Undefined);
					let var_name = strip_dollar_prefix(name.text());
					symbol_table.set(var_name, Variable::scalar(val), true)?;
				}
				Instruction::DeclareVar(name) => {
					let val = stack.pop().unwrap_or(Value::Undefined);
					let var_name = strip_dollar_prefix(name.text());
					symbol_table.set(var_name, Variable::scalar(val), true)?;
				}

				// === Arithmetic ===
				Instruction::Add => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_add(l, r)?);
				}
				Instruction::Sub => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_sub(l, r)?);
				}
				Instruction::Mul => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_mul(l, r)?);
				}
				Instruction::Div => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_div(l, r)?);
				}
				Instruction::Rem => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_rem(l, r)?);
				}

				// === Unary ===
				Instruction::Negate => {
					let v = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_negate(v)?);
				}
				Instruction::LogicNot => {
					let v = stack.pop().unwrap_or(Value::Undefined);
					stack.push(Value::Boolean(!scalar::value_is_truthy(&v)));
				}

				// === Comparison ===
				Instruction::CmpEq => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_eq(&l, &r));
				}
				Instruction::CmpNe => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_ne(&l, &r));
				}
				Instruction::CmpLt => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_lt(&l, &r));
				}
				Instruction::CmpLe => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_le(&l, &r));
				}
				Instruction::CmpGt => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_gt(&l, &r));
				}
				Instruction::CmpGe => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_ge(&l, &r));
				}

				// === Logic ===
				Instruction::LogicAnd => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_and(&l, &r));
				}
				Instruction::LogicOr => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_or(&l, &r));
				}
				Instruction::LogicXor => {
					let r = stack.pop().unwrap_or(Value::Undefined);
					let l = stack.pop().unwrap_or(Value::Undefined);
					let lb = scalar::value_is_truthy(&l);
					let rb = scalar::value_is_truthy(&r);
					stack.push(Value::Boolean(lb ^ rb));
				}

				// === Compound ===
				Instruction::Cast(target) => {
					let v = stack.pop().unwrap_or(Value::Undefined);
					stack.push(scalar::scalar_cast(v, *target)?);
				}
				Instruction::Between => {
					let upper = stack.pop().unwrap_or(Value::Undefined);
					let lower = stack.pop().unwrap_or(Value::Undefined);
					let val = stack.pop().unwrap_or(Value::Undefined);
					let ge = scalar::scalar_ge(&val, &lower);
					let le = scalar::scalar_le(&val, &upper);
					let result = match (ge, le) {
						(Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(a && b),
						_ => Value::Undefined,
					};
					stack.push(result);
				}
				Instruction::InList {
					count,
					negated,
				} => {
					let count = *count as usize;
					let negated = *negated;
					let mut items: Vec<Value> = Vec::with_capacity(count);
					for _ in 0..count {
						items.push(stack.pop().unwrap_or(Value::Undefined));
					}
					items.reverse();
					let val = stack.pop().unwrap_or(Value::Undefined);
					let found = items.iter().any(|item| {
						matches!(scalar::scalar_eq(&val, item), Value::Boolean(true))
					});
					stack.push(Value::Boolean(if negated {
						!found
					} else {
						found
					}));
				}

				// === Control flow ===
				Instruction::Jump(addr) => {
					ip = *addr;
					continue;
				}
				Instruction::JumpIfFalsePop(addr) => {
					let v = stack.pop().unwrap_or(Value::Undefined);
					if !scalar::value_is_truthy(&v) {
						ip = *addr;
						continue;
					}
				}
				Instruction::JumpIfTruePop(addr) => {
					let v = stack.pop().unwrap_or(Value::Undefined);
					if scalar::value_is_truthy(&v) {
						ip = *addr;
						continue;
					}
				}

				Instruction::EnterScope(scope_type) => {
					symbol_table.enter_scope(scope_type.clone());
				}
				Instruction::ExitScope => {
					let _ = symbol_table.exit_scope();
				}

				// === Return ===
				Instruction::ReturnValue => {
					let v = stack.pop().unwrap_or(Value::Undefined);
					return Ok(v);
				}
				Instruction::ReturnVoid => {
					return Ok(Value::Undefined);
				}

				// === Query ===
				Instruction::Query(plan) => match plan {
					QueryPlan::Map(map_node) => {
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
								stack.push(result_column.data.get_value(0));
							}
						}
					}
					_ => {
						// Other plan types would need full VM execution
					}
				},

				Instruction::Emit => {
					// Emit in function body context - the stack top is the result
				}

				// === Function calls within function body ===
				Instruction::Call {
					name,
					arity,
				} => {
					let arity = *arity as usize;
					let mut args: Vec<Value> = Vec::with_capacity(arity);
					for _ in 0..arity {
						args.push(stack.pop().unwrap_or(Value::Undefined));
					}
					args.reverse();

					// Try user-defined function
					if let Some(func_def) = symbol_table.get_function(name.text()) {
						let func_def = func_def.clone();
						let base_depth = symbol_table.scope_depth();
						symbol_table.enter_scope(ScopeType::Function);
						for (param, arg_val) in func_def.parameters.iter().zip(args.iter()) {
							let param_name = strip_dollar_prefix(param.name.text());
							symbol_table.set(
								param_name,
								Variable::scalar(arg_val.clone()),
								true,
							)?;
						}
						let result = self.execute_function_body_for_scalar(
							&func_def.body,
							symbol_table,
							params,
						)?;
						while symbol_table.scope_depth() > base_depth {
							let _ = symbol_table.exit_scope();
						}
						stack.push(result);
					} else if let Some(functor) = self.functions.get_scalar(name.text()) {
						let mut arg_cols = Vec::with_capacity(args.len());
						for arg in &args {
							let mut data = ColumnData::undefined(0);
							data.push_value(arg.clone());
							arg_cols.push(Column::new("_", data));
						}
						let columns = Columns::new(arg_cols);
						let result_data = functor.scalar(ScalarFunctionContext {
							fragment: name.clone(),
							columns: &columns,
							row_count: 1,
							clock: &self.clock,
						})?;
						if result_data.len() > 0 {
							stack.push(result_data.get_value(0));
						} else {
							stack.push(Value::Undefined);
						}
					}
				}

				Instruction::DefineFunction(func_def) => {
					symbol_table
						.define_function(func_def.name.text().to_string(), func_def.clone());
				}

				_ => {
					// DDL/DML instructions not expected in function body
				}
			}
			ip += 1;
		}

		// Return top of stack or Undefined
		Ok(stack.pop().unwrap_or(Value::Undefined))
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
