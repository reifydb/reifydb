// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData, view::group_by::GroupByView};
use reifydb_routine::function::{AggregateFunctionContext, ScalarFunctionContext, registry::Functions};
use reifydb_rql::{
	expression::CallExpression,
	instruction::{CompiledFunction, Instruction, ScopeType},
	query::QueryPlan,
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::{
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId, r#type::Type},
};

use super::eval::evaluate;
use crate::{
	Result,
	error::EngineError,
	expression::context::{EvalContext, EvalSession},
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
		return ColumnData::none_typed(Type::Boolean, 0);
	}

	let mut data = ColumnData::none_typed(Type::Boolean, 0);
	for value in values {
		data.push_value(value.clone());
	}
	data
}

/// Evaluate a call expression with pre-evaluated arguments (avoids re-compiling argument expressions).
pub(crate) fn call_eval_with_args(
	ctx: &EvalContext,
	call: &CallExpression,
	arguments: Columns,
	functions: &Functions,
) -> Result<Column> {
	let function_name = call.func.0.text();

	// Check if we're in aggregation context and if function exists as aggregate
	if ctx.is_aggregate_context {
		if let Some(mut aggregate_fn) = functions.get_aggregate(function_name) {
			let column = if call.args.is_empty() {
				Column {
					name: Fragment::internal("dummy"),
					data: ColumnData::with_capacity(Type::Int4, ctx.row_count),
				}
			} else {
				arguments[0].clone()
			};

			let mut group_view = GroupByView::new();
			let all_indices: Vec<usize> = (0..ctx.row_count).collect();
			group_view.insert(Vec::<Value>::new(), all_indices);

			let agg_fragment = call.func.0.clone();
			aggregate_fn
				.aggregate(AggregateFunctionContext {
					fragment: agg_fragment.clone(),
					column: &column,
					groups: &group_view,
				})
				.map_err(|e| e.with_context(agg_fragment.clone()))?;

			let (_keys, result_data) = aggregate_fn.finalize().map_err(|e| e.with_context(agg_fragment))?;

			return Ok(Column {
				name: call.full_fragment_owned(),
				data: result_data,
			});
		}
	}

	// Try user-defined function from symbol table first
	if let Some(func_def) = ctx.symbols.get_function(function_name) {
		return call_user_defined_function(ctx, call, func_def.clone(), &arguments, functions);
	}

	// Fall back to built-in scalar function handling
	let functor = functions.get_scalar(function_name).ok_or_else(|| -> Error {
		EngineError::UnknownFunction {
			name: call.func.0.text().to_string(),
			fragment: call.func.0.clone(),
		}
		.into()
	})?;

	let row_count = ctx.row_count;

	let fn_fragment = call.func.0.clone();
	let final_data = functor
		.scalar(ScalarFunctionContext {
			fragment: fn_fragment.clone(),
			columns: &arguments,
			row_count,
			runtime_context: ctx.runtime_context,
			identity: ctx.identity,
		})
		.map_err(|e| e.with_context(fn_fragment))?;

	Ok(Column {
		name: call.full_fragment_owned(),
		data: final_data,
	})
}

/// Execute a user-defined function for each row, returning a column of results
fn call_user_defined_function(
	ctx: &EvalContext,
	call: &CallExpression,
	func_def: CompiledFunction,
	arguments: &Columns,
	functions: &Functions,
) -> Result<Column> {
	let row_count = ctx.row_count;
	let mut results: Vec<Value> = Vec::with_capacity(row_count);

	// Function body is already pre-compiled
	let body_instructions = &func_def.body;

	let mut func_symbols = ctx.symbols.clone();

	// For each row, execute the function
	for row_idx in 0..row_count {
		let base_depth = func_symbols.scope_depth();
		func_symbols.enter_scope(ScopeType::Function);

		// Bind arguments to parameters
		for (param, arg_col) in func_def.parameters.iter().zip(arguments.iter()) {
			let param_name = strip_dollar_prefix(param.name.text());
			let value = arg_col.data().get_value(row_idx);
			func_symbols.set(param_name, Variable::scalar(value), true)?;
		}

		// Execute function body instructions and get result
		let result = execute_function_body_for_scalar(
			&body_instructions,
			&mut func_symbols,
			ctx.params,
			functions,
			ctx.runtime_context,
			ctx.identity,
		)?;

		while func_symbols.scope_depth() > base_depth {
			let _ = func_symbols.exit_scope();
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
	instructions: &[Instruction],
	symbols: &mut SymbolTable,
	params: &Params,
	functions: &Functions,
	runtime_context: &RuntimeContext,
	identity: IdentityId,
) -> Result<Value> {
	let mut ip = 0;
	let mut stack: Vec<Value> = Vec::new();

	while ip < instructions.len() {
		match &instructions[ip] {
			Instruction::Halt => break,
			Instruction::Nop => {}

			Instruction::PushConst(v) => stack.push(v.clone()),
			Instruction::PushNone => stack.push(Value::none()),
			Instruction::Pop => {
				stack.pop();
			}
			Instruction::Dup => {
				if let Some(v) = stack.last() {
					stack.push(v.clone());
				}
			}

			Instruction::LoadVar(name) => {
				let var_name = strip_dollar_prefix(name.text());
				let val = symbols
					.get(&var_name)
					.map(|v| match v {
						Variable::Scalar(c) => c.scalar_value(),
						_ => Value::none(),
					})
					.unwrap_or(Value::none());
				stack.push(val);
			}
			Instruction::StoreVar(name) => {
				let val = stack.pop().unwrap_or(Value::none());
				let var_name = strip_dollar_prefix(name.text());
				symbols.set(var_name, Variable::scalar(val), true)?;
			}
			Instruction::DeclareVar(name) => {
				let val = stack.pop().unwrap_or(Value::none());
				let var_name = strip_dollar_prefix(name.text());
				symbols.set(var_name, Variable::scalar(val), true)?;
			}

			Instruction::Add => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_add(l, r)?);
			}
			Instruction::Sub => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_sub(l, r)?);
			}
			Instruction::Mul => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_mul(l, r)?);
			}
			Instruction::Div => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_div(l, r)?);
			}
			Instruction::Rem => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_rem(l, r)?);
			}

			Instruction::Negate => {
				let v = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_negate(v)?);
			}
			Instruction::LogicNot => {
				let v = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_not(&v));
			}

			Instruction::CmpEq => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_eq(&l, &r));
			}
			Instruction::CmpNe => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_ne(&l, &r));
			}
			Instruction::CmpLt => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_lt(&l, &r));
			}
			Instruction::CmpLe => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_le(&l, &r));
			}
			Instruction::CmpGt => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_gt(&l, &r));
			}
			Instruction::CmpGe => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_ge(&l, &r));
			}

			Instruction::LogicAnd => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_and(&l, &r));
			}
			Instruction::LogicOr => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_or(&l, &r));
			}
			Instruction::LogicXor => {
				let r = stack.pop().unwrap_or(Value::none());
				let l = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_xor(&l, &r));
			}

			Instruction::Cast(target) => {
				let v = stack.pop().unwrap_or(Value::none());
				stack.push(scalar::scalar_cast(v, target.clone())?);
			}
			Instruction::Between => {
				let upper = stack.pop().unwrap_or(Value::none());
				let lower = stack.pop().unwrap_or(Value::none());
				let val = stack.pop().unwrap_or(Value::none());
				let ge = scalar::scalar_ge(&val, &lower);
				let le = scalar::scalar_le(&val, &upper);
				let result = match (ge, le) {
					(Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(a && b),
					_ => Value::none(),
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
					items.push(stack.pop().unwrap_or(Value::none()));
				}
				items.reverse();
				let val = stack.pop().unwrap_or(Value::none());
				let has_undefined = matches!(val, Value::None { .. })
					|| items.iter().any(|item| matches!(item, Value::None { .. }));
				if has_undefined {
					stack.push(Value::none());
				} else {
					let found = items.iter().any(|item| {
						matches!(scalar::scalar_eq(&val, item), Value::Boolean(true))
					});
					stack.push(Value::Boolean(if negated {
						!found
					} else {
						found
					}));
				}
			}

			Instruction::Jump(addr) => {
				ip = *addr;
				continue;
			}
			Instruction::JumpIfFalsePop(addr) => {
				let v = stack.pop().unwrap_or(Value::none());
				if !scalar::value_is_truthy(&v) {
					ip = *addr;
					continue;
				}
			}
			Instruction::JumpIfTruePop(addr) => {
				let v = stack.pop().unwrap_or(Value::none());
				if scalar::value_is_truthy(&v) {
					ip = *addr;
					continue;
				}
			}

			Instruction::EnterScope(scope_type) => {
				symbols.enter_scope(scope_type.clone());
			}
			Instruction::ExitScope => {
				let _ = symbols.exit_scope();
			}

			Instruction::ReturnValue => {
				let v = stack.pop().unwrap_or(Value::none());
				return Ok(v);
			}
			Instruction::ReturnVoid => {
				return Ok(Value::none());
			}

			Instruction::Query(plan) => match plan {
				QueryPlan::Map(map_node) => {
					if map_node.input.is_none() && !map_node.map.is_empty() {
						let call_session = EvalSession {
							params,
							symbols,
							functions,
							runtime_context,
							arena: None,
							identity,
							is_aggregate_context: false,
						};
						let evaluation_context = call_session.eval_empty();
						let result_column = evaluate(&evaluation_context, &map_node.map[0])?;
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

			Instruction::Call {
				name,
				arity,
				..
			} => {
				let arity = *arity as usize;
				let mut args: Vec<Value> = Vec::with_capacity(arity);
				for _ in 0..arity {
					args.push(stack.pop().unwrap_or(Value::none()));
				}
				args.reverse();

				// Try user-defined function
				if let Some(func_def) = symbols.get_function(name.text()) {
					let func_def = func_def.clone();
					let base_depth = symbols.scope_depth();
					symbols.enter_scope(ScopeType::Function);
					for (param, arg_val) in func_def.parameters.iter().zip(args.iter()) {
						let param_name = strip_dollar_prefix(param.name.text());
						symbols.set(param_name, Variable::scalar(arg_val.clone()), true)?;
					}
					let result = execute_function_body_for_scalar(
						&func_def.body,
						symbols,
						params,
						functions,
						runtime_context,
						identity,
					)?;
					while symbols.scope_depth() > base_depth {
						let _ = symbols.exit_scope();
					}
					stack.push(result);
				} else if let Some(functor) = functions.get_scalar(name.text()) {
					let mut arg_cols = Vec::with_capacity(args.len());
					for arg in &args {
						let mut data = ColumnData::none_typed(Type::Boolean, 0);
						data.push_value(arg.clone());
						arg_cols.push(Column::new("_", data));
					}
					let columns = Columns::new(arg_cols);
					let fn_fragment = name.clone();
					let result_data = functor
						.scalar(ScalarFunctionContext {
							fragment: fn_fragment.clone(),
							columns: &columns,
							row_count: 1,
							runtime_context,
							identity,
						})
						.map_err(|e| e.with_context(fn_fragment))?;
					if result_data.len() > 0 {
						stack.push(result_data.get_value(0));
					} else {
						stack.push(Value::none());
					}
				}
			}

			Instruction::DefineFunction(func_def) => {
				symbols.define_function(func_def.name.text().to_string(), func_def.clone());
			}

			_ => {
				// DDL/DML instructions not expected in function body
			}
		}
		ip += 1;
	}

	// Return top of stack or Undefined
	Ok(stack.pop().unwrap_or(Value::none()))
}
