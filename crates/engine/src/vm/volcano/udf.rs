// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Volcano operator that evaluates user-defined function calls row-by-row via the VM.
//!
//! This node sits between the data source and the downstream expression-evaluating operators
//! (Map, Filter, Extend, etc.). It pre-computes UDF results as synthetic columns so that
//! the expression evaluator never needs to handle UDFs directly.

use std::sync::Arc;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders};
use reifydb_rql::{
	expression::Expression,
	instruction::{Instruction, ScopeType},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{Value, frame::frame::Frame, r#type::Type};
use tracing::instrument;

use crate::{
	Result,
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
		udf_extract::{ExtractedUdf, extract_udf_calls},
	},
	vm::{
		exec::{call::collect_call_result, stack::strip_dollar_prefix},
		stack::{SymbolTable, Variable},
		vm::{EMPTY_PARAMS, Vm},
		volcano::query::{QueryContext, QueryNode},
	},
};

/// Pre-compiled UDF call ready for row-by-row execution.
struct CompiledUdfCall {
	/// The extracted UDF definition and metadata.
	udf: ExtractedUdf,
	/// Compiled argument expressions for column-oriented evaluation.
	compiled_args: Vec<CompiledExpr>,
}

pub(crate) struct UdfEvalNode {
	input: Box<dyn QueryNode>,
	udf_calls: Vec<ExtractedUdf>,
	context: Option<(Arc<QueryContext>, Vec<CompiledUdfCall>)>,
}

impl UdfEvalNode {
	pub fn new(input: Box<dyn QueryNode>, udf_calls: Vec<ExtractedUdf>) -> Self {
		Self {
			input,
			udf_calls,
			context: None,
		}
	}

	/// If expressions contain UDF calls, wraps input with a UdfEvalNode and rewrites expressions.
	/// Otherwise returns input and expressions unchanged.
	/// Returns (wrapped_input, rewritten_expressions, synthetic_column_names).
	pub fn wrap_if_needed(
		input: Box<dyn QueryNode>,
		expressions: &[Expression],
		symbols: &SymbolTable,
	) -> (Box<dyn QueryNode>, Vec<Expression>, Vec<String>) {
		let mut counter = 0;
		let mut all_udfs = Vec::new();
		let rewritten: Vec<Expression> = expressions
			.iter()
			.map(|e| {
				let (expr, udfs) = extract_udf_calls(e, symbols, &mut counter);
				all_udfs.extend(udfs);
				expr
			})
			.collect();

		if all_udfs.is_empty() {
			(input, expressions.to_vec(), Vec::new())
		} else {
			let udf_names: Vec<String> =
				all_udfs.iter().map(|u| u.result_column.text().to_string()).collect();
			(Box::new(UdfEvalNode::new(input, all_udfs)), rewritten, udf_names)
		}
	}
}

impl QueryNode for UdfEvalNode {
	#[instrument(level = "trace", skip_all, name = "volcano::udf_eval::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let compile_ctx = CompileContext {
			functions: &ctx.services.functions,
			symbols: &ctx.symbols,
		};

		// Compile argument expressions for each UDF call
		let compiled: Vec<CompiledUdfCall> = self
			.udf_calls
			.drain(..)
			.map(|udf| {
				let compiled_args = udf
					.arg_expressions
					.iter()
					.map(|e| compile_expression(&compile_ctx, e).expect("compile UDF arg"))
					.collect();
				CompiledUdfCall {
					udf,
					compiled_args,
				}
			})
			.collect();

		self.context = Some((Arc::new(ctx.clone()), compiled));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::udf_eval::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let Some(mut columns) = self.input.next(rx, ctx)? else {
			return Ok(None);
		};

		let (stored_ctx, compiled_calls) = self.context.as_ref().unwrap();
		let row_count = columns.row_count();

		if row_count == 0 {
			return Ok(Some(columns));
		}

		for call in compiled_calls {
			// Evaluate argument expressions column-oriented
			let session = EvalContext::from_query(stored_ctx);
			let eval_ctx = session.with_eval(columns.clone(), row_count);

			let mut arg_columns = Vec::with_capacity(call.compiled_args.len());
			for compiled_arg in &call.compiled_args {
				arg_columns.push(compiled_arg.execute(&eval_ctx)?);
			}

			let result_column = if is_vectorizable(&call.udf.func_def.body) {
				// Batch path: execute UDF body ONCE with batch_size = row_count
				let mut func_symbols = stored_ctx.symbols.clone();
				func_symbols.enter_scope(ScopeType::Function);

				// Bind arguments as full Columns
				for (param, arg_col) in call.udf.func_def.parameters.iter().zip(arg_columns.iter()) {
					let param_name = strip_dollar_prefix(param.name.text()).to_string();
					let col_var = Variable::columns(Columns::new(vec![arg_col.clone()]));
					func_symbols.set(param_name, col_var, true)?;
				}

				// Execute via columnar VM - one invocation for all rows
				let mut vm = Vm::with_batch_size_from_services(
					func_symbols,
					row_count,
					&stored_ctx.services,
					&EMPTY_PARAMS,
					stored_ctx.identity,
				);
				let mut func_result: Vec<Frame> = Vec::new();
				vm.run(&stored_ctx.services, rx, &call.udf.func_def.body, &mut func_result)?;

				// Extract result column from return value
				let result_var = collect_call_result(&mut vm, &mut func_result);
				match result_var {
					Variable::Columns {
						columns: c,
						..
					} if !c.is_empty() => c.columns.into_inner().into_iter().next().unwrap(),
					_ => {
						let data = ColumnBuffer::none_typed(Type::Any, row_count);
						ColumnWithName {
							name: call.udf.result_column.clone(),
							data,
						}
					}
				}
			} else {
				// Scalar fallback: execute UDF body row-by-row via canonical VM
				let mut results: Vec<Value> = Vec::with_capacity(row_count);
				let mut func_symbols = stored_ctx.symbols.clone();

				for row_idx in 0..row_count {
					func_symbols.enter_scope(ScopeType::Function);

					for (param, arg_col) in
						call.udf.func_def.parameters.iter().zip(arg_columns.iter())
					{
						let param_name = strip_dollar_prefix(param.name.text()).to_string();
						let value = arg_col.data().get_value(row_idx);
						func_symbols.set(param_name, Variable::scalar(value), true)?;
					}

					let mut vm = Vm::from_services(
						func_symbols,
						&stored_ctx.services,
						&EMPTY_PARAMS,
						stored_ctx.identity,
					);
					let mut func_result: Vec<Frame> = Vec::new();
					vm.run(&stored_ctx.services, rx, &call.udf.func_def.body, &mut func_result)?;
					let result_var = collect_call_result(&mut vm, &mut func_result);
					let result = match result_var {
						Variable::Columns {
							columns: c,
						} if c.is_scalar() => c.scalar_value(),
						_ => Value::none(),
					};

					func_symbols = vm.symbols;
					let _ = func_symbols.exit_scope();
					results.push(result);
				}

				let col_type = results.first().map(|v| v.get_type()).unwrap_or(Type::Any);
				let mut data = ColumnBuffer::none_typed(col_type, 0);
				for value in &results {
					data.push_value(value.clone());
				}
				ColumnWithName {
					name: call.udf.result_column.clone(),
					data,
				}
			};

			columns.columns
				.make_mut()
				.push(ColumnWithName::new(call.udf.result_column.clone(), result_column.data));
		}

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

/// Check if a UDF body contains only instructions that the columnar VM can batch-execute.
///
/// Every instruction listed here must have a batch-capable handler in `vm/exec/`:
/// either a kernel that operates on `Column`s (arithmetic, comparison, logic,
/// between, in_list, cast), or a mask-aware dispatch path (jump instructions,
/// scoped control flow). If you add an instruction to this list, verify its
/// handler accepts multi-row columns, or `pop_value()` will fail at runtime.
pub(crate) fn is_vectorizable(instructions: &[Instruction]) -> bool {
	instructions.iter().all(|instr| {
		matches!(
			instr,
			Instruction::PushConst(_)
				| Instruction::PushNone | Instruction::Pop
				| Instruction::Dup | Instruction::LoadVar(_)
				| Instruction::StoreVar(_) | Instruction::DeclareVar(_)
				| Instruction::FieldAccess { .. }
				| Instruction::Add | Instruction::Sub
				| Instruction::Mul | Instruction::Div
				| Instruction::Rem | Instruction::Negate
				| Instruction::LogicNot | Instruction::CmpEq
				| Instruction::CmpNe | Instruction::CmpLt
				| Instruction::CmpLe | Instruction::CmpGt
				| Instruction::CmpGe | Instruction::LogicAnd
				| Instruction::LogicOr | Instruction::LogicXor
				| Instruction::Between | Instruction::InList { .. }
				| Instruction::Cast(_) | Instruction::Jump(_)
				| Instruction::JumpIfFalsePop(_)
				| Instruction::JumpIfTruePop(_)
				| Instruction::EnterScope(_) | Instruction::ExitScope
				| Instruction::ReturnValue | Instruction::ReturnVoid
				| Instruction::DefineFunction(_)
				| Instruction::DefineClosure(_)
				| Instruction::Call { .. } | Instruction::Nop
				| Instruction::Halt
		)
	})
}

/// Remove synthetic `__udf_N` columns from output so they never leak to the user.
pub(crate) fn strip_udf_columns(columns: &mut Columns, udf_names: &[String]) {
	if udf_names.is_empty() {
		return;
	}
	columns.columns.make_mut().retain(|c| !udf_names.iter().any(|n| n == c.name.text()));
}

/// Extract UDF calls from expressions and evaluate them for a single row with no input columns.
/// Returns the rewritten expressions and a `Columns` containing the UDF result columns.
/// If no UDFs are found, returns `None`.
pub(crate) fn evaluate_udfs_no_input(
	expressions: &[Expression],
	ctx: &QueryContext,
	rx: &mut Transaction<'_>,
) -> Result<Option<(Vec<Expression>, Columns)>> {
	let mut counter = 0;
	let mut all_udfs = Vec::new();
	let rewritten: Vec<Expression> = expressions
		.iter()
		.map(|e| {
			let (expr, udfs) = extract_udf_calls(e, &ctx.symbols, &mut counter);
			all_udfs.extend(udfs);
			expr
		})
		.collect();

	if all_udfs.is_empty() {
		return Ok(None);
	}

	let compile_ctx = CompileContext {
		functions: &ctx.services.functions,
		symbols: &ctx.symbols,
	};
	let session = EvalContext::from_query(ctx);
	let mut result_columns = Vec::new();

	for udf in &all_udfs {
		let mut func_symbols = ctx.symbols.clone();
		func_symbols.enter_scope(ScopeType::Function);

		// Evaluate arguments as scalar expressions (no input columns)
		for (param, arg_expr) in udf.func_def.parameters.iter().zip(udf.arg_expressions.iter()) {
			let compiled_arg = compile_expression(&compile_ctx, arg_expr).expect("compile UDF arg");
			let eval_ctx = session.with_eval_empty();
			let arg_col = compiled_arg.execute(&eval_ctx)?;
			let value = arg_col.data().get_value(0);
			let param_name = strip_dollar_prefix(param.name.text()).to_string();
			func_symbols.set(param_name, Variable::scalar(value), true)?;
		}

		// Execute UDF via canonical VM
		let mut vm = Vm::from_services(func_symbols, &ctx.services, &EMPTY_PARAMS, ctx.identity);
		let mut func_result: Vec<Frame> = Vec::new();
		vm.run(&ctx.services, rx, &udf.func_def.body, &mut func_result)?;
		let result_var = collect_call_result(&mut vm, &mut func_result);
		let value = match result_var {
			Variable::Columns {
				columns: c,
			} if c.is_scalar() => c.scalar_value(),
			_ => Value::none(),
		};

		let mut data = ColumnBuffer::none_typed(value.get_type(), 0);
		data.push_value(value);
		result_columns.push(ColumnWithName {
			name: udf.result_column.clone(),
			data,
		});
	}

	Ok(Some((rewritten, Columns::new(result_columns))))
}
