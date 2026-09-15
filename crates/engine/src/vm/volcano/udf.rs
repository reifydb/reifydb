// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders};
use reifydb_evaluate::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
		udf_extract::{ExtractedUdf, extract_udf_calls},
	},
	stack::{SymbolTable, Variable, strip_dollar_prefix},
};
use reifydb_rql::{
	expression::Expression,
	instruction::{Instruction, ScopeType},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, frame::frame::Frame, value_type::ValueType};
use tracing::instrument;

use crate::{
	Result,
	vm::{
		exec::call::{
			cast_to_declared_return_type, cast_to_parameter_type, check_arity, collect_call_result,
			declared_return_column, untyped_return_column,
		},
		vm::{EMPTY_PARAMS, UdfCall, Vm},
		volcano::query::{QueryContext, QueryNode, eval_context_from_query},
	},
};

struct CompiledUdfCall {
	udf: ExtractedUdf,

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

	#[instrument(level = "trace", skip_all, name = "volcano::udf_eval::args")]
	fn eval_args(call: &CompiledUdfCall, eval_ctx: &EvalContext) -> Result<Vec<ColumnWithName>> {
		let mut arg_columns = Vec::with_capacity(call.compiled_args.len());
		for (compiled_arg, parameter) in call.compiled_args.iter().zip(call.udf.callable.parameters.iter()) {
			let argument = compiled_arg.execute(eval_ctx)?;
			arg_columns.push(ColumnWithName::new(
				argument.name,
				cast_to_parameter_type(eval_ctx, parameter, argument.data)?,
			));
		}
		Ok(arg_columns)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::udf_eval::vectorized")]
	fn run_vectorized<'a>(
		rx: &mut Transaction<'a>,
		stored_ctx: &QueryContext,
		eval_ctx: &EvalContext,
		call: &CompiledUdfCall,
		arg_columns: &[ColumnWithName],
		row_count: usize,
	) -> Result<ColumnWithName> {
		let mut func_symbols = stored_ctx.symbols.clone();
		func_symbols.enter_scope(ScopeType::Function);

		for (cap_name, cap_var) in &call.udf.callable.captured {
			func_symbols.set(cap_name.clone(), cap_var.clone(), true)?;
		}

		for (param, arg_col) in call.udf.callable.parameters.iter().zip(arg_columns.iter()) {
			let param_name = strip_dollar_prefix(param.name.text()).to_string();
			let col_var = Variable::columns(Columns::new(vec![arg_col.clone()]));
			func_symbols.set(param_name, col_var, true)?;
		}

		let mut vm = Vm::with_batch_size_from_services(
			func_symbols,
			row_count,
			UdfCall {
				fragment: call.udf.fragment.clone(),
				return_type: call.udf.callable.return_type.clone(),
			},
			&stored_ctx.services,
			&EMPTY_PARAMS,
			stored_ctx.identity,
		);
		let mut func_result: Vec<Frame> = Vec::new();
		vm.run(&stored_ctx.services, rx, &call.udf.callable.body, &mut func_result)?;

		let result_var = collect_call_result(&mut vm, &mut func_result);
		let column = match result_var {
			Variable::Columns {
				columns: c,
				..
			} if !c.is_empty() => {
				let name = c.names.first().cloned().unwrap_or_else(|| call.udf.result_column.clone());
				let data = c.columns.into_iter().next().unwrap();
				ColumnWithName::new(name, data)
			}
			_ => {
				let data = ColumnBuffer::none_typed(ValueType::Any, row_count);
				ColumnWithName {
					name: call.udf.result_column.clone(),
					data,
				}
			}
		};
		match &call.udf.callable.return_type {
			Some(declared) => {
				let data = cast_to_declared_return_type(
					eval_ctx,
					&column.data,
					declared,
					&call.udf.name,
					&call.udf.fragment,
				)?;
				Ok(ColumnWithName::new(column.name, data))
			}
			None => Ok(column),
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::udf_eval::scalar")]
	fn run_scalar<'a>(
		rx: &mut Transaction<'a>,
		stored_ctx: &QueryContext,
		eval_ctx: &EvalContext,
		call: &CompiledUdfCall,
		arg_columns: &[ColumnWithName],
		row_count: usize,
	) -> Result<ColumnWithName> {
		let mut results: Vec<Value> = Vec::with_capacity(row_count);
		let mut func_symbols = stored_ctx.symbols.clone();

		for row_idx in 0..row_count {
			func_symbols.enter_scope(ScopeType::Function);

			for (cap_name, cap_var) in &call.udf.callable.captured {
				func_symbols.set(cap_name.clone(), cap_var.clone(), true)?;
			}

			for (param, arg_col) in call.udf.callable.parameters.iter().zip(arg_columns.iter()) {
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
			vm.run(&stored_ctx.services, rx, &call.udf.callable.body, &mut func_result)?;
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

		let data = match &call.udf.callable.return_type {
			Some(declared) => {
				declared_return_column(eval_ctx, results, declared, &call.udf.name, &call.udf.fragment)?
			}
			None => untyped_return_column(results, &call.udf.name, &call.udf.fragment)?,
		};
		Ok(ColumnWithName {
			name: call.udf.result_column.clone(),
			data,
		})
	}
}

impl QueryNode for UdfEvalNode {
	#[instrument(level = "trace", skip_all, name = "volcano::udf_eval::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		for udf in &self.udf_calls {
			check_arity(&udf.callable.parameters, udf.arg_expressions.len(), &udf.name, &udf.fragment)?;
		}

		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};

		let compiled: Vec<CompiledUdfCall> = self
			.udf_calls
			.drain(..)
			.map(|udf| {
				let compiled_args = udf
					.arg_expressions
					.iter()
					.map(|e| compile_expression(&compile_ctx, e))
					.collect::<Result<Vec<_>>>()?;
				Ok(CompiledUdfCall {
					udf,
					compiled_args,
				})
			})
			.collect::<Result<Vec<_>>>()?;

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

		for call in compiled_calls {
			let session = eval_context_from_query(stored_ctx);
			let eval_ctx = session.with_eval(columns.clone(), row_count);

			let arg_columns = Self::eval_args(call, &eval_ctx)?;

			let result_column = if is_vectorizable(&call.udf.callable.body) {
				Self::run_vectorized(rx, stored_ctx, &eval_ctx, call, &arg_columns, row_count)?
			} else {
				Self::run_scalar(rx, stored_ctx, &eval_ctx, call, &arg_columns, row_count)?
			};

			columns.columns.push(result_column.data);
			columns.names.push(call.udf.result_column.clone());
		}

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

fn returns_void_early(instructions: &[Instruction]) -> bool {
	instructions.iter().enumerate().any(|(idx, instr)| {
		matches!(instr, Instruction::ReturnVoid)
			&& instructions[idx + 1..].iter().any(|rest| !matches!(rest, Instruction::Halt))
	})
}

pub(crate) fn is_vectorizable(instructions: &[Instruction]) -> bool {
	if returns_void_early(instructions) {
		return false;
	}

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

pub(crate) fn strip_udf_columns(columns: &mut Columns, udf_names: &[String]) {
	if udf_names.is_empty() {
		return;
	}
	let keep: Vec<bool> = columns.names.iter().map(|n| !udf_names.iter().any(|u| u == n.text())).collect();
	let mut idx = 0;
	columns.columns.retain(|_| {
		let k = keep[idx];
		idx += 1;
		k
	});
	let mut idx = 0;
	columns.names.retain(|_| {
		let k = keep[idx];
		idx += 1;
		k
	});
}

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
		symbols: &ctx.symbols,
	};
	let session = eval_context_from_query(ctx);
	let mut result_columns = Vec::new();

	for udf in &all_udfs {
		check_arity(&udf.callable.parameters, udf.arg_expressions.len(), &udf.name, &udf.fragment)?;
		let mut func_symbols = ctx.symbols.clone();
		func_symbols.enter_scope(ScopeType::Function);

		for (cap_name, cap_var) in &udf.callable.captured {
			func_symbols.set(cap_name.clone(), cap_var.clone(), true)?;
		}

		for (param, arg_expr) in udf.callable.parameters.iter().zip(udf.arg_expressions.iter()) {
			let compiled_arg = compile_expression(&compile_ctx, arg_expr)?;
			let eval_ctx = session.with_eval_empty();
			let arg_col = compiled_arg.execute(&eval_ctx)?;
			let value = cast_to_parameter_type(&eval_ctx, param, arg_col.data)?.get_value(0);
			let param_name = strip_dollar_prefix(param.name.text()).to_string();
			func_symbols.set(param_name, Variable::scalar(value), true)?;
		}

		let mut vm = Vm::from_services(func_symbols, &ctx.services, &EMPTY_PARAMS, ctx.identity);
		let mut func_result: Vec<Frame> = Vec::new();
		vm.run(&ctx.services, rx, &udf.callable.body, &mut func_result)?;
		let result_var = collect_call_result(&mut vm, &mut func_result);
		let value = match result_var {
			Variable::Columns {
				columns: c,
			} if c.is_scalar() => c.scalar_value(),
			_ => Value::none(),
		};

		let data = match &udf.callable.return_type {
			Some(declared) => declared_return_column(
				&session.with_eval_empty(),
				vec![value],
				declared,
				&udf.name,
				&udf.fragment,
			)?,
			None => {
				let mut data = ColumnBuffer::none_typed(value.get_type(), 0);
				data.push_value(value);
				data
			}
		};
		result_columns.push(ColumnWithName {
			name: udf.result_column.clone(),
			data,
		});
	}

	Ok(Some((rewritten, Columns::new(result_columns))))
}
