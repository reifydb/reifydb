// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use reifydb_catalog::catalog::{Catalog, procedure::ResolvedProcedure};
use reifydb_core::{
	interface::catalog::{
		policy::{CallableOp, PolicyTargetType},
		procedure::{Procedure, ProcedureParam},
	},
	internal_error,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_routine::{function::FunctionContext, procedure::context::ProcedureContext};
use reifydb_rql::{
	compiler::{CompilationResult, Compiled},
	instruction::{CompiledClosure, CompiledFunction, Instruction, ScopeType},
	nodes::FunctionParameter,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::{Error as ReifyError, ProcedureErrorKind, TypeError},
	fragment::Fragment,
	params::Params,
	value::{Value, constraint::TypeConstraint, frame::frame::Frame, r#type::Type},
};

use super::stack::strip_dollar_prefix;
use crate::{
	Result,
	error::EngineError,
	expression::cast::cast_column_data,
	policy::PolicyEvaluator,
	vm::{
		exec::broadcast::broadcast_many,
		services::Services,
		stack::{ClosureValue, ControlFlow, Variable},
		vm::{EMPTY_PARAMS, Vm},
		volcano::udf::is_vectorizable,
	},
};

/// Groups the shared (services, tx) pair passed to call-family methods.
pub(crate) struct CallContext<'a, 'b> {
	pub services: &'a Arc<Services>,
	pub tx: &'a mut Transaction<'b>,
}

impl<'a> Vm<'a> {
	/// Coerce every column inside `result` to the declared function return type.
	/// Used by call sites so that mixed-width scalar execution paths collapse to a
	/// single column type before the result flows back to the caller.
	pub(crate) fn coerce_return_value(
		&self,
		result: Variable,
		return_type: Option<&TypeConstraint>,
	) -> Result<Variable> {
		let Some(tc) = return_type else {
			return Ok(result);
		};
		let target = tc.get_type();
		match result {
			Variable::Columns {
				columns,
			} => {
				let ctx = self.eval_ctx();
				let coerced: Vec<ColumnWithName> = columns
					.names
					.iter()
					.zip(columns.columns.iter())
					.map(|(name, data)| {
						let casted = cast_column_data(
							&ctx,
							data,
							target.clone(),
							name.clone(),
						)?;
						Ok(ColumnWithName::new(name.clone(), casted))
					})
					.collect::<Result<Vec<_>>>()?;
				Ok(Variable::columns(Columns::new(coerced)))
			}
			other => Ok(other),
		}
	}

	/// Coerce a single scalar `Value` to `target` by wrapping in a 1-row column,
	/// casting, and extracting. Slow-path helper used by the per-row fallback so
	/// that the accumulator column's element type stays uniform.
	fn coerce_value(&self, value: Value, target: &Type) -> Result<Value> {
		let mut data = ColumnBuffer::with_capacity(value.get_type(), 1);
		data.push_value(value);
		let ctx = self.eval_ctx();
		let cast = cast_column_data(&ctx, &data, target.clone(), Fragment::internal("coerce_return"))?;
		Ok(cast.get_value(0))
	}
}

pub(crate) fn collect_call_result(vm: &mut Vm, func_result: &mut Vec<Frame>) -> Variable {
	match mem::replace(&mut vm.control_flow, ControlFlow::Normal) {
		ControlFlow::Return(c) => {
			let columns = c.unwrap_or(Columns::scalar(Value::none()));
			Variable::columns(columns)
		}
		_ => {
			if let Some(frame) = func_result.pop() {
				if !frame.columns.is_empty() && !frame.columns[0].data.is_empty() {
					Variable::columns(frame.into())
				} else {
					Variable::scalar(Value::none())
				}
			} else {
				vm.stack.pop().ok().unwrap_or(Variable::scalar(Value::none()))
			}
		}
	}
}

impl<'a> Vm<'a> {
	pub(crate) fn exec_call(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		name: &Fragment,
		arity: u8,
		is_procedure_call: bool,
	) -> Result<()> {
		let arity = arity as usize;
		let func_name = name.text();

		// Columnar dispatch: user functions and closures can take a column
		// per argument (either a batch path for vectorizable bodies or a
		// per-row fallback). Procedures don't run inside UDF bodies, so
		// they stay on the scalar path.
		if self.batch_size > 1 {
			if let Some(func_def) = self.symbols.get_function(func_name).cloned() {
				return self.call_user_function_columnar(services, tx, &func_def, arity, name);
			}
			if let Some(closure_val) = self.symbols.get(strip_dollar_prefix(func_name)).cloned()
				&& let Variable::Closure(closure) = closure_val
			{
				return self.call_closure_columnar(services, tx, closure, arity);
			}
		}

		let mut args = Vec::with_capacity(arity);
		for _ in 0..arity {
			args.push(self.pop_value()?);
		}
		args.reverse();

		// 1. User-defined function (DEF)
		if let Some(func_def) = self.symbols.get_function(func_name).cloned() {
			return self.call_user_function(services, tx, &func_def, args, name);
		}

		// 2. Closure variable
		if let Some(closure_val) = self.symbols.get(strip_dollar_prefix(func_name)).cloned()
			&& let Variable::Closure(closure) = closure_val
		{
			return self.call_closure(services, tx, closure, args);
		}

		// 3. Catalog procedure
		let proc_def = {
			let mut tx_tmp = tx.reborrow();
			services.catalog.find_procedure_by_qualified_name(&mut tx_tmp, func_name)?
		};

		match proc_def {
			Some(ResolvedProcedure::Local(proc_def)) => {
				let ctx = CallContext {
					services,
					tx,
				};
				self.call_local_procedure(ctx, &proc_def, args, name, func_name)
			}
			Some(ResolvedProcedure::Test(proc_def)) => {
				let ctx = CallContext {
					services,
					tx,
				};
				self.call_test_procedure(ctx, &proc_def, args, name, func_name)
			}
			#[cfg(not(reifydb_single_threaded))]
			Some(ResolvedProcedure::Remote {
				address,
				token,
			}) => self.call_remote_procedure(services, args, name, func_name, &address, token.as_deref()),
			#[cfg(reifydb_single_threaded)]
			Some(ResolvedProcedure::Remote {
				..
			}) => Err(TypeError::Procedure {
				kind: ProcedureErrorKind::UndefinedProcedure {
					name: func_name.to_string(),
				},
				message: format!("Unknown procedure: {}", func_name),
				fragment: name.clone(),
			}
			.into()),
			None => {
				let ctx = CallContext {
					services,
					tx,
				};
				self.call_builtin_or_error(ctx, args, name, func_name, is_procedure_call)
			}
		}
	}

	fn call_user_function_columnar(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		func_def: &CompiledFunction,
		arity: usize,
		name: &Fragment,
	) -> Result<()> {
		let arg_columns = self.pop_args_as_columns(arity)?;

		if is_vectorizable(&func_def.body) {
			let row_count = arg_columns.first().map(|c| c.data.len()).unwrap_or(self.batch_size);
			self.run_function_body_batch(
				services,
				tx,
				&func_def.body,
				&func_def.parameters,
				arg_columns,
				row_count,
				None,
				func_def.return_type.as_ref(),
			)
		} else {
			self.run_function_body_per_row(
				services,
				tx,
				&func_def.body,
				&func_def.parameters,
				arg_columns,
				None,
				name,
				func_def.return_type.as_ref(),
			)
		}
	}

	fn call_closure_columnar(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		closure: ClosureValue,
		arity: usize,
	) -> Result<()> {
		let arg_columns = self.pop_args_as_columns(arity)?;

		if is_vectorizable(&closure.def.body) {
			let row_count = arg_columns.first().map(|c| c.data.len()).unwrap_or(self.batch_size);
			self.run_function_body_batch(
				services,
				tx,
				&closure.def.body,
				&closure.def.parameters,
				arg_columns,
				row_count,
				Some(&closure.captured),
				None,
			)
		} else {
			self.run_function_body_per_row(
				services,
				tx,
				&closure.def.body,
				&closure.def.parameters,
				arg_columns,
				Some(&closure.captured),
				&Fragment::internal("closure"),
				None,
			)
		}
	}

	fn pop_args_as_columns(&mut self, arity: usize) -> Result<Vec<ColumnWithName>> {
		let mut arg_columns = Vec::with_capacity(arity);
		for _ in 0..arity {
			arg_columns.push(self.pop_as_column()?);
		}
		arg_columns.reverse();
		Ok(broadcast_many(arg_columns))
	}

	#[allow(clippy::too_many_arguments)]
	fn run_function_body_batch(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		body: &[Instruction],
		parameters: &[FunctionParameter],
		arg_columns: Vec<ColumnWithName>,
		_row_count: usize,
		captured: Option<&HashMap<String, Variable>>,
		return_type: Option<&TypeConstraint>,
	) -> Result<()> {
		let saved_ip = self.ip;
		self.symbols.enter_scope(ScopeType::Function);

		if let Some(captured) = captured {
			for (cap_name, cap_var) in captured {
				self.symbols.set(cap_name.clone(), cap_var.clone(), true)?;
			}
		}

		for (param, arg_col) in parameters.iter().zip(arg_columns.into_iter()) {
			let param_name = strip_dollar_prefix(param.name.text()).to_string();
			let col_var = Variable::columns(Columns::new(vec![arg_col]));
			self.symbols.set(param_name, col_var, true)?;
		}

		self.ip = 0;
		let mut func_result = Vec::new();
		self.run_isolated_body(services, tx, body, &mut func_result)?;

		let stack_value = collect_call_result(self, &mut func_result);
		self.ip = saved_ip;
		let _ = self.symbols.exit_scope();
		let coerced = self.coerce_return_value(stack_value, return_type)?;
		self.stack.push(coerced);
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn run_function_body_per_row(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		body: &[Instruction],
		parameters: &[FunctionParameter],
		arg_columns: Vec<ColumnWithName>,
		captured: Option<&HashMap<String, Variable>>,
		name: &Fragment,
		return_type: Option<&TypeConstraint>,
	) -> Result<()> {
		let row_count = arg_columns.first().map(|c| c.data.len()).unwrap_or(0);
		let mut results: Vec<Value> = Vec::with_capacity(row_count);
		let mut func_symbols = self.symbols.clone();

		for row_idx in 0..row_count {
			func_symbols.enter_scope(ScopeType::Function);
			if let Some(captured) = captured {
				for (cap_name, cap_var) in captured {
					func_symbols.set(cap_name.clone(), cap_var.clone(), true)?;
				}
			}
			for (param, arg_col) in parameters.iter().zip(arg_columns.iter()) {
				let param_name = strip_dollar_prefix(param.name.text()).to_string();
				let value = arg_col.data().get_value(row_idx);
				func_symbols.set(
					param_name.clone(),
					Variable::scalar_named(&param_name, value),
					true,
				)?;
			}

			let mut vm = Vm::from_services(func_symbols, services, &EMPTY_PARAMS, tx.identity());
			let mut func_result: Vec<Frame> = Vec::new();
			vm.run(services, tx, body, &mut func_result)?;
			let result_var = collect_call_result(&mut vm, &mut func_result);
			let value = match result_var {
				Variable::Columns {
					columns: c,
				} if c.is_scalar() => c.scalar_value(),
				_ => Value::none(),
			};

			func_symbols = vm.symbols;
			let _ = func_symbols.exit_scope();
			results.push(value);
		}

		// Determine the accumulator column's element type.
		// - If the function declares a return type, every scalar result is coerced to it so the accumulator
		//   stays uniform (and mixed-width scalar paths don't panic inside `push_value`).
		// - Otherwise, promote all result types to a common supertype.
		let col_type = match return_type {
			Some(tc) => tc.get_type(),
			None => Type::super_type_of(results.iter().map(|v| v.get_type())),
		};

		let mut data = ColumnBuffer::with_capacity(col_type.clone(), row_count);
		for value in results {
			let coerced = self.coerce_value(value, &col_type)?;
			data.push_value(coerced);
		}
		let result_col = ColumnWithName::new(name.clone(), data);
		self.stack.push(Variable::columns(Columns::new(vec![result_col])));
		Ok(())
	}

	fn call_user_function(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		func_def: &CompiledFunction,
		args: Vec<Value>,
		_name: &Fragment,
	) -> Result<()> {
		let saved_ip = self.ip;
		self.symbols.enter_scope(ScopeType::Function);

		for (param, arg) in func_def.parameters.iter().zip(args.into_iter()) {
			let param_name = strip_dollar_prefix(param.name.text()).to_string();
			self.symbols.set(param_name.clone(), Variable::scalar_named(&param_name, arg), true)?;
		}

		self.ip = 0;
		let mut func_result = Vec::new();
		self.run_isolated_body(services, tx, &func_def.body, &mut func_result)?;

		let stack_value = collect_call_result(self, &mut func_result);
		self.ip = saved_ip;
		let _ = self.symbols.exit_scope();
		let coerced = self.coerce_return_value(stack_value, func_def.return_type.as_ref())?;
		self.stack.push(coerced);
		Ok(())
	}

	fn call_closure(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		closure: ClosureValue,
		args: Vec<Value>,
	) -> Result<()> {
		let saved_ip = self.ip;
		self.symbols.enter_scope(ScopeType::Function);

		for (name, var) in &closure.captured {
			self.symbols.set(name.clone(), var.clone(), true)?;
		}

		for (param, arg) in closure.def.parameters.iter().zip(args.into_iter()) {
			let param_name = strip_dollar_prefix(param.name.text()).to_string();
			self.symbols.set(param_name.clone(), Variable::scalar_named(&param_name, arg), true)?;
		}

		self.ip = 0;
		let mut closure_result = Vec::new();
		self.run_isolated_body(services, tx, &closure.def.body, &mut closure_result)?;

		let stack_value = collect_call_result(self, &mut closure_result);
		self.ip = saved_ip;
		let _ = self.symbols.exit_scope();
		self.stack.push(stack_value);
		Ok(())
	}

	fn call_local_procedure(
		&mut self,
		ctx: CallContext<'_, '_>,
		proc_def: &Procedure,
		args: Vec<Value>,
		name: &Fragment,
		func_name: &str,
	) -> Result<()> {
		// Enforce procedure call policy
		let (pol_ns, pol_name) = if let Some((ns, n)) = Catalog::split_qualified_name(func_name) {
			(ns, n.to_string())
		} else {
			("default".to_string(), func_name.to_string())
		};
		PolicyEvaluator::new(ctx.services, &self.symbols).enforce_identity_policy(
			ctx.tx,
			&pol_ns,
			&pol_name,
			CallableOp::Call,
			PolicyTargetType::Procedure,
		)?;

		match proc_def {
			Procedure::Native {
				native_name,
				..
			}
			| Procedure::Ffi {
				native_name,
				..
			}
			| Procedure::Wasm {
				native_name,
				..
			} => {
				let native_name = native_name.clone();
				if let Some(proc_impl) = ctx.services.procedures.get_procedure(&native_name) {
					let call_params = Params::Positional(Arc::new(args));
					let proc_ctx = ProcedureContext {
						params: &call_params,
						catalog: &ctx.services.catalog,
						functions: &ctx.services.functions,
						runtime_context: &ctx.services.runtime_context,
						ioc: &ctx.services.ioc,
					};
					let columns = proc_impl
						.call(&proc_ctx, ctx.tx)
						.map_err(|e| e.with_context(name.clone()))?;
					self.stack.push(Variable::columns(columns));
					Ok(())
				} else {
					Err(TypeError::Procedure {
						kind: ProcedureErrorKind::NoRegisteredImplementation {
							name: native_name.clone(),
						},
						message: format!(
							"native procedure '{}' has no registered implementation",
							native_name
						),
						fragment: name.clone(),
					}
					.into())
				}
			}
			Procedure::Rql {
				body,
				params,
				..
			}
			| Procedure::Test {
				body,
				params,
				..
			} => {
				// Catalog-stored RQL procedure
				let source = body.clone();
				let params = params.clone();
				let compiled = ctx.services.compiler.compile(ctx.tx, &source)?;
				match compiled {
					CompilationResult::Ready(compiled_list) => {
						self.execute_procedure_body(ctx, &compiled_list, &params, args, name)?;
						Ok(())
					}
					CompilationResult::Incremental(_) => Err(internal_error!(
						"Procedure body should not require incremental compilation"
					)),
				}
			}
		}
	}

	fn call_test_procedure(
		&mut self,
		ctx: CallContext<'_, '_>,
		proc_def: &Procedure,
		args: Vec<Value>,
		name: &Fragment,
		func_name: &str,
	) -> Result<()> {
		if !matches!(ctx.tx, Transaction::Test(..)) {
			return Err(TypeError::Procedure {
				kind: ProcedureErrorKind::UndefinedProcedure {
					name: func_name.to_string(),
				},
				message: format!("test procedure {} can only be called from test context", func_name),
				fragment: name.clone(),
			}
			.into());
		}

		let source = proc_def.body().unwrap_or_default().to_string();
		let params = proc_def.params().to_vec();
		let compiled = ctx.services.compiler.compile(ctx.tx, &source)?;
		match compiled {
			CompilationResult::Ready(compiled_list) => {
				self.execute_procedure_body(ctx, &compiled_list, &params, args, name)?;
				Ok(())
			}
			CompilationResult::Incremental(_) => {
				Err(internal_error!("Procedure body should not require incremental compilation"))
			}
		}
	}

	/// Shared logic for executing a compiled procedure body (used by both Local and Test procedures).
	fn execute_procedure_body(
		&mut self,
		ctx: CallContext<'_, '_>,
		compiled_list: &[Compiled],
		proc_params: &[ProcedureParam],
		args: Vec<Value>,
		_name: &Fragment,
	) -> Result<()> {
		let saved_ip = self.ip;
		self.symbols.enter_scope(ScopeType::Function);

		for (param_def, arg) in proc_params.iter().zip(args.into_iter()) {
			let bare_name = strip_dollar_prefix(&param_def.name);
			self.symbols.set(param_def.name.clone(), Variable::scalar_named(bare_name, arg), true)?;
		}

		let mut proc_result = Vec::new();
		for compiled in compiled_list.iter() {
			self.ip = 0;
			self.run(ctx.services, ctx.tx, &compiled.instructions, &mut proc_result)?;
			if !self.control_flow.is_normal() {
				break;
			}
		}

		let stack_value = collect_call_result(self, &mut proc_result);
		self.ip = saved_ip;
		let _ = self.symbols.exit_scope();
		self.stack.push(stack_value);
		Ok(())
	}

	#[cfg(not(reifydb_single_threaded))]
	fn call_remote_procedure(
		&mut self,
		services: &Arc<Services>,
		args: Vec<Value>,
		name: &Fragment,
		func_name: &str,
		address: &str,
		token: Option<&str>,
	) -> Result<()> {
		if let Some(ref registry) = services.remote_registry {
			let param_refs: Vec<String> = (1..=args.len()).map(|i| format!("${}", i)).collect();
			let remote_rql = format!("CALL {}({})", func_name, param_refs.join(", "));
			let frames = registry.forward_query(
				address,
				&remote_rql,
				Params::Positional(Arc::new(args)),
				token,
			)?;
			if let Some(frame) = frames.into_iter().next() {
				let cols: Columns = frame.into();
				self.stack.push(Variable::columns(cols));
			} else {
				self.stack.push(Variable::scalar(Value::none()));
			}
			Ok(())
		} else {
			Err(TypeError::Procedure {
				kind: ProcedureErrorKind::UndefinedProcedure {
					name: func_name.to_string(),
				},
				message: format!("Unknown procedure: {}", func_name),
				fragment: name.clone(),
			}
			.into())
		}
	}

	fn call_builtin_or_error(
		&mut self,
		ctx: CallContext<'_, '_>,
		args: Vec<Value>,
		name: &Fragment,
		func_name: &str,
		is_procedure_call: bool,
	) -> Result<()> {
		// Runtime-registered native procedure (no catalog entry needed)
		if let Some(proc_impl) = ctx.services.get_procedure(func_name) {
			let call_params = Params::Positional(Arc::new(args));
			let proc_ctx = ProcedureContext {
				params: &call_params,
				catalog: &ctx.services.catalog,
				functions: &ctx.services.functions,
				runtime_context: &ctx.services.runtime_context,
				ioc: &ctx.services.ioc,
			};
			let columns = proc_impl.call(&proc_ctx, ctx.tx).map_err(|e| e.with_context(name.clone()))?;

			// Special handling: identity::inject updates the transaction's identity
			if func_name == "identity::inject"
				&& let Some(col) = columns.first()
				&& let Value::IdentityId(id) = col.data().get_value(0)
			{
				ctx.tx.set_identity(id);
			}

			self.stack.push(Variable::columns(columns));
			return Ok(());
		}

		// Generator function
		if let Some(generator) = ctx.services.functions.get_generator(func_name) {
			let arg_columns: Vec<ColumnWithName> = args
				.into_iter()
				.enumerate()
				.map(|(i, v)| {
					let mut data = ColumnBuffer::with_capacity(v.get_type(), 1);
					data.push_value(v);
					ColumnWithName::new(format!("arg{}", i), data)
				})
				.collect();
			let columns_args = Columns::new(arg_columns);
			let identity = ctx.tx.identity();
			let fn_ctx = FunctionContext::new(
				name.clone(),
				&ctx.services.runtime_context,
				identity,
				columns_args.row_count(),
			);
			let columns = generator.call(&fn_ctx, &columns_args)?;
			self.stack.push(Variable::columns(columns));
			return Ok(());
		}

		// Procedure call to an unknown procedure
		if is_procedure_call {
			return Err(TypeError::Procedure {
				kind: ProcedureErrorKind::UndefinedProcedure {
					name: func_name.to_string(),
				},
				message: format!("Unknown procedure: {}", func_name),
				fragment: name.clone(),
			}
			.into());
		}

		let function = ctx.services.functions.get(func_name).ok_or_else(|| {
			ReifyError::from(EngineError::UnknownCallable {
				name: func_name.to_string(),
				fragment: name.clone(),
			})
		})?;

		let arg_columns: Vec<ColumnWithName> = args
			.into_iter()
			.enumerate()
			.map(|(i, v)| {
				let mut data = ColumnBuffer::with_capacity(v.get_type(), 1);
				data.push_value(v);
				ColumnWithName::new(format!("arg{}", i), data)
			})
			.collect();
		let columns_args = Columns::new(arg_columns);
		let identity = ctx.tx.identity();
		let fn_ctx = FunctionContext::new(name.clone(), &ctx.services.runtime_context, identity, 1);
		let result_columns = function.call(&fn_ctx, &columns_args).map_err(|e| e.with_context(name.clone()))?;
		let value =
			result_columns.into_iter().next().map(|col| col.data().get_value(0)).unwrap_or(Value::none());
		self.stack.push(Variable::scalar(value));
		Ok(())
	}

	pub(crate) fn exec_define_function(&mut self, node: &CompiledFunction) {
		let func_name = node.name.text().to_string();
		self.symbols.define_function(func_name, node.clone());
	}

	pub(crate) fn exec_return_value(&mut self) -> Result<()> {
		let cols = self.pop_as_columns()?;
		self.control_flow = ControlFlow::Return(Some(cols));
		Ok(())
	}

	pub(crate) fn exec_return_void(&mut self) {
		self.control_flow = ControlFlow::Return(None);
	}

	pub(crate) fn exec_define_closure(&mut self, closure_def: &CompiledClosure) {
		let mut captured = HashMap::new();
		for cap_name in &closure_def.captures {
			let stripped = strip_dollar_prefix(cap_name.text()).to_string();
			if let Some(var) = self.symbols.get(&stripped) {
				captured.insert(stripped, var.clone());
			}
		}
		self.stack.push(Variable::Closure(ClosureValue {
			def: closure_def.clone(),
			captured,
		}));
	}
}
