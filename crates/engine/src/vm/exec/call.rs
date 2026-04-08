// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use reifydb_catalog::catalog::{Catalog, procedure::ResolvedProcedure};
use reifydb_core::{
	interface::catalog::{
		policy::PolicyTargetType,
		procedure::{Procedure, ProcedureParam, ProcedureTrigger},
	},
	internal_error,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_routine::{function::FunctionContext, procedure::context::ProcedureContext};
use reifydb_rql::{
	compiler::{CompilationResult, Compiled},
	expression::{CallExpression, ConstantExpression, Expression, IdentExpression},
	instruction::{CompiledClosure, CompiledFunction, ScopeType},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::{ProcedureErrorKind, TypeError},
	fragment::Fragment,
	params::Params,
	value::{Value, frame::frame::Frame},
};

use super::stack::strip_dollar_prefix;
use crate::{
	Result,
	expression::{context::EvalSession, eval::evaluate},
	policy::PolicyEvaluator,
	vm::{
		services::Services,
		stack::{ClosureValue, ControlFlow, Variable},
		vm::Vm,
	},
};

/// Groups the shared (services, tx, params) triple passed to call-family methods.
pub(crate) struct CallContext<'a, 'b> {
	pub services: &'a Arc<Services>,
	pub tx: &'a mut Transaction<'b>,
	pub params: &'a Params,
}

/// Collect the return value from a function/procedure/closure execution.
/// This pattern was previously copy-pasted 4 times in the Call handler.
fn collect_call_result(vm: &mut Vm, func_result: &mut Vec<Frame>) -> Variable {
	match mem::replace(&mut vm.control_flow, ControlFlow::Normal) {
		ControlFlow::Return(c) => Variable::Scalar(c.unwrap_or(Columns::scalar(Value::none()))),
		_ => {
			if let Some(frame) = func_result.pop() {
				if !frame.columns.is_empty() && !frame.columns[0].data.is_empty() {
					Variable::Columns(frame.into())
				} else {
					Variable::scalar(Value::none())
				}
			} else {
				vm.stack.pop().ok().unwrap_or(Variable::scalar(Value::none()))
			}
		}
	}
}

impl Vm {
	pub(crate) fn exec_call(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		params: &Params,
		name: &Fragment,
		arity: u8,
		is_procedure_call: bool,
	) -> Result<()> {
		let arity = arity as usize;
		let func_name = name.text();

		// testing:: prefix reserved for future use

		let mut args = Vec::with_capacity(arity);
		for _ in 0..arity {
			args.push(self.pop_value()?);
		}
		args.reverse();

		// 1. User-defined function (DEF)
		if let Some(func_def) = self.symbols.get_function(func_name).cloned() {
			return self.call_user_function(services, tx, params, &func_def, args, name);
		}

		// 2. Closure variable
		if let Some(closure_val) = self.symbols.get(strip_dollar_prefix(func_name)).cloned()
			&& let Variable::Closure(closure) = closure_val
		{
			return self.call_closure(services, tx, params, closure, args);
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
					params,
				};
				self.call_local_procedure(ctx, &proc_def, args, name, func_name)
			}
			Some(ResolvedProcedure::Test(proc_def)) => {
				let ctx = CallContext {
					services,
					tx,
					params,
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
					params,
				};
				self.call_builtin_or_error(ctx, args, name, func_name, is_procedure_call, arity)
			}
		}
	}

	fn call_user_function(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		params: &Params,
		func_def: &CompiledFunction,
		args: Vec<Value>,
		_name: &Fragment,
	) -> Result<()> {
		let saved_ip = self.ip;
		self.symbols.enter_scope(ScopeType::Function);

		for (param, arg) in func_def.parameters.iter().zip(args.into_iter()) {
			let param_name = strip_dollar_prefix(param.name.text()).to_string();
			self.symbols.set(param_name, Variable::scalar(arg), true)?;
		}

		self.ip = 0;
		let mut func_result = Vec::new();
		self.run(services, tx, &func_def.body, params, &mut func_result)?;

		let stack_value = collect_call_result(self, &mut func_result);
		self.ip = saved_ip;
		let _ = self.symbols.exit_scope();
		self.stack.push(stack_value);
		Ok(())
	}

	fn call_closure(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		params: &Params,
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
			self.symbols.set(param_name, Variable::scalar(arg), true)?;
		}

		self.ip = 0;
		let mut closure_result = Vec::new();
		self.run(services, tx, &closure.def.body, params, &mut closure_result)?;

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
			"call",
			PolicyTargetType::Procedure,
		)?;

		match &proc_def.trigger {
			ProcedureTrigger::NativeCall {
				native_name,
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
					self.stack.push(Variable::Columns(columns));
					Ok(())
				} else {
					Err(internal_error!(
						"NativeCall procedure '{}' has no registered implementation",
						native_name
					))
				}
			}
			_ => {
				// Catalog-stored RQL procedure
				let source = proc_def.body.clone();
				let compiled = ctx.services.compiler.compile(ctx.tx, &source)?;
				match compiled {
					CompilationResult::Ready(compiled_list) => {
						self.execute_procedure_body(
							ctx,
							&compiled_list,
							&proc_def.params,
							args,
							name,
						)?;
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

		let source = proc_def.body.clone();
		let compiled = ctx.services.compiler.compile(ctx.tx, &source)?;
		match compiled {
			CompilationResult::Ready(compiled_list) => {
				self.execute_procedure_body(ctx, &compiled_list, &proc_def.params, args, name)?;
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
			self.symbols.set(param_def.name.clone(), Variable::scalar(arg), true)?;
		}

		let mut proc_result = Vec::new();
		for compiled in compiled_list.iter() {
			self.ip = 0;
			self.run(ctx.services, ctx.tx, &compiled.instructions, ctx.params, &mut proc_result)?;
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
				self.stack.push(Variable::Columns(cols));
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
		arity: usize,
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

			self.stack.push(Variable::Columns(columns));
			return Ok(());
		}

		// Generator function
		if let Some(generator) = ctx.services.functions.get_generator(func_name) {
			let arg_columns: Vec<Column> = args
				.into_iter()
				.enumerate()
				.map(|(i, v)| {
					let mut data = ColumnData::with_capacity(v.get_type(), 1);
					data.push_value(v);
					Column::new(format!("arg{}", i), data)
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
			self.stack.push(Variable::Columns(columns));
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

		// Built-in function: evaluate via column evaluator
		let vm_session = EvalSession {
			params: ctx.params,
			symbols: &self.symbols,
			functions: &ctx.services.functions,
			runtime_context: &ctx.services.runtime_context,
			arena: None,
			identity: ctx.tx.identity(),
			is_aggregate_context: false,
		};
		let evaluation_context = vm_session.eval_empty();

		let mut arg_exprs = Vec::with_capacity(arity);
		for arg in &args {
			arg_exprs.push(value_to_expression(arg));
		}

		let proper_call = Expression::Call(CallExpression {
			func: IdentExpression(name.clone()),
			args: arg_exprs,
			fragment: name.clone(),
		});

		let result_column = evaluate(&evaluation_context, &proper_call)?;
		let value = if !result_column.data.is_empty() {
			result_column.data.get_value(0)
		} else {
			Value::none()
		};
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

fn value_to_expression(value: &Value) -> Expression {
	match value {
		Value::None {
			..
		} => Expression::Constant(ConstantExpression::None {
			fragment: Fragment::None,
		}),
		Value::Boolean(b) => Expression::Constant(ConstantExpression::Bool {
			fragment: Fragment::internal(if *b {
				"true"
			} else {
				"false"
			}),
		}),
		Value::Utf8(s) => Expression::Constant(ConstantExpression::Text {
			fragment: Fragment::internal(s),
		}),
		_ => Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::internal(format!("{}", value)),
		}),
	}
}
