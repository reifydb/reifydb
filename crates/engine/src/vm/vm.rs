// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::policy::PolicyTargetType,
	internal_error,
	value::column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders},
};
use reifydb_rql::{
	compiler::CompilationResult,
	expression::{CallExpression, ConstantExpression, Expression, IdentExpression},
	instruction::{Instruction, ScopeType},
	query::QueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::{ProcedureErrorKind, RuntimeErrorKind, TypeError},
	fragment::Fragment,
	params::Params,
	value::{Value, frame::frame::Frame, identity::IdentityId, r#type::Type},
};

use super::{
	scalar,
	services::Services,
	stack::{ClosureValue, ControlFlow, Stack, SymbolTable, Variable},
	volcano::{
		compile::compile,
		query::{QueryContext, QueryNode},
	},
};
use crate::{
	arena::QueryArena,
	expression::{context::EvalContext, eval::evaluate},
	vm::instruction::{
		ddl::{
			alter::policy::alter_policy,
			create::{
				authentication::create_authentication, event::create_event, policy::create_policy,
				role::create_role, user::create_user,
			},
			drop::{
				authentication::drop_authentication, policy::drop_policy, role::drop_role,
				user::drop_user,
			},
			grant::grant,
			revoke::revoke,
		},
		dml::dispatch::dispatch,
	},
};

const MAX_ITERATIONS: usize = 10_000;

fn strip_dollar_prefix(name: &str) -> String {
	if name.starts_with('$') {
		name[1..].to_string()
	} else {
		name.to_string()
	}
}

pub struct Vm {
	pub(crate) ip: usize,
	iteration_count: usize,
	stack: Stack,
	pub symbol_table: SymbolTable,
	pub control_flow: ControlFlow,
	pub(crate) dispatch_depth: u8,
	pub(crate) identity: IdentityId,
}

impl Vm {
	pub fn new(symbol_table: SymbolTable, identity: IdentityId) -> Self {
		Self {
			ip: 0,
			iteration_count: 0,
			stack: Stack::new(),
			symbol_table,
			control_flow: ControlFlow::Normal,
			dispatch_depth: 0,
			identity,
		}
	}

	/// Pop a scalar Value from the stack. Works for Scalar(Columns) and
	/// 1x1 Columns variants.
	fn pop_value(&mut self) -> crate::Result<Value> {
		match self.stack.pop()? {
			Variable::Scalar(c) => Ok(c.scalar_value()),
			Variable::Columns(c) if c.len() == 1 && c.row_count() == 1 => Ok(c.scalar_value()),
			_ => Err(internal_error!("Expected scalar value on stack")),
		}
	}

	/// Pop the top of stack as Columns. Works for any variant.
	fn pop_as_columns(&mut self) -> crate::Result<Columns> {
		match self.stack.pop()? {
			Variable::Scalar(c)
			| Variable::Columns(c)
			| Variable::ForIterator {
				columns: c,
				..
			} => Ok(c),
			Variable::Closure(_) => Ok(Columns::scalar(Value::none())),
		}
	}

	pub(crate) fn run(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		instructions: &[Instruction],
		params: &Params,
		result: &mut Vec<Frame>,
	) -> crate::Result<()> {
		while self.ip < instructions.len() {
			match &instructions[self.ip] {
				Instruction::Halt => return Ok(()),
				Instruction::Nop => {}

				Instruction::PushConst(value) => {
					self.stack.push(Variable::scalar(value.clone()));
				}
				Instruction::PushNone => {
					self.stack.push(Variable::scalar(Value::none()));
				}
				Instruction::Pop => {
					self.stack.pop()?;
				}
				Instruction::Dup => {
					let value = self.stack.pop()?;
					let cloned = value.clone();
					self.stack.push(value);
					self.stack.push(cloned);
				}

				Instruction::LoadVar(fragment) => {
					let name = strip_dollar_prefix(fragment.text());
					match self.symbol_table.get(&name) {
						Some(Variable::Scalar(c)) => {
							self.stack.push(Variable::Scalar(c.clone()));
						}
						Some(Variable::Closure(c)) => {
							self.stack.push(Variable::Closure(c.clone()));
						}
						Some(Variable::Columns(_)) => {
							return Err(TypeError::Runtime {
								kind: RuntimeErrorKind::VariableIsDataframe {
									name: name.to_string(),
								},
								message: format!(
									"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
									name
								),
							}
							.into());
						}
						Some(Variable::ForIterator {
							..
						}) => {
							return Err(internal_error!(
								"Cannot load a FOR iterator as a value"
							));
						}
						None => {
							return Err(TypeError::Runtime {
								kind: RuntimeErrorKind::VariableNotFound {
									name: name.to_string(),
								},
								message: format!("Variable '{}' is not defined", name),
							}
							.into());
						}
					}
				}
				Instruction::StoreVar(fragment) => {
					let name = strip_dollar_prefix(fragment.text());
					let value = self.pop_value()?;
					self.symbol_table.reassign(name, Variable::scalar(value))?;
				}
				Instruction::DeclareVar(fragment) => {
					let name = strip_dollar_prefix(fragment.text());
					let sv = self.stack.pop()?;
					let variable = match sv {
						Variable::Scalar(c) => Variable::Scalar(c),
						Variable::Closure(c) => Variable::Closure(c),
						Variable::Columns(c)
						| Variable::ForIterator {
							columns: c,
							..
						} => {
							if c.len() == 1 && c.row_count() == 1 {
								Variable::Scalar(c)
							} else {
								Variable::Columns(c)
							}
						}
					};
					self.symbol_table.set(name, variable, true)?;
				}
				Instruction::FieldAccess {
					object,
					field,
				} => {
					let var_name = strip_dollar_prefix(object.text());
					let field_name = field.text();
					match self.symbol_table.get(&var_name) {
						Some(Variable::Columns(columns)) => {
							let col = columns
								.columns
								.iter()
								.find(|c| c.name.text() == field_name);
							match col {
								Some(col) => {
									let value = col.data.get_value(0);
									self.stack.push(Variable::scalar(value));
								}
								None => {
									let available: Vec<String> = columns
										.columns
										.iter()
										.map(|c| c.name.text().to_string())
										.collect();
									return Err(TypeError::Runtime {
										kind: RuntimeErrorKind::FieldNotFound {
											variable: var_name.to_string(),
											field: field_name.to_string(),
											available: available.clone(),
										},
										message: format!(
											"Field '{}' not found on variable '{}'",
											field_name, var_name
										),
									}
									.into());
								}
							}
						}
						Some(Variable::Scalar(_)) | Some(Variable::Closure(_)) => {
							return Err(TypeError::Runtime {
								kind: RuntimeErrorKind::FieldNotFound {
									variable: var_name.to_string(),
									field: field_name.to_string(),
									available: vec![],
								},
								message: format!(
									"Field '{}' not found on variable '{}'",
									field_name, var_name
								),
							}
							.into());
						}
						Some(Variable::ForIterator {
							..
						}) => {
							return Err(TypeError::Runtime {
								kind: RuntimeErrorKind::VariableIsDataframe {
									name: var_name.to_string(),
								},
								message: format!(
									"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
									var_name
								),
							}
							.into());
						}
						None => {
							return Err(TypeError::Runtime {
								kind: RuntimeErrorKind::VariableNotFound {
									name: var_name.to_string(),
								},
								message: format!(
									"Variable '{}' is not defined",
									var_name
								),
							}
							.into());
						}
					}
				}

				Instruction::Add => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_add(left, right)?));
				}
				Instruction::Sub => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_sub(left, right)?));
				}
				Instruction::Mul => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_mul(left, right)?));
				}
				Instruction::Div => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_div(left, right)?));
				}
				Instruction::Rem => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_rem(left, right)?));
				}

				Instruction::Negate => {
					let value = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_negate(value)?));
				}
				Instruction::LogicNot => {
					let value = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_not(&value)));
				}

				Instruction::CmpEq => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_eq(&left, &right)));
				}
				Instruction::CmpNe => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_ne(&left, &right)));
				}
				Instruction::CmpLt => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_lt(&left, &right)));
				}
				Instruction::CmpLe => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_le(&left, &right)));
				}
				Instruction::CmpGt => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_gt(&left, &right)));
				}
				Instruction::CmpGe => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_ge(&left, &right)));
				}

				Instruction::LogicAnd => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_and(&left, &right)));
				}
				Instruction::LogicOr => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_or(&left, &right)));
				}
				Instruction::LogicXor => {
					let right = self.pop_value()?;
					let left = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_xor(&left, &right)));
				}

				Instruction::Between => {
					let upper = self.pop_value()?;
					let lower = self.pop_value()?;
					let value = self.pop_value()?;
					let ge = scalar::scalar_ge(&value, &lower);
					let le = scalar::scalar_le(&value, &upper);
					let result = match (ge, le) {
						(Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(a && b),
						_ => Value::none(),
					};
					self.stack.push(Variable::scalar(result));
				}
				Instruction::InList {
					count,
					negated,
				} => {
					let count = *count as usize;
					let negated = *negated;
					let mut list_items = Vec::with_capacity(count);
					for _ in 0..count {
						list_items.push(self.pop_value()?);
					}
					list_items.reverse();
					let value = self.pop_value()?;
					let has_undefined = matches!(value, Value::None { .. })
						|| list_items.iter().any(|item| matches!(item, Value::None { .. }));
					if has_undefined {
						self.stack.push(Variable::scalar(Value::none()));
					} else {
						let found = list_items.iter().any(|item| {
							matches!(scalar::scalar_eq(&value, item), Value::Boolean(true))
						});
						let result = if negated {
							!found
						} else {
							found
						};
						self.stack.push(Variable::scalar(Value::Boolean(result)));
					}
				}
				Instruction::Cast(target) => {
					let value = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_cast(value, target.clone())?));
				}

				Instruction::Emit => {
					let Some(value) = self.stack.pop().ok() else {
						self.ip += 1;
						continue;
					};
					match value {
						Variable::Columns(c)
						| Variable::ForIterator {
							columns: c,
							..
						} => {
							result.push(Frame::from(c));
						}
						Variable::Scalar(c) => {
							result.push(Frame::from(c));
						}
						Variable::Closure(_) => {
							result.push(Frame::from(Columns::scalar(Value::none())));
						}
					}
				}

				Instruction::Jump(addr) => {
					self.iteration_count += 1;
					if self.iteration_count > MAX_ITERATIONS {
						return Err(TypeError::Runtime {
							kind: RuntimeErrorKind::MaxIterationsExceeded {
								limit: MAX_ITERATIONS,
							},
							message: format!(
								"Loop exceeded maximum iteration limit of {}",
								MAX_ITERATIONS
							),
						}
						.into());
					}
					self.ip = *addr;
					continue;
				}
				Instruction::JumpIfFalsePop(addr) => {
					let value = self.pop_value()?;
					if !scalar::value_is_truthy(&value) {
						self.ip = *addr;
						continue;
					}
				}
				Instruction::JumpIfTruePop(addr) => {
					let value = self.pop_value()?;
					if scalar::value_is_truthy(&value) {
						self.ip = *addr;
						continue;
					}
				}
				Instruction::EnterScope(scope_type) => {
					self.symbol_table.enter_scope(scope_type.clone());
				}
				Instruction::ExitScope => {
					self.symbol_table.exit_scope()?;
				}
				Instruction::Break {
					exit_scopes,
					addr,
				} => {
					for _ in 0..*exit_scopes {
						self.symbol_table.exit_scope()?;
					}
					self.ip = *addr;
					continue;
				}
				Instruction::Continue {
					exit_scopes,
					addr,
				} => {
					for _ in 0..*exit_scopes {
						self.symbol_table.exit_scope()?;
					}
					self.ip = *addr;
					continue;
				}

				Instruction::ForInit {
					variable_name,
				} => {
					let columns = match self.stack.pop()? {
						Variable::Columns(c)
						| Variable::ForIterator {
							columns: c,
							..
						} => c,
						Variable::Scalar(_) | Variable::Closure(_) => {
							return Err(internal_error!(
								"ForInit expects Columns on data stack, got Scalar"
							));
						}
					};
					let var_name = variable_name.text();
					let iter_key = format!("__for_{}", var_name);
					self.symbol_table.set(
						iter_key,
						Variable::ForIterator {
							columns,
							index: 0,
						},
						true,
					)?;
				}
				Instruction::ForNext {
					variable_name,
					addr,
				} => {
					let var_name = variable_name.text();
					let clean_name = if var_name.starts_with('$') {
						&var_name[1..]
					} else {
						var_name
					};
					let iter_key = format!("__for_{}", var_name);

					let (columns, index) = match self.symbol_table.get(&iter_key) {
						Some(Variable::ForIterator {
							columns,
							index,
						}) => (columns.clone(), *index),
						_ => {
							self.ip = *addr;
							continue;
						}
					};

					if index >= columns.row_count() {
						self.ip = *addr;
						continue;
					}

					if columns.len() == 1 {
						let value = columns.columns[0].data.get_value(index);
						self.symbol_table.set(
							clean_name.to_string(),
							Variable::scalar(value),
							true,
						)?;
					} else {
						let mut row_columns = Vec::new();
						for col in columns.columns.iter() {
							let value = col.data.get_value(index);
							let mut data = ColumnData::none_typed(Type::Boolean, 0);
							data.push_value(value);
							row_columns.push(Column::new(col.name.clone(), data));
						}
						let row_frame = Columns::new(row_columns);
						self.symbol_table.set(
							clean_name.to_string(),
							Variable::Columns(row_frame),
							true,
						)?;
					}

					self.symbol_table.reassign(
						iter_key,
						Variable::ForIterator {
							columns,
							index: index + 1,
						},
					)?;
				}

				Instruction::DefineFunction(node) => {
					let func_name = node.name.text().to_string();
					self.symbol_table.define_function(func_name, node.clone());
				}

				Instruction::Call {
					name,
					arity,
					is_procedure_call,
				} => {
					let arity = *arity as usize;
					let is_procedure_call = *is_procedure_call;
					let func_name = name.text();

					let mut args = Vec::with_capacity(arity);
					for _ in 0..arity {
						args.push(self.pop_value()?);
					}
					args.reverse();

					if let Some(func_def) = self.symbol_table.get_function(func_name) {
						let func_def = func_def.clone();

						let saved_ip = self.ip;

						self.symbol_table.enter_scope(ScopeType::Function);

						for (param, arg) in func_def.parameters.iter().zip(args.into_iter()) {
							let param_name = strip_dollar_prefix(param.name.text());
							self.symbol_table.set(
								param_name,
								Variable::scalar(arg),
								true,
							)?;
						}

						self.ip = 0;
						let mut func_result = Vec::new();
						self.run(services, tx, &func_def.body, params, &mut func_result)?;

						let stack_value = match std::mem::replace(
							&mut self.control_flow,
							ControlFlow::Normal,
						) {
							ControlFlow::Return(c) => Variable::Scalar(
								c.unwrap_or(Columns::scalar(Value::none())),
							),
							_ => {
								if let Some(frame) = func_result.last() {
									if !frame.columns.is_empty()
										&& frame.columns[0].data.len() > 0
									{
										let cols: Vec<Column> =
											frame.columns
												.iter()
												.map(|fc| {
													let mut data = ColumnData::none_typed(Type::Boolean, 0);
													for i in 0..fc
														.data
														.len()
													{
														data.push_value(fc.data.get_value(i));
													}
													Column::new(fc.name.as_str(), data)
												})
												.collect();
										Variable::Columns(Columns::new(cols))
									} else {
										Variable::scalar(Value::none())
									}
								} else {
									self.stack.pop().ok().unwrap_or(
										Variable::scalar(Value::none()),
									)
								}
							}
						};

						self.ip = saved_ip;
						let _ = self.symbol_table.exit_scope();

						self.stack.push(stack_value);
					} else if let Some(Variable::Closure(closure_val)) =
						self.symbol_table.get(&strip_dollar_prefix(func_name)).cloned()
					{
						let saved_ip = self.ip;

						self.symbol_table.enter_scope(ScopeType::Function);

						for (name, var) in &closure_val.captured {
							self.symbol_table.set(name.clone(), var.clone(), true)?;
						}

						for (param, arg) in
							closure_val.def.parameters.iter().zip(args.into_iter())
						{
							let param_name = strip_dollar_prefix(param.name.text());
							self.symbol_table.set(
								param_name,
								Variable::scalar(arg),
								true,
							)?;
						}

						self.ip = 0;
						let mut closure_result = Vec::new();
						self.run(
							services,
							tx,
							&closure_val.def.body,
							params,
							&mut closure_result,
						)?;

						let stack_value = match std::mem::replace(
							&mut self.control_flow,
							ControlFlow::Normal,
						) {
							ControlFlow::Return(c) => Variable::Scalar(
								c.unwrap_or(Columns::scalar(Value::none())),
							),
							_ => {
								if let Some(frame) = closure_result.last() {
									if !frame.columns.is_empty()
										&& frame.columns[0].data.len() > 0
									{
										let cols: Vec<Column> =
											frame.columns
												.iter()
												.map(|fc| {
													let mut data = ColumnData::none_typed(Type::Boolean, 0);
													for i in 0..fc
														.data
														.len()
													{
														data.push_value(fc.data.get_value(i));
													}
													Column::new(fc.name.as_str(), data)
												})
												.collect();
										Variable::Columns(Columns::new(cols))
									} else {
										Variable::scalar(Value::none())
									}
								} else {
									self.stack.pop().ok().unwrap_or(
										Variable::scalar(Value::none()),
									)
								}
							}
						};

						self.ip = saved_ip;
						let _ = self.symbol_table.exit_scope();

						self.stack.push(stack_value);
					} else {
						// Check catalog for stored procedures before falling back to built-in
						// functions
						let proc_def = {
							let mut tx_tmp = tx.reborrow();
							services.catalog.find_procedure_by_qualified_name(
								&mut tx_tmp,
								func_name,
							)?
						};

						if let Some(proc_def) = proc_def {
							// Enforce procedure call policy
							let (pol_ns, pol_name) = if let Some((ns, name)) =
								Catalog::split_qualified_name(func_name)
							{
								(ns, name.to_string())
							} else {
								("default".to_string(), func_name.to_string())
							};
							crate::policy::enforce_identity_policy(
								services,
								tx,
								self.identity,
								&pol_ns,
								&pol_name,
								"call",
								PolicyTargetType::Procedure,
								&self.symbol_table,
							)?;

							// Catalog-stored RQL procedure
							let source = proc_def.body.clone();
							let compiled = services.compiler.compile(tx, &source)?;
							match compiled {
								CompilationResult::Ready(compiled_list) => {
									// Save IP
									let saved_ip = self.ip;

									// Enter function scope
									self.symbol_table
										.enter_scope(ScopeType::Function);

									// Bind procedure params to call
									// args
									for (param_def, arg) in proc_def
										.params
										.iter()
										.zip(args.into_iter())
									{
										self.symbol_table.set(
											param_def.name.clone(),
											Variable::scalar(arg),
											true,
										)?;
									}

									// Execute compiled instructions
									let mut proc_result = Vec::new();
									for compiled in compiled_list.iter() {
										self.ip = 0;
										self.run(
											services,
											tx,
											&compiled.instructions,
											params,
											&mut proc_result,
										)?;
										if !self.control_flow.is_normal() {
											break;
										}
									}

									// Collect result (same pattern
									// as DEF functions)
									let stack_value = match std::mem::replace(
										&mut self.control_flow,
										ControlFlow::Normal,
									) {
										ControlFlow::Return(c) => {
											Variable::Scalar(c.unwrap_or(
												Columns::scalar(
													Value::none(),
												),
											))
										}
										_ => {
											if let Some(frame) =
												proc_result.last()
											{
												if !frame
													.columns
													.is_empty() && frame
													.columns[0]
													.data
													.len()
													> 0
												{
													let cols: Vec<Column> =
														frame.columns
															.iter()
															.map(|fc| {
																let mut data = ColumnData::none_typed(Type::Boolean, 0);
																for i in 0..fc.data.len() {
																	data.push_value(fc.data.get_value(i));
																}
																Column::new(fc.name.as_str(), data)
															})
															.collect();
													Variable::Columns(Columns::new(cols))
												} else {
													Variable::scalar(Value::none())
												}
											} else {
												self.stack.pop().ok().unwrap_or(
													Variable::scalar(Value::none()),
												)
											}
										}
									};

									// Restore IP and exit scope
									self.ip = saved_ip;
									let _ = self.symbol_table.exit_scope();

									self.stack.push(stack_value);
								}
								CompilationResult::Incremental(_) => {
									return Err(internal_error!(
										"Procedure body should not require incremental compilation"
									));
								}
							}
						} else if let Some(proc_impl) =
							services.procedures.get_procedure(func_name)
						{
							// Runtime-registered native procedure (no catalog entry needed)
							let call_params = Params::Positional(args);
							let identity = self.identity;
							let executor = crate::vm::executor::Executor::from_services(
								services.clone(),
							);
							let ctx = crate::procedure::context::ProcedureContext {
								identity,
								params: &call_params,
								catalog: &services.catalog,
								functions: &services.functions,
								clock: &services.clock,
								executor: &executor,
							};
							let columns = proc_impl.call(&ctx, tx)?;

							// Special handling: identity::inject updates the VM's identity
							if func_name == "identity::inject" {
								if let Some(col) = columns.get(0) {
									if let Value::IdentityId(id) =
										col.data().get_value(0)
									{
										self.identity = id;
									}
								}
							}

							self.stack.push(Variable::Columns(columns));
						} else if is_procedure_call {
							return Err(TypeError::Procedure {
								kind: ProcedureErrorKind::UndefinedProcedure {
									name: func_name.to_string(),
								},
								message: format!("Unknown procedure: {}", func_name),
								fragment: name.clone(),
							}
							.into());
						} else {
							// Built-in function: evaluate via column evaluator
							let evaluation_context = EvalContext {
								target: None,
								columns: Columns::empty(),
								row_count: 1,
								take: None,
								params,
								symbol_table: &self.symbol_table,
								is_aggregate_context: false,
								functions: &services.functions,
								clock: &services.clock,
								arena: None,
								identity: self.identity,
							};

							let mut arg_exprs = Vec::with_capacity(arity);
							for arg in &args {
								arg_exprs.push(value_to_expression(arg));
							}

							let proper_call = Expression::Call(CallExpression {
								func: IdentExpression(name.clone()),
								args: arg_exprs,
								fragment: name.clone(),
							});

							let result_column = evaluate(
								&evaluation_context,
								&proper_call,
								&services.functions,
								&services.clock,
							)?;
							let value = if result_column.data.len() > 0 {
								result_column.data.get_value(0)
							} else {
								Value::none()
							};
							self.stack.push(Variable::scalar(value));
						}
					}
				}

				Instruction::ReturnValue => {
					let cols = self.pop_as_columns()?;
					self.control_flow = ControlFlow::Return(Some(cols));
					return Ok(());
				}
				Instruction::ReturnVoid => {
					self.control_flow = ControlFlow::Return(None);
					return Ok(());
				}

				Instruction::DefineClosure(closure_def) => {
					let mut captured = HashMap::new();
					for cap_name in &closure_def.captures {
						let stripped = strip_dollar_prefix(cap_name.text());
						if let Some(var) = self.symbol_table.get(&stripped) {
							captured.insert(stripped, var.clone());
						}
					}
					self.stack.push(Variable::Closure(ClosureValue {
						def: closure_def.clone(),
						captured,
					}));
				}

				Instruction::CreateNamespace(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::namespace::create_namespace(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateTable(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::table::create_table(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateRingBuffer(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::ringbuffer::create_ringbuffer(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateFlow(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::flow::create_flow(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateDeferredView(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::deferred::create_deferred_view(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateTransactionalView(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::transactional::create_transactional_view(services, txn, node.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateDictionary(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::dictionary::create_dictionary(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateSumType(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::sumtype::create_sumtype(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateSubscription(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns =
						super::instruction::ddl::create::subscription::create_subscription(
							services,
							txn,
							node.clone(),
						)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::AlterSequence(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::alter::sequence::alter_table_sequence(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreatePrimaryKey(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::primary_key::create_primary_key(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateColumnProperty(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns =
						super::instruction::ddl::create::property::create_column_property(
							services,
							txn,
							node.clone(),
						)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateProcedure(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::procedure::create_procedure(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateSeries(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::series::create_series(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateEvent(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = create_event(services, txn, node.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateTag(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::tag::create_tag(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}

				Instruction::CreateMigration(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::create::migration::create_migration(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::Migrate(node) => {
					let columns = super::instruction::ddl::migrate::migrate::execute_migrate(
						self,
						services,
						tx,
						node.clone(),
						params,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::RollbackMigration(node) => {
					let columns =
						super::instruction::ddl::migrate::rollback::execute_rollback_migration(
							self,
							services,
							tx,
							node.clone(),
							params,
						)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::Dispatch(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"DISPATCH requires a command or admin transaction"
							));
						}
						_ => {}
					}
					let depth = self.dispatch_depth;
					self.dispatch_depth += 1;
					let columns = dispatch(self, services, tx, node.clone(), params, depth)?;
					self.dispatch_depth -= 1;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::AlterFlow(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::alter::flow::execute_alter_flow(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}

				Instruction::AlterTable(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = super::instruction::ddl::alter::table::execute_alter_table(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}

				// === DDL (Drop) ===
				Instruction::DropNamespace(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::namespace::drop_namespace(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropTable(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::table::drop_table(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropView(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::view::drop_view(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropRingBuffer(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::ringbuffer::drop_ringbuffer(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropSeries(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::series::drop_series(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropDictionary(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::dictionary::drop_dictionary(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropSumType(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::sumtype::drop_sumtype(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropFlow(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::flow::drop_flow(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropSubscription(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::drop::subscription::drop_subscription(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}

				// === DML ===
				Instruction::Delete(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::table_delete::delete(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DeleteRingBuffer(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::ringbuffer_delete::delete_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::InsertTable(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::table_insert::insert_table(
						services,
						&mut std_txn,
						node.clone(),
						&mut self.symbol_table,
						self.identity,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::InsertRingBuffer(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::ringbuffer_insert::insert_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::InsertDictionary(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::dictionary_insert::insert_dictionary(
						services,
						&mut std_txn,
						node.clone(),
						&mut self.symbol_table,
						self.identity,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::InsertSeries(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::series_insert::insert_series(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DeleteSeries(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::series_delete::delete_series(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::Update(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::table_update::update_table(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::UpdateRingBuffer(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::ringbuffer_update::update_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::UpdateSeries(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(internal_error!(
								"Mutation operations cannot be executed in a query transaction"
							));
						}
						_ => {}
					}
					let mut std_txn = tx.reborrow();
					let columns = super::instruction::dml::series_update::update_series(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
						self.identity,
						&self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}

				Instruction::Query(plan) => {
					let mut std_txn = tx.reborrow();
					if let Some(columns) = run_query_plan(
						services,
						&mut std_txn,
						plan.clone(),
						params.clone(),
						&mut self.symbol_table,
						self.identity,
					)? {
						self.stack.push(Variable::Columns(columns));
					}
				}

				Instruction::Append {
					target,
				} => {
					let clean_name = strip_dollar_prefix(target.text());
					let columns = match self.stack.pop()? {
						Variable::Columns(cols) => cols,
						_ => {
							return Err(internal_error!(
								"APPEND requires columns/frame data on stack"
							));
						}
					};

					match self.symbol_table.get(&clean_name) {
						Some(Variable::Columns(_)) => {
							let mut existing =
								match self.symbol_table.get(&clean_name).unwrap() {
									Variable::Columns(f) => f.clone(),
									_ => unreachable!(),
								};
							existing.append_columns(columns)?;
							self.symbol_table
								.reassign(clean_name, Variable::Columns(existing))?;
						}
						None => {
							self.symbol_table.set(
								clean_name,
								Variable::Columns(columns),
								true,
							)?;
						}
						_ => {
							return Err(TypeError::Runtime {
								kind: RuntimeErrorKind::AppendTargetNotFrame {
									name: clean_name.to_string(),
								},
								message: format!(
									"Cannot APPEND to variable '{}' because it is not a Frame",
									clean_name
								),
							}
							.into());
						}
					}
				}

				// Auth/Permissions
				Instruction::CreateUser(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = create_user(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateRole(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = create_role(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::Grant(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = grant(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::Revoke(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = revoke(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropUser(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = drop_user(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropRole(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = drop_role(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreatePolicy(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = create_policy(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::AlterPolicy(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = alter_policy(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropPolicy(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = drop_policy(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateAuthentication(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = create_authentication(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DropAuthentication(plan) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => {
							return Err(internal_error!(
								"DDL operations require an admin transaction"
							));
						}
					};
					let columns = drop_authentication(services, txn, plan.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
			}

			self.ip += 1;

			if !self.control_flow.is_normal() {
				return Ok(());
			}
		}
		Ok(())
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
			fragment: Fragment::internal(&format!("{}", value)),
		}),
	}
}

fn run_query_plan(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: QueryPlan,
	params: Params,
	symbol_table: &mut SymbolTable,
	identity: IdentityId,
) -> crate::Result<Option<Columns>> {
	let context = Arc::new(QueryContext {
		services: services.clone(),
		source: None,
		batch_size: 1024,
		params,
		stack: symbol_table.clone(),
		identity,
	});

	let mut query_node = compile(plan, txn, context.clone());
	query_node.initialize(txn, &context)?;

	let mut all_columns: Option<Columns> = None;
	let mut mutable_context = (*context).clone();
	let mut arena = QueryArena::new();

	while let Some(batch) = query_node.next(txn, &mut mutable_context)? {
		match &mut all_columns {
			None => all_columns = Some(batch),
			Some(existing) => existing.append_columns(batch)?,
		}
		arena.reset();
	}

	if all_columns.is_none() {
		let headers = query_node.headers().unwrap_or_else(ColumnHeaders::empty);
		let empty_columns: Vec<Column> = headers
			.columns
			.into_iter()
			.map(|name| Column {
				name,
				data: ColumnData::none_typed(Type::Boolean, 0),
			})
			.collect();
		return Ok(Some(Columns::new(empty_columns)));
	}

	Ok(all_columns)
}
