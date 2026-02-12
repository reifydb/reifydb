// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders};
use reifydb_rql::{
	expression::{CallExpression, Expression, IdentExpression},
	instruction::{Instruction, ScopeType},
	query::QueryPlan,
};
use reifydb_transaction::transaction::{AsTransaction, Transaction};
use reifydb_type::{
	params::Params,
	value::{Value, frame::frame::Frame},
};

use super::{
	scalar,
	services::Services,
	stack::{ControlFlow, Stack, SymbolTable, Variable},
	volcano::{
		compile::compile,
		query::{QueryContext, QueryNode},
	},
};
use crate::{
	arena::QueryArena,
	expression::{context::EvalContext, eval::evaluate},
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
	ip: usize,
	iteration_count: usize,
	stack: Stack,
	pub symbol_table: SymbolTable,
	pub control_flow: ControlFlow,
}

impl Vm {
	pub fn new(symbol_table: SymbolTable) -> Self {
		Self {
			ip: 0,
			iteration_count: 0,
			stack: Stack::new(),
			symbol_table,
			control_flow: ControlFlow::Normal,
		}
	}

	/// Pop a scalar Value from the stack. Works for Scalar(Columns) and
	/// 1x1 Columns variants.
	fn pop_value(&mut self) -> crate::Result<Value> {
		match self.stack.pop()? {
			Variable::Scalar(c) => Ok(c.scalar_value()),
			Variable::Columns(c) if c.len() == 1 && c.row_count() == 1 => Ok(c.scalar_value()),
			_ => Err(reifydb_type::error::Error(
				reifydb_core::error::diagnostic::internal::internal_with_context(
					"Expected scalar value on stack",
					file!(),
					line!(),
					column!(),
					module_path!(),
					module_path!(),
				),
			)),
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

				// === Stack ===
				Instruction::PushConst(value) => {
					self.stack.push(Variable::scalar(value.clone()));
				}
				Instruction::PushUndefined => {
					self.stack.push(Variable::scalar(Value::Undefined));
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

				// === Variables ===
				Instruction::LoadVar(fragment) => {
					let name = strip_dollar_prefix(fragment.text());
					match self.symbol_table.get(&name) {
						Some(Variable::Scalar(c)) => {
							self.stack.push(Variable::Scalar(c.clone()));
						}
						Some(Variable::Columns(_)) => {
							return Err(reifydb_type::error::Error(
								reifydb_type::error::diagnostic::runtime::variable_is_dataframe(&name),
							));
						}
						Some(Variable::ForIterator {
							..
						}) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Cannot load a FOR iterator as a value",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						None => {
							return Err(reifydb_type::error::Error(
								reifydb_type::error::diagnostic::runtime::variable_not_found(&name),
							));
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

				// === Arithmetic ===
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

				// === Unary ===
				Instruction::Negate => {
					let value = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_negate(value)?));
				}
				Instruction::LogicNot => {
					let value = self.pop_value()?;
					self.stack.push(Variable::scalar(scalar::scalar_not(&value)));
				}

				// === Comparison ===
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

				// === Logic ===
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

				// === Compound ===
				Instruction::Between => {
					let upper = self.pop_value()?;
					let lower = self.pop_value()?;
					let value = self.pop_value()?;
					let ge = scalar::scalar_ge(&value, &lower);
					let le = scalar::scalar_le(&value, &upper);
					let result = match (ge, le) {
						(Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(a && b),
						_ => Value::Undefined,
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
					let has_undefined = matches!(value, Value::Undefined)
						|| list_items.iter().any(|item| matches!(item, Value::Undefined));
					if has_undefined {
						self.stack.push(Variable::scalar(Value::Undefined));
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
					self.stack.push(Variable::scalar(scalar::scalar_cast(value, *target)?));
				}

				// === Control flow ===
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
					}
				}

				Instruction::Jump(addr) => {
					self.iteration_count += 1;
					if self.iteration_count > MAX_ITERATIONS {
						return Err(reifydb_type::error::Error(
							reifydb_type::error::diagnostic::runtime::max_iterations_exceeded(MAX_ITERATIONS),
						));
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

				// === Loops ===
				Instruction::ForInit {
					variable_name,
				} => {
					let columns = match self.stack.pop()? {
						Variable::Columns(c)
						| Variable::ForIterator {
							columns: c,
							..
						} => c,
						Variable::Scalar(_) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"ForInit expects Columns on data stack, got Scalar",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
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
							let mut data = ColumnData::undefined(0);
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

				// === Functions ===
				Instruction::DefineFunction(node) => {
					let func_name = node.name.text().to_string();
					self.symbol_table.define_function(func_name, node.clone());
				}

				Instruction::Call {
					name,
					arity,
				} => {
					let arity = *arity as usize;
					let func_name = name.text();

					// Pop arity args (right-to-left on stack), reverse to restore order
					let mut args = Vec::with_capacity(arity);
					for _ in 0..arity {
						args.push(self.pop_value()?);
					}
					args.reverse();

					if let Some(func_def) = self.symbol_table.get_function(func_name) {
						let func_def = func_def.clone();

						// Save IP
						let saved_ip = self.ip;

						// Enter function scope
						self.symbol_table.enter_scope(ScopeType::Function);

						// Bind params
						for (param, arg) in func_def.parameters.iter().zip(args.into_iter()) {
							let param_name = strip_dollar_prefix(param.name.text());
							self.symbol_table.set(
								param_name,
								Variable::scalar(arg),
								true,
							)?;
						}

						// Execute body recursively
						self.ip = 0;
						let mut func_result = Vec::new();
						self.run(services, tx, &func_def.body, params, &mut func_result)?;

						// Check for return value
						let stack_value = match std::mem::replace(
							&mut self.control_flow,
							ControlFlow::Normal,
						) {
							ControlFlow::Return(c) => Variable::Scalar(
								c.unwrap_or(Columns::scalar(Value::Undefined)),
							),
							_ => {
								// If no explicit return, check if function body emitted
								// a result via Emit
								if let Some(frame) = func_result.last() {
									if !frame.columns.is_empty()
										&& frame.columns[0].data.len() > 0
									{
										// Convert Frame back to Columns to
										// preserve column names
										let cols: Vec<Column> =
											frame.columns
												.iter()
												.map(|fc| {
													let mut data = ColumnData::undefined(0);
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
										Variable::scalar(Value::Undefined)
									}
								} else {
									// Check if anything was left on the stack by
									// the function body
									self.stack.pop().ok().unwrap_or(
										Variable::scalar(Value::Undefined),
									)
								}
							}
						};

						// Restore IP and exit scope
						self.ip = saved_ip;
						let _ = self.symbol_table.exit_scope();

						self.stack.push(stack_value);
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
							Value::Undefined
						};
						self.stack.push(Variable::scalar(value));
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

				// === DDL ===
				Instruction::CreateNamespace(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
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
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
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
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
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
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
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
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
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
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::create::transactional::create_transactional_view(services, txn, node.clone())?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateDictionary(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::create::dictionary::create_dictionary(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::CreateSubscription(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
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
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::alter::sequence::alter_table_sequence(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::AlterTable(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::alter::table::alter_table(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::AlterView(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::alter::view::execute_alter_view(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::AlterFlow(node) => {
					let txn = match tx {
						Transaction::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = super::instruction::ddl::alter::flow::execute_alter_flow(
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
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Mutation operations cannot be executed in a query transaction",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						_ => {}
					}
					let mut std_txn = tx.as_transaction();
					let columns = super::instruction::dml::table_delete::delete(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::DeleteRingBuffer(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Mutation operations cannot be executed in a query transaction",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						_ => {}
					}
					let mut std_txn = tx.as_transaction();
					let columns = super::instruction::dml::ringbuffer_delete::delete_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::InsertTable(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Mutation operations cannot be executed in a query transaction",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						_ => {}
					}
					let mut std_txn = tx.as_transaction();
					let columns = super::instruction::dml::table_insert::insert_table(
						services,
						&mut std_txn,
						node.clone(),
						&mut self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::InsertRingBuffer(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Mutation operations cannot be executed in a query transaction",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						_ => {}
					}
					let mut std_txn = tx.as_transaction();
					let columns = super::instruction::dml::ringbuffer_insert::insert_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::InsertDictionary(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Mutation operations cannot be executed in a query transaction",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						_ => {}
					}
					let mut std_txn = tx.as_transaction();
					let columns = super::instruction::dml::dictionary_insert::insert_dictionary(
						services,
						&mut std_txn,
						node.clone(),
						&mut self.symbol_table,
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::Update(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Mutation operations cannot be executed in a query transaction",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						_ => {}
					}
					let mut std_txn = tx.as_transaction();
					let columns = super::instruction::dml::table_update::update_table(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}
				Instruction::UpdateRingBuffer(node) => {
					match tx {
						Transaction::Query(_) => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"Mutation operations cannot be executed in a query transaction",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
							));
						}
						_ => {}
					}
					let mut std_txn = tx.as_transaction();
					let columns = super::instruction::dml::ringbuffer_update::update_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(Variable::Columns(columns));
				}

				// === Query ===
				Instruction::Query(plan) => {
					let mut std_txn = tx.as_transaction();
					if let Some(columns) = run_query_plan(
						services,
						&mut std_txn,
						plan.clone(),
						params.clone(),
						&mut self.symbol_table,
					)? {
						self.stack.push(Variable::Columns(columns));
					}
				}

				// === Append ===
				Instruction::Append {
					target,
				} => {
					let clean_name = strip_dollar_prefix(target.text());
					let columns = match self.stack.pop()? {
						Variable::Columns(cols) => cols,
						_ => {
							return Err(reifydb_type::error::Error(
								reifydb_core::error::diagnostic::internal::internal_with_context(
									"APPEND requires columns/frame data on stack",
									file!(), line!(), column!(), module_path!(), module_path!(),
								),
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
							// Auto-create the Columns variable
							self.symbol_table.set(
								clean_name,
								Variable::Columns(columns),
								true,
							)?;
						}
						_ => {
							return Err(reifydb_type::error::Error(
								reifydb_type::error::diagnostic::runtime::append_target_not_frame(&clean_name),
							));
						}
					}
				}
			}

			self.ip += 1;

			// Propagate non-normal control flow
			if !self.control_flow.is_normal() {
				return Ok(());
			}
		}
		Ok(())
	}
}

fn value_to_expression(value: &Value) -> Expression {
	use reifydb_rql::expression::ConstantExpression;
	use reifydb_type::fragment::Fragment;
	match value {
		Value::Undefined => Expression::Constant(ConstantExpression::Undefined {
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

/// Run a query plan and return the result columns.
fn run_query_plan(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: QueryPlan,
	params: Params,
	symbol_table: &mut SymbolTable,
) -> crate::Result<Option<Columns>> {
	let context = Arc::new(QueryContext {
		services: services.clone(),
		source: None,
		batch_size: 1024,
		params,
		stack: symbol_table.clone(),
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
				data: ColumnData::undefined(0),
			})
			.collect();
		return Ok(Some(Columns::new(empty_columns)));
	}

	Ok(all_columns)
}
