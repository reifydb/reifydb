// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::{
	Transaction, admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction,
};
use reifydb_type::{params::Params, value::frame::frame::Frame};

use crate::{
	evaluate::{ColumnEvaluationContext, column::evaluate},
	execute::Executor,
	instruction::Instruction,
	stack::{Stack, Variable},
};

const MAX_ITERATIONS: usize = 10_000;

pub(crate) enum TransactionAccess<'a> {
	Admin(&'a mut AdminTransaction),
	Command(&'a mut CommandTransaction),
	Query(&'a mut QueryTransaction),
}

impl<'a> TransactionAccess<'a> {
	fn as_transaction(&mut self) -> Transaction<'_> {
		match self {
			TransactionAccess::Admin(txn) => Transaction::from(&mut **txn),
			TransactionAccess::Command(txn) => Transaction::from(&mut **txn),
			TransactionAccess::Query(txn) => Transaction::from(&mut **txn),
		}
	}
}

impl Executor {
	pub(crate) fn interpret(
		&self,
		tx: &mut TransactionAccess<'_>,
		instructions: &[Instruction],
		params: &Params,
		stack: &mut Stack,
		result: &mut Vec<Frame>,
	) -> crate::Result<()> {
		let mut ip = 0;
		let mut iteration_count: usize = 0;

		while ip < instructions.len() {
			match &instructions[ip] {
				Instruction::Halt => return Ok(()),
				Instruction::Nop => {}

				// === Linearized Control Flow ===
				Instruction::Jump(addr) => {
					iteration_count += 1;
					if iteration_count > MAX_ITERATIONS {
						return Err(reifydb_type::error::Error(
							reifydb_type::error::diagnostic::runtime::max_iterations_exceeded(MAX_ITERATIONS),
						));
					}
					ip = *addr;
					continue;
				}
				Instruction::JumpIfFalse {
					condition,
					addr,
				} => {
					if !self.evaluate_condition(condition, params, stack)? {
						ip = *addr;
						continue;
					}
				}
				Instruction::EnterScope(scope_type) => {
					stack.enter_scope(scope_type.clone());
				}
				Instruction::ExitScope => {
					stack.exit_scope()?;
				}
				Instruction::Break {
					exit_scopes,
					addr,
				} => {
					for _ in 0..*exit_scopes {
						stack.exit_scope()?;
					}
					ip = *addr;
					continue;
				}
				Instruction::Continue {
					exit_scopes,
					addr,
				} => {
					for _ in 0..*exit_scopes {
						stack.exit_scope()?;
					}
					ip = *addr;
					continue;
				}

				// === FOR loop support ===
				Instruction::ForInit {
					variable_name,
					iterable,
				} => {
					let mut std_txn = tx.as_transaction();
					let iterable_cols =
						self.query(&mut std_txn, iterable.clone(), params.clone(), stack)?;

					let columns = iterable_cols.unwrap_or_else(Columns::empty);
					let var_name = variable_name.text();
					let iter_key = format!("__for_{}", var_name);
					stack.set(
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

					// Get iterator state
					let (columns, index) = match stack.get(&iter_key) {
						Some(Variable::ForIterator {
							columns,
							index,
						}) => (columns.clone(), *index),
						_ => {
							ip = *addr;
							continue;
						}
					};

					if index >= columns.row_count() {
						ip = *addr;
						continue;
					}

					// Bind current row to variable
					if columns.len() == 1 {
						let value = columns.columns[0].data.get_value(index);
						stack.set(clean_name.to_string(), Variable::Scalar(value), true)?;
					} else {
						let mut row_columns = Vec::new();
						for col in columns.columns.iter() {
							let value = col.data.get_value(index);
							let mut data = ColumnData::undefined(0);
							data.push_value(value);
							row_columns.push(Column::new(col.name.clone(), data));
						}
						let row_frame = Columns::new(row_columns);
						stack.set(clean_name.to_string(), Variable::Frame(row_frame), true)?;
					}

					// Increment index
					stack.reassign(
						iter_key,
						Variable::ForIterator {
							columns,
							index: index + 1,
						},
					)?;
				}

				// === DDL ===
				Instruction::CreateNamespace(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_namespace(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::CreateTable(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_table(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::CreateRingBuffer(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_ringbuffer(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::CreateFlow(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_flow(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::CreateDeferredView(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_deferred_view(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::CreateTransactionalView(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_transactional_view(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::CreateDictionary(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_dictionary(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::CreateSubscription(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.create_subscription(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::AlterSequence(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.alter_table_sequence(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::AlterTable(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.alter_table(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::AlterView(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.execute_alter_view(txn, node.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::AlterFlow(node) => {
					let txn = match tx {
						TransactionAccess::Admin(txn) => txn,
						_ => return Err(reifydb_type::error::Error(
							reifydb_core::error::diagnostic::internal::internal_with_context(
								"DDL operations require an admin transaction",
								file!(), line!(), column!(), module_path!(), module_path!(),
							),
						)),
					};
					let columns = self.execute_alter_flow(txn, node.clone())?;
					result.push(Frame::from(columns));
				}

				// === DML ===
				Instruction::Delete(node) => {
					match tx {
						TransactionAccess::Query(_) => {
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
					let columns = self.delete(&mut std_txn, node.clone(), params.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::DeleteRingBuffer(node) => {
					match tx {
						TransactionAccess::Query(_) => {
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
					let columns =
						self.delete_ringbuffer(&mut std_txn, node.clone(), params.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::InsertTable(node) => {
					match tx {
						TransactionAccess::Query(_) => {
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
					let columns = self.insert_table(&mut std_txn, node.clone(), stack)?;
					result.push(Frame::from(columns));
				}
				Instruction::InsertRingBuffer(node) => {
					match tx {
						TransactionAccess::Query(_) => {
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
					let columns =
						self.insert_ringbuffer(&mut std_txn, node.clone(), params.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::InsertDictionary(node) => {
					match tx {
						TransactionAccess::Query(_) => {
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
					let columns = self.insert_dictionary(&mut std_txn, node.clone(), stack)?;
					result.push(Frame::from(columns));
				}
				Instruction::Update(node) => {
					match tx {
						TransactionAccess::Query(_) => {
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
					let columns = self.update_table(&mut std_txn, node.clone(), params.clone())?;
					result.push(Frame::from(columns));
				}
				Instruction::UpdateRingBuffer(node) => {
					match tx {
						TransactionAccess::Query(_) => {
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
					let columns =
						self.update_ringbuffer(&mut std_txn, node.clone(), params.clone())?;
					result.push(Frame::from(columns));
				}

				// === Query ===
				Instruction::Query(plan) => {
					let mut std_txn = tx.as_transaction();
					if let Some(columns) =
						self.query(&mut std_txn, plan.clone(), params.clone(), stack)?
					{
						result.push(Frame::from(columns));
					}
				}

				// === Variables ===
				Instruction::Declare(node) => {
					let plan = reifydb_rql::plan::physical::PhysicalPlan::Declare(node.clone());
					let mut std_txn = tx.as_transaction();
					self.query(&mut std_txn, plan, params.clone(), stack)?;
				}
				Instruction::Assign(node) => {
					let plan = reifydb_rql::plan::physical::PhysicalPlan::Assign(node.clone());
					let mut std_txn = tx.as_transaction();
					self.query(&mut std_txn, plan, params.clone(), stack)?;
				}
			}
			ip += 1;
		}
		Ok(())
	}

	fn evaluate_condition(&self, condition: &Expression, params: &Params, stack: &Stack) -> crate::Result<bool> {
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
