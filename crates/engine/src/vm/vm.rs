// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders};
use reifydb_rql::{
	expression::{CallExpression, Expression, IdentExpression},
	instruction::Instruction,
	nodes::{AssignValue, LetValue, PhysicalPlan},
	query::QueryPlan,
};
use reifydb_type::{params::Params, value::frame::frame::Frame};

use super::{
	interpret::TransactionAccess,
	services::Services,
	stack::{ControlFlow, ScopeType, Stack, StackValue, SymbolTable, Variable},
	volcano::{
		compile::compile,
		query::{QueryContext, QueryNode},
	},
};
use crate::evaluate::{ColumnEvaluationContext, column::evaluate};

const MAX_ITERATIONS: usize = 10_000;

/// Convert a PhysicalPlan to a QueryPlan for query operations.
/// This is used when PhysicalPlans are stored in LetValue/AssignValue::Statement.
fn physical_to_query_plan(plan: PhysicalPlan) -> Option<QueryPlan> {
	match plan {
		PhysicalPlan::TableScan(n) => Some(QueryPlan::TableScan(n)),
		PhysicalPlan::TableVirtualScan(n) => Some(QueryPlan::TableVirtualScan(n)),
		PhysicalPlan::ViewScan(n) => Some(QueryPlan::ViewScan(n)),
		PhysicalPlan::RingBufferScan(n) => Some(QueryPlan::RingBufferScan(n)),
		PhysicalPlan::FlowScan(n) => Some(QueryPlan::FlowScan(n)),
		PhysicalPlan::DictionaryScan(n) => Some(QueryPlan::DictionaryScan(n)),
		PhysicalPlan::IndexScan(n) => Some(QueryPlan::IndexScan(n)),
		PhysicalPlan::RowPointLookup(n) => Some(QueryPlan::RowPointLookup(n)),
		PhysicalPlan::RowListLookup(n) => Some(QueryPlan::RowListLookup(n)),
		PhysicalPlan::RowRangeScan(n) => Some(QueryPlan::RowRangeScan(n)),
		PhysicalPlan::Aggregate(n) => Some(QueryPlan::Aggregate(n)),
		PhysicalPlan::Distinct(n) => Some(QueryPlan::Distinct(n)),
		PhysicalPlan::Filter(n) => Some(QueryPlan::Filter(n)),
		PhysicalPlan::JoinInner(n) => Some(QueryPlan::JoinInner(n)),
		PhysicalPlan::JoinLeft(n) => Some(QueryPlan::JoinLeft(n)),
		PhysicalPlan::JoinNatural(n) => Some(QueryPlan::JoinNatural(n)),
		PhysicalPlan::Merge(n) => Some(QueryPlan::Merge(n)),
		PhysicalPlan::Take(n) => Some(QueryPlan::Take(n)),
		PhysicalPlan::Sort(n) => Some(QueryPlan::Sort(n)),
		PhysicalPlan::Map(n) => Some(QueryPlan::Map(n)),
		PhysicalPlan::Extend(n) => Some(QueryPlan::Extend(n)),
		PhysicalPlan::Patch(n) => Some(QueryPlan::Patch(n)),
		PhysicalPlan::Apply(n) => Some(QueryPlan::Apply(n)),
		PhysicalPlan::InlineData(n) => Some(QueryPlan::InlineData(n)),
		PhysicalPlan::Generator(n) => Some(QueryPlan::Generator(n)),
		PhysicalPlan::Window(n) => Some(QueryPlan::Window(n)),
		PhysicalPlan::Variable(n) => Some(QueryPlan::Variable(n)),
		PhysicalPlan::Environment(n) => Some(QueryPlan::Environment(n)),
		PhysicalPlan::Scalarize(n) => Some(QueryPlan::Scalarize(n)),
		// Non-query plans return None
		_ => None,
	}
}

fn strip_dollar_prefix(name: &str) -> String {
	if name.starts_with('$') {
		name[1..].to_string()
	} else {
		name.to_string()
	}
}

fn columns_to_variable(columns: &Columns) -> Variable {
	if columns.len() == 1 && columns.row_count() == 1 {
		if let Some(first_column) = columns.iter().next() {
			if let Some(first_value) = first_column.data().iter().next() {
				return Variable::scalar(first_value);
			}
		}
	}
	Variable::frame(columns.clone())
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

	pub(crate) fn run(
		&mut self,
		services: &Arc<Services>,
		tx: &mut TransactionAccess<'_>,
		instructions: &[Instruction],
		params: &Params,
		result: &mut Vec<Frame>,
	) -> crate::Result<()> {
		while self.ip < instructions.len() {
			match &instructions[self.ip] {
				Instruction::Halt => return Ok(()),
				Instruction::Nop => {}

				Instruction::Emit => {
					let Some(value) = self.stack.pop().ok() else {
						self.ip += 1;
						continue;
					};
					match value {
						StackValue::Columns(c) => result.push(Frame::from(c)),
						StackValue::Scalar(v) => {
							let mut data = ColumnData::undefined(0);
							data.push_value(v);
							let col = Column::new("value", data);
							result.push(Frame::from(Columns::new(vec![col])));
						}
					}
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
				Instruction::EvalCondition(condition) => {
					let result = self.evaluate_condition(services, condition, params)?;
					self.stack
						.push(StackValue::Scalar(reifydb_type::value::Value::Boolean(result)));
				}
				Instruction::JumpIfFalsePop(addr) => {
					let value = self.stack.pop()?;
					let is_false = match value {
						StackValue::Scalar(reifydb_type::value::Value::Boolean(false)) => true,
						StackValue::Scalar(reifydb_type::value::Value::Boolean(true)) => false,
						_ => true,
					};
					if is_false {
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
						StackValue::Columns(c) => c,
						StackValue::Scalar(_) => {
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

					// Get iterator state
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

					// Bind current row to variable
					if columns.len() == 1 {
						let value = columns.columns[0].data.get_value(index);
						self.symbol_table.set(
							clean_name.to_string(),
							Variable::Scalar(value),
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
							Variable::Frame(row_frame),
							true,
						)?;
					}

					// Increment index
					self.symbol_table.reassign(
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
					let columns = super::instruction::ddl::create::namespace::create_namespace(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::create::table::create_table(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::create::ringbuffer::create_ringbuffer(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::create::flow::create_flow(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::create::deferred::create_deferred_view(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::create::transactional::create_transactional_view(services, txn, node.clone())?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::create::dictionary::create_dictionary(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns =
						super::instruction::ddl::create::subscription::create_subscription(
							services,
							txn,
							node.clone(),
						)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::alter::sequence::alter_table_sequence(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::alter::table::alter_table(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::alter::view::execute_alter_view(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::ddl::alter::flow::execute_alter_flow(
						services,
						txn,
						node.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::dml::table_delete::delete(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::dml::ringbuffer_delete::delete_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::dml::table_insert::insert_table(
						services,
						&mut std_txn,
						node.clone(),
						&mut self.symbol_table,
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::dml::ringbuffer_insert::insert_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::dml::dictionary_insert::insert_dictionary(
						services,
						&mut std_txn,
						node.clone(),
						&mut self.symbol_table,
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::dml::table_update::update_table(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
					let columns = super::instruction::dml::ringbuffer_update::update_ringbuffer(
						services,
						&mut std_txn,
						node.clone(),
						params.clone(),
					)?;
					self.stack.push(StackValue::Columns(columns));
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
						self.stack.push(StackValue::Columns(columns));
					}
				}

				// === Variables ===
				Instruction::Declare(node) => {
					let name = strip_dollar_prefix(node.name.text());
					let columns = self.evaluate_let_value(&node.value, services, tx, params)?;
					let variable = columns_to_variable(&columns);
					self.symbol_table.set(name, variable, true)?;
				}
				Instruction::Assign(node) => {
					let name = strip_dollar_prefix(node.name.text());
					let columns = self.evaluate_assign_value(&node.value, services, tx, params)?;
					let variable = columns_to_variable(&columns);
					self.symbol_table.reassign(name, variable)?;
				}

				// === User-defined functions ===
				Instruction::DefineFunction(node) => {
					// Register the function in the symbol table
					let func_name = node.name.text().to_string();
					self.symbol_table.define_function(func_name, node.clone());
				}

				Instruction::CallFunction(node) => {
					// Look up the function in the symbol table
					let func_name = node.name.text();
					if let Some(func_def) = self.symbol_table.get_function(func_name) {
						// Clone the function definition to avoid borrow issues
						let func_def = func_def.clone();

						// Create a new scope for the function call
						self.symbol_table.enter_scope(ScopeType::Function);

						// Bind arguments to parameters
						for (param, arg) in
							func_def.parameters.iter().zip(node.arguments.iter())
						{
							let param_name = strip_dollar_prefix(param.name.text());
							// Evaluate the argument
							let evaluation_context = ColumnEvaluationContext {
								target: None,
								columns: Columns::empty(),
								row_count: 1,
								take: None,
								params,
								symbol_table: &self.symbol_table,
								is_aggregate_context: false,
							};
							let result_column = evaluate(
								&evaluation_context,
								arg,
								&services.functions,
							)?;
							// Get the first value from the column (or undefined if empty)
							let value = if result_column.data.len() > 0 {
								result_column.data.get_value(0)
							} else {
								reifydb_type::value::Value::Undefined
							};
							self.symbol_table.set(
								param_name,
								Variable::Scalar(value),
								true,
							)?;
						}

						// Function body is already pre-compiled
						let body_instructions = &func_def.body;

						// Execute the function body instructions
						let mut body_ip = 0;
						while body_ip < body_instructions.len() {
							match &body_instructions[body_ip] {
								Instruction::Halt => break,
								Instruction::Return(ret_node) => {
									if let Some(ref expr) = ret_node.value {
										let evaluation_context =
											ColumnEvaluationContext {
												target: None,
												columns: Columns::empty(
												),
												row_count: 1,
												take: None,
												params,
												symbol_table: &self
													.symbol_table,
												is_aggregate_context:
													false,
											};
										let result_column = evaluate(
											&evaluation_context,
											expr,
											&services.functions,
										)?;
										let value = if result_column.data.len()
											> 0
										{
											result_column.data.get_value(0)
										} else {
											reifydb_type::value::Value::Undefined
										};
										let columns = Columns::single_row([(
											"value", value,
										)]);
										self.stack.push(StackValue::Columns(
											columns,
										));
									}
									break;
								}
								Instruction::Query(plan) => {
									let mut std_txn = tx.as_transaction();
									if let Some(columns) = run_query_plan(
										services,
										&mut std_txn,
										plan.clone(),
										params.clone(),
										&mut self.symbol_table,
									)? {
										self.stack.push(StackValue::Columns(
											columns,
										));
									}
								}
								Instruction::Emit => {
									// Emit is handled - result is already on stack
								}
								Instruction::EvalCondition(expr) => {
									let result = self.evaluate_condition(
										services, expr, params,
									)?;
									self.stack.push(StackValue::Scalar(
										reifydb_type::value::Value::Boolean(
											result,
										),
									));
								}
								Instruction::JumpIfFalsePop(addr) => {
									let value = self.stack.pop()?;
									let is_false = match value {
										StackValue::Scalar(reifydb_type::value::Value::Boolean(false)) => {
											true
										}
										StackValue::Scalar(reifydb_type::value::Value::Boolean(true)) => {
											false
										}
										_ => true,
									};
									if is_false {
										body_ip = *addr;
										continue;
									}
								}
								Instruction::Jump(addr) => {
									body_ip = *addr;
									continue;
								}
								Instruction::EnterScope(scope_type) => {
									self.symbol_table
										.enter_scope(scope_type.clone());
								}
								Instruction::ExitScope => {
									let _ = self.symbol_table.exit_scope();
								}
								Instruction::Nop => {}
								_ => {
									// Handle other instructions as needed
								}
							}
							body_ip += 1;
						}

						// Exit the function scope
						let _ = self.symbol_table.exit_scope();
					} else {
						// User-defined function not found - try as built-in function
						let call_expr = Expression::Call(CallExpression {
							func: IdentExpression(node.name.clone()),
							args: node.arguments.clone(),
							fragment: node.name.clone(),
						});

						let evaluation_context = ColumnEvaluationContext {
							target: None,
							columns: Columns::empty(),
							row_count: 1,
							take: None,
							params,
							symbol_table: &self.symbol_table,
							is_aggregate_context: false,
						};

						let result_column =
							evaluate(&evaluation_context, &call_expr, &services.functions)?;
						let value = if result_column.data.len() > 0 {
							result_column.data.get_value(0)
						} else {
							reifydb_type::value::Value::Undefined
						};
						let columns = Columns::single_row([("value", value)]);
						self.stack.push(StackValue::Columns(columns));
					}
				}

				Instruction::Return(node) => {
					// Return is handled within function call execution
					// If we encounter it at the top level, just evaluate and push result
					if let Some(ref expr) = node.value {
						let evaluation_context = ColumnEvaluationContext {
							target: None,
							columns: Columns::empty(),
							row_count: 1,
							take: None,
							params,
							symbol_table: &self.symbol_table,
							is_aggregate_context: false,
						};
						let result_column =
							evaluate(&evaluation_context, expr, &services.functions)?;
						// Get the first value
						let value = if result_column.data.len() > 0 {
							result_column.data.get_value(0)
						} else {
							reifydb_type::value::Value::Undefined
						};
						let columns = Columns::single_row([("value", value)]);
						self.stack.push(StackValue::Columns(columns));
					}
				}
			}
			self.ip += 1;
		}
		Ok(())
	}

	fn evaluate_let_value(
		&mut self,
		value: &LetValue,
		services: &Arc<Services>,
		tx: &mut TransactionAccess<'_>,
		params: &Params,
	) -> crate::Result<Columns> {
		match value {
			LetValue::Expression(expr) => {
				let evaluation_context = ColumnEvaluationContext {
					target: None,
					columns: Columns::empty(),
					row_count: 1,
					take: None,
					params,
					symbol_table: &self.symbol_table,
					is_aggregate_context: false,
				};
				let result_column = evaluate(&evaluation_context, expr, &services.functions)?;
				Ok(Columns::new(vec![result_column]))
			}
			LetValue::Statement(physical_plans) => {
				if physical_plans.is_empty() {
					return Ok(Columns::empty());
				}
				let last_plan = physical_plans.last().unwrap();
				let query_plan = physical_to_query_plan(last_plan.clone())
					.expect("LetValue::Statement should contain query plans");
				let mut std_txn = tx.as_transaction();
				let result = run_query_plan(
					services,
					&mut std_txn,
					query_plan,
					params.clone(),
					&mut self.symbol_table,
				)?;
				Ok(result.unwrap_or_else(Columns::empty))
			}
		}
	}

	fn evaluate_assign_value(
		&mut self,
		value: &AssignValue,
		services: &Arc<Services>,
		tx: &mut TransactionAccess<'_>,
		params: &Params,
	) -> crate::Result<Columns> {
		match value {
			AssignValue::Expression(expr) => {
				let evaluation_context = ColumnEvaluationContext {
					target: None,
					columns: Columns::empty(),
					row_count: 1,
					take: None,
					params,
					symbol_table: &self.symbol_table,
					is_aggregate_context: false,
				};
				let result_column = evaluate(&evaluation_context, expr, &services.functions)?;
				Ok(Columns::new(vec![result_column]))
			}
			AssignValue::Statement(physical_plans) => {
				if physical_plans.is_empty() {
					return Ok(Columns::empty());
				}
				let last_plan = physical_plans.last().unwrap();
				let query_plan = physical_to_query_plan(last_plan.clone())
					.expect("AssignValue::Statement should contain query plans");
				let mut std_txn = tx.as_transaction();
				let result = run_query_plan(
					services,
					&mut std_txn,
					query_plan,
					params.clone(),
					&mut self.symbol_table,
				)?;
				Ok(result.unwrap_or_else(Columns::empty))
			}
		}
	}

	fn evaluate_condition(
		&self,
		services: &Arc<Services>,
		condition: &Expression,
		params: &Params,
	) -> crate::Result<bool> {
		let evaluation_context = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params,
			symbol_table: &self.symbol_table,
			is_aggregate_context: false,
		};

		let result_column = evaluate(&evaluation_context, condition, &services.functions)?;

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

/// Run a query plan and return the result columns.
/// This is a standalone function that can be called from anywhere with access to Services.
fn run_query_plan(
	services: &Arc<Services>,
	txn: &mut reifydb_transaction::transaction::Transaction<'_>,
	plan: QueryPlan,
	params: Params,
	symbol_table: &mut SymbolTable,
) -> crate::Result<Option<Columns>> {
	// Convert QueryPlan to PhysicalPlan for the volcano executor
	let physical_plan: PhysicalPlan = plan.into();
	let context = Arc::new(QueryContext {
		services: services.clone(),
		source: None,
		batch_size: 1024,
		params,
		stack: symbol_table.clone(),
	});

	let mut query_node = compile(physical_plan, txn, context.clone());

	// Initialize the operator
	query_node.initialize(txn, &context)?;

	// Collect all results
	let mut all_columns: Option<Columns> = None;
	let mut mutable_context = (*context).clone();

	while let Some(batch) = query_node.next(txn, &mut mutable_context)? {
		match &mut all_columns {
			None => all_columns = Some(batch),
			Some(existing) => existing.append_columns(batch)?,
		}
	}

	// If no results were collected, return empty columns with the proper headers
	// This ensures queries on empty tables return a frame with 0 rows instead of no frame
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
