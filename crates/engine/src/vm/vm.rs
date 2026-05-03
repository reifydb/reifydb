// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::instruction::{Instruction, ScopeType};
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	params::Params,
	util::bitvec::BitVec,
	value::{Value, frame::frame::Frame, identity::IdentityId},
};

use super::{
	exec::{
		mask::{LoopMaskState, MaskFrame, extract_bool_bitvec},
		stack::strip_dollar_prefix,
	},
	instruction::{
		ddl::{
			alter::{
				remote_namespace::alter_remote_namespace, sequence::alter_table_sequence,
				table::execute_alter_table,
			},
			create::{
				binding::create_binding, deferred::create_deferred_view, dictionary::create_dictionary,
				migration::create_migration, namespace::create_namespace,
				primary_key::create_primary_key, procedure::create_procedure,
				property::create_column_property, remote_namespace::create_remote_namespace,
				ringbuffer::create_ringbuffer, series::create_series, sink::create_sink,
				source::create_source, subscription::create_subscription, sumtype::create_sumtype,
				table::create_table, tag::create_tag, test::create_test,
				transactional::create_transactional_view,
			},
			drop::{
				binding::drop_binding, dictionary::drop_dictionary, namespace::drop_namespace,
				procedure::drop_procedure, ringbuffer::drop_ringbuffer, series::drop_series,
				sink::drop_sink, source::drop_source, subscription::drop_subscription,
				sumtype::drop_sumtype, table::drop_table, view::drop_view,
			},
		},
		dml::{
			dictionary_insert::insert_dictionary, ringbuffer_delete::delete_ringbuffer,
			ringbuffer_insert::insert_ringbuffer, ringbuffer_update::update_ringbuffer,
			series_delete::delete_series, series_insert::insert_series, series_update::update_series,
			table_delete::delete, table_insert::insert_table, table_update::update_table,
		},
	},
	services::Services,
	stack::{ControlFlow, Stack, SymbolTable, Variable},
};
use crate::{
	Result,
	expression::context::EvalContext,
	vm::instruction::ddl::{
		alter::policy::alter_policy,
		create::{
			authentication::create_authentication, event::create_event, identity::create_identity,
			policy::create_policy, role::create_role,
		},
		drop::{
			authentication::drop_authentication, handler::drop_handler, identity::drop_identity,
			policy::drop_policy, role::drop_role, test::drop_test,
		},
		grant::grant,
		revoke::revoke,
	},
};

pub static EMPTY_PARAMS: LazyLock<Params> = LazyLock::new(|| Params::None);

pub struct Vm<'a> {
	pub(crate) ip: usize,
	pub(crate) iteration_count: usize,
	pub(crate) stack: Stack,
	pub symbols: SymbolTable,
	pub control_flow: ControlFlow,
	pub(crate) dispatch_depth: u8,

	pub(crate) batch_size: usize,

	pub(crate) active_mask: Option<BitVec>,

	pub(crate) mask_stack: Vec<MaskFrame>,

	pub(crate) loop_mask_stack: Vec<LoopMaskState>,

	pub(crate) params: &'a Params,
	pub(crate) routines: &'a Routines,
	pub(crate) runtime_context: &'a RuntimeContext,
	pub(crate) identity: IdentityId,
}

impl<'a> Vm<'a> {
	pub fn from_services(
		symbols: SymbolTable,
		services: &'a Services,
		params: &'a Params,
		identity: IdentityId,
	) -> Self {
		Self::build(symbols, 1, params, &services.routines, &services.runtime_context, identity)
	}

	pub fn with_batch_size_from_services(
		symbols: SymbolTable,
		batch_size: usize,
		services: &'a Services,
		params: &'a Params,
		identity: IdentityId,
	) -> Self {
		Self::build(symbols, batch_size, params, &services.routines, &services.runtime_context, identity)
	}

	fn build(
		symbols: SymbolTable,
		batch_size: usize,
		params: &'a Params,
		routines: &'a Routines,
		runtime_context: &'a RuntimeContext,
		identity: IdentityId,
	) -> Self {
		Self {
			ip: 0,
			iteration_count: 0,
			stack: Stack::new(),
			symbols,
			control_flow: ControlFlow::Normal,
			dispatch_depth: 0,
			batch_size,
			active_mask: None,
			mask_stack: Vec::new(),
			loop_mask_stack: Vec::new(),
			params,
			routines,
			runtime_context,
			identity,
		}
	}

	pub(crate) fn eval_ctx(&self) -> EvalContext<'_> {
		EvalContext {
			params: self.params,
			symbols: &self.symbols,
			routines: self.routines,
			runtime_context: self.runtime_context,
			arena: None,
			identity: self.identity,
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: self.batch_size,
			target: None,
			take: None,
		}
	}

	pub(crate) fn run_isolated_body(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		instructions: &[Instruction],
		result: &mut Vec<Frame>,
	) -> Result<()> {
		let saved = self.params;
		self.params = &EMPTY_PARAMS;
		let run_result = self.run(services, tx, instructions, result);
		self.params = saved;
		run_result
	}

	pub(crate) fn pop_value(&mut self) -> Result<Value> {
		match self.stack.pop()? {
			Variable::Columns {
				columns: c,
			} if c.is_scalar() => Ok(c.scalar_value()),
			_ => Err(internal_error!("Expected scalar value on stack")),
		}
	}

	pub(crate) fn pop_as_columns(&mut self) -> Result<Columns> {
		match self.stack.pop()? {
			Variable::Columns {
				columns: c,
				..
			}
			| Variable::ForIterator {
				columns: c,
				..
			} => Ok(c),
			Variable::Closure(_) => Ok(Columns::single_row([("value", Value::none())])),
		}
	}

	pub(crate) fn run(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		instructions: &[Instruction],
		result: &mut Vec<Frame>,
	) -> Result<()> {
		let params = self.params;
		while self.ip < instructions.len() {
			if self.batch_size > 1 && self.check_mask_merge_point()? {}

			match &instructions[self.ip] {
				Instruction::Halt => return Ok(()),
				Instruction::Nop => {}

				Instruction::PushConst(v) => self.exec_push_const(v),
				Instruction::PushNone => self.exec_push_none(),
				Instruction::Pop => self.exec_pop()?,
				Instruction::Dup => self.exec_dup()?,

				Instruction::LoadVar(f) => self.exec_load_var(f)?,
				Instruction::StoreVar(f) => {
					if self.batch_size > 1 && self.is_masked() {
						let name = strip_dollar_prefix(f.text());
						let value = self.stack.pop()?;
						self.exec_store_var_masked(name, value)?;
					} else if self.batch_size > 1 {
						let name = strip_dollar_prefix(f.text());
						let value = self.stack.pop()?;
						self.symbols.reassign(name.to_string(), value)?;
					} else {
						self.exec_store_var(f)?;
					}
				}
				Instruction::DeclareVar(f) => self.exec_declare_var(f)?,
				Instruction::FieldAccess {
					object,
					field,
				} => self.exec_field_access(object, field)?,

				Instruction::Add => self.exec_add()?,
				Instruction::Sub => self.exec_sub()?,
				Instruction::Mul => self.exec_mul()?,
				Instruction::Div => self.exec_div()?,
				Instruction::Rem => self.exec_rem()?,
				Instruction::Negate => self.exec_negate()?,
				Instruction::LogicNot => self.exec_logic_not()?,

				Instruction::CmpEq => self.exec_cmp_eq()?,
				Instruction::CmpNe => self.exec_cmp_ne()?,
				Instruction::CmpLt => self.exec_cmp_lt()?,
				Instruction::CmpLe => self.exec_cmp_le()?,
				Instruction::CmpGt => self.exec_cmp_gt()?,
				Instruction::CmpGe => self.exec_cmp_ge()?,

				Instruction::LogicAnd => self.exec_logic_and()?,
				Instruction::LogicOr => self.exec_logic_or()?,
				Instruction::LogicXor => self.exec_logic_xor()?,
				Instruction::Between => self.exec_between()?,
				Instruction::InList {
					count,
					negated,
				} => self.exec_in_list(*count, *negated)?,
				Instruction::Cast(target) => self.exec_cast(target)?,

				Instruction::Jump(addr) => {
					if self.batch_size > 1
						&& (!self.mask_stack.is_empty() || !self.loop_mask_stack.is_empty())
					{
						if self.exec_jump_masked(*addr)? {
							continue;
						}
					} else {
						self.exec_jump(*addr)?;
						continue;
					}
				}
				Instruction::JumpIfFalsePop(addr) => {
					if self.batch_size > 1 {
						let is_while_loop = instructions.get(self.ip + 1).is_some_and(|next| {
							matches!(next, Instruction::EnterScope(ScopeType::Loop))
						});

						if is_while_loop
							&& self.loop_mask_stack
								.last()
								.is_none_or(|s| s.loop_end_addr != *addr)
						{
							let var = self.stack.pop()?;
							let bool_bv = extract_bool_bitvec(&var)?;
							let parent = self.effective_mask();
							let candidate = self.intersect_condition(&bool_bv);

							if candidate == parent {
							} else if candidate.none() {
								self.ip = *addr;
								continue;
							} else {
								self.enter_loop_mask(*addr, candidate);
							}
						} else if self.exec_jump_if_false_pop_columnar(*addr)? {
							continue;
						}
					} else if self.exec_jump_if_false_pop(*addr)? {
						continue;
					}
				}
				Instruction::JumpIfTruePop(addr) => {
					if self.batch_size > 1 {
						if self.exec_jump_if_true_pop_columnar(*addr)? {
							continue;
						}
					} else if self.exec_jump_if_true_pop(*addr)? {
						continue;
					}
				}
				Instruction::EnterScope(scope_type) => self.exec_enter_scope(scope_type),
				Instruction::ExitScope => self.exec_exit_scope()?,
				Instruction::Break {
					exit_scopes,
					addr,
				} => {
					if self.batch_size > 1 && !self.loop_mask_stack.is_empty() {
						self.exec_break_masked(*exit_scopes, *addr)?;
					} else {
						self.exec_break(*exit_scopes, *addr)?;
					}
					continue;
				}
				Instruction::Continue {
					exit_scopes,
					addr,
				} => {
					if self.batch_size > 1 && !self.loop_mask_stack.is_empty() {
						self.exec_continue_masked(*exit_scopes, *addr)?;
					} else {
						self.exec_continue(*exit_scopes, *addr)?;
					}
					continue;
				}

				Instruction::ForInit {
					variable_name,
				} => self.exec_for_init(variable_name)?,
				Instruction::ForNext {
					variable_name,
					addr,
				} => {
					if self.exec_for_next(variable_name, *addr)? {
						continue;
					}
				}

				Instruction::DefineFunction(node) => self.exec_define_function(node),
				Instruction::Call {
					name,
					arity,
					is_procedure_call,
				} => {
					self.exec_call(services, tx, name, *arity, *is_procedure_call)?;
				}
				Instruction::ReturnValue => {
					self.exec_return_value()?;
					return Ok(());
				}
				Instruction::ReturnVoid => {
					self.exec_return_void();
					return Ok(());
				}
				Instruction::DefineClosure(def) => self.exec_define_closure(def),

				Instruction::Emit => self.exec_emit(result),
				Instruction::Append {
					target,
				} => self.exec_append(target)?,

				Instruction::Query(plan) => self.exec_query(services, tx, plan, params)?,

				Instruction::CreateNamespace(n) => {
					self.exec_ddl(services, tx, |s, t| create_namespace(s, t, n.clone()))?
				}
				Instruction::CreateRemoteNamespace(n) => {
					self.exec_ddl(services, tx, |s, t| create_remote_namespace(s, t, n.clone()))?
				}
				Instruction::CreateTable(n) => {
					self.exec_ddl(services, tx, |s, t| create_table(s, t, n.clone()))?
				}
				Instruction::CreateRingBuffer(n) => {
					self.exec_ddl(services, tx, |s, t| create_ringbuffer(s, t, n.clone()))?
				}
				Instruction::CreateDeferredView(n) => {
					self.exec_ddl(services, tx, |s, t| create_deferred_view(s, t, n.clone()))?
				}
				Instruction::CreateTransactionalView(n) => {
					self.exec_ddl(services, tx, |s, t| create_transactional_view(s, t, n.clone()))?
				}
				Instruction::CreateDictionary(n) => {
					self.exec_ddl(services, tx, |s, t| create_dictionary(s, t, n.clone()))?
				}
				Instruction::CreateSumType(n) => {
					self.exec_ddl(services, tx, |s, t| create_sumtype(s, t, n.clone()))?
				}
				Instruction::CreatePrimaryKey(n) => {
					self.exec_ddl(services, tx, |s, t| create_primary_key(s, t, n.clone()))?
				}
				Instruction::CreateColumnProperty(n) => {
					self.exec_ddl(services, tx, |s, t| create_column_property(s, t, n.clone()))?
				}
				Instruction::CreateProcedure(n) => {
					self.exec_ddl(services, tx, |s, t| create_procedure(s, t, n.clone()))?
				}
				Instruction::CreateSeries(n) => {
					self.exec_ddl(services, tx, |s, t| create_series(s, t, n.clone()))?
				}
				Instruction::CreateEvent(n) => {
					self.exec_ddl(services, tx, |s, t| create_event(s, t, n.clone()))?
				}
				Instruction::CreateTag(n) => {
					self.exec_ddl(services, tx, |s, t| create_tag(s, t, n.clone()))?
				}
				Instruction::CreateSource(n) => {
					self.exec_ddl(services, tx, |s, t| create_source(s, t, n.clone()))?
				}
				Instruction::CreateSink(n) => {
					self.exec_ddl(services, tx, |s, t| create_sink(s, t, n.clone()))?
				}
				Instruction::CreateBinding(n) => {
					self.exec_ddl(services, tx, |s, t| create_binding(s, t, n.clone()))?
				}
				Instruction::CreateTest(n) => {
					self.exec_ddl(services, tx, |s, t| create_test(s, t, n.clone()))?
				}
				Instruction::CreateMigration(n) => {
					self.exec_ddl(services, tx, |s, t| create_migration(s, t, n.clone()))?
				}
				Instruction::CreateIdentity(n) => {
					self.exec_ddl(services, tx, |s, t| create_identity(s, t, n.clone()))?
				}
				Instruction::CreateRole(n) => {
					self.exec_ddl(services, tx, |s, t| create_role(s, t, n.clone()))?
				}
				Instruction::CreatePolicy(n) => {
					self.exec_ddl(services, tx, |s, t| create_policy(s, t, n.clone()))?
				}
				Instruction::CreateAuthentication(n) => {
					self.exec_ddl(services, tx, |s, t| create_authentication(s, t, n.clone()))?
				}
				Instruction::Grant(n) => self.exec_ddl(services, tx, |s, t| grant(s, t, n.clone()))?,
				Instruction::Revoke(n) => {
					self.exec_ddl(services, tx, |s, t| revoke(s, t, n.clone()))?
				}

				Instruction::CreateSubscription(n) => {
					self.exec_ddl_sub(services, tx, |s, t| create_subscription(s, t, n.clone()))?
				}

				Instruction::AlterTable(n) => {
					self.exec_ddl(services, tx, |s, t| execute_alter_table(s, t, n.clone()))?
				}
				Instruction::AlterRemoteNamespace(n) => {
					self.exec_ddl(services, tx, |s, t| alter_remote_namespace(s, t, n.clone()))?
				}
				Instruction::AlterSequence(n) => {
					self.exec_ddl(services, tx, |s, t| alter_table_sequence(s, t, n.clone()))?
				}
				Instruction::AlterPolicy(n) => {
					self.exec_ddl(services, tx, |s, t| alter_policy(s, t, n.clone()))?
				}

				Instruction::DropNamespace(n) => {
					self.exec_ddl(services, tx, |s, t| drop_namespace(s, t, n.clone()))?
				}
				Instruction::DropTable(n) => {
					self.exec_ddl(services, tx, |s, t| drop_table(s, t, n.clone()))?
				}
				Instruction::DropView(n) => {
					self.exec_ddl(services, tx, |s, t| drop_view(s, t, n.clone()))?
				}
				Instruction::DropRingBuffer(n) => {
					self.exec_ddl(services, tx, |s, t| drop_ringbuffer(s, t, n.clone()))?
				}
				Instruction::DropSeries(n) => {
					self.exec_ddl(services, tx, |s, t| drop_series(s, t, n.clone()))?
				}
				Instruction::DropDictionary(n) => {
					self.exec_ddl(services, tx, |s, t| drop_dictionary(s, t, n.clone()))?
				}
				Instruction::DropSumType(n) => {
					self.exec_ddl(services, tx, |s, t| drop_sumtype(s, t, n.clone()))?
				}
				Instruction::DropSource(n) => {
					self.exec_ddl(services, tx, |s, t| drop_source(s, t, n.clone()))?
				}
				Instruction::DropSink(n) => {
					self.exec_ddl(services, tx, |s, t| drop_sink(s, t, n.clone()))?
				}
				Instruction::DropProcedure(n) => {
					self.exec_ddl(services, tx, |s, t| drop_procedure(s, t, n.clone()))?
				}
				Instruction::DropHandler(n) => {
					self.exec_ddl(services, tx, |s, t| drop_handler(s, t, n.clone()))?
				}
				Instruction::DropTest(n) => {
					self.exec_ddl(services, tx, |s, t| drop_test(s, t, n.clone()))?
				}
				Instruction::DropBinding(n) => {
					self.exec_ddl(services, tx, |s, t| drop_binding(s, t, n.clone()))?
				}
				Instruction::DropIdentity(n) => {
					self.exec_ddl(services, tx, |s, t| drop_identity(s, t, n.clone()))?
				}
				Instruction::DropRole(n) => {
					self.exec_ddl(services, tx, |s, t| drop_role(s, t, n.clone()))?
				}
				Instruction::DropPolicy(n) => {
					self.exec_ddl(services, tx, |s, t| drop_policy(s, t, n.clone()))?
				}
				Instruction::DropAuthentication(n) => {
					self.exec_ddl(services, tx, |s, t| drop_authentication(s, t, n.clone()))?
				}

				Instruction::DropSubscription(n) => {
					self.exec_ddl_sub(services, tx, |s, t| drop_subscription(s, t, n.clone()))?
				}

				Instruction::Delete(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						delete(s, t, n.clone(), p, sym)
					})?
				}
				Instruction::DeleteRingBuffer(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						delete_ringbuffer(s, t, n.clone(), p, sym)
					})?
				}
				Instruction::DeleteSeries(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						delete_series(s, t, n.clone(), p, sym)
					})?
				}
				Instruction::Update(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						update_table(s, t, n.clone(), p, sym)
					})?
				}
				Instruction::UpdateRingBuffer(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						update_ringbuffer(s, t, n.clone(), p, sym)
					})?
				}
				Instruction::UpdateSeries(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						update_series(s, t, n.clone(), p, sym)
					})?
				}
				Instruction::InsertTable(n) => {
					self.exec_dml_with_mut_symbols(services, tx, |s, t, sym| {
						insert_table(s, t, n.clone(), sym)
					})?
				}
				Instruction::InsertDictionary(n) => {
					self.exec_dml_with_mut_symbols(services, tx, |s, t, sym| {
						insert_dictionary(s, t, n.clone(), sym)
					})?
				}
				Instruction::InsertRingBuffer(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						insert_ringbuffer(s, t, n.clone(), p, sym)
					})?
				}
				Instruction::InsertSeries(n) => {
					self.exec_dml_with_params(services, tx, params, |s, t, p, sym| {
						insert_series(s, t, n.clone(), p, sym)
					})?
				}

				Instruction::Dispatch(n) => self.exec_dispatch(services, tx, n, params)?,
				Instruction::Migrate(n) => self.exec_migrate(services, tx, n)?,
				Instruction::RollbackMigration(n) => self.exec_rollback_migration(services, tx, n)?,
				Instruction::AssertBlock(n) => self.exec_assert_block(services, tx, n)?,
			}

			self.ip += 1;

			if !self.control_flow.is_normal() {
				return Ok(());
			}
		}
		Ok(())
	}
}
