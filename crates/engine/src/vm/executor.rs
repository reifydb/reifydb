// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc};

use bumpalo::Bump;
use reifydb_catalog::{catalog::Catalog, vtable::system::flow_operator_store::SystemFlowOperatorStore};
use reifydb_core::{error::diagnostic::subscription, util::ioc::IocContainer, value::column::columns::Columns};
use reifydb_function::registry::Functions;
use reifydb_metric::metric::MetricReader;
use reifydb_policy::inject_read_policies;
use reifydb_rql::{
	ast::parse_str,
	compiler::{CompilationResult, constrain_policy},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::{
	RqlExecutor, TestTransaction, Transaction, admin::AdminTransaction, command::CommandTransaction,
	query::QueryTransaction, subscription::SubscriptionTransaction,
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_type::error::Diagnostic;
use reifydb_type::{
	error::Error,
	params::Params,
	value::{Value, frame::frame::Frame, r#type::Type},
};
use tracing::instrument;

#[cfg(not(target_arch = "wasm32"))]
use crate::remote::{self, RemoteRegistry};
use crate::{
	Result,
	policy::PolicyEvaluator,
	procedure::registry::Procedures,
	transform::registry::Transforms,
	vm::{
		Admin, Command, Query, Subscription, Test,
		services::Services,
		stack::{SymbolTable, Variable},
		vm::Vm,
	},
};

/// Executor is the orchestration layer for RQL statement execution.
pub struct Executor(Arc<Services>);

impl Clone for Executor {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Deref for Executor {
	type Target = Services;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Executor {
	pub fn new(
		catalog: Catalog,
		runtime_context: RuntimeContext,
		functions: Functions,
		procedures: Procedures,
		transforms: Transforms,
		flow_operator_store: SystemFlowOperatorStore,
		stats_reader: MetricReader<SingleStore>,
		ioc: IocContainer,
		#[cfg(not(target_arch = "wasm32"))] remote_registry: Option<RemoteRegistry>,
	) -> Self {
		Self(Arc::new(Services::new(
			catalog,
			runtime_context,
			functions,
			procedures,
			transforms,
			flow_operator_store,
			stats_reader,
			ioc,
			#[cfg(not(target_arch = "wasm32"))]
			remote_registry,
		)))
	}

	/// Get a reference to the underlying Services
	pub fn services(&self) -> &Arc<Services> {
		&self.0
	}

	/// Construct an Executor from an existing `Arc<Services>`.
	pub fn from_services(services: Arc<Services>) -> Self {
		Self(services)
	}

	#[allow(dead_code)]
	pub fn testing() -> Self {
		Self(Services::testing())
	}

	/// If the error is a REMOTE_001 and we have a RemoteRegistry, forward the query.
	/// Returns `Ok(Some(frames))` if forwarded, `Ok(None)` if not a remote query.
	#[cfg(not(target_arch = "wasm32"))]
	fn try_forward_remote_query(&self, err: &Error, rql: &str, params: Params) -> Result<Option<Vec<Frame>>> {
		if let Some(ref registry) = self.0.remote_registry {
			if remote::is_remote_query(err) {
				if let Some(address) = remote::extract_remote_address(err) {
					let token = remote::extract_remote_token(err);
					return registry
						.forward_query(&address, rql, params, token.as_deref())
						.map(Some);
				}
			}
		}
		Ok(None)
	}
}

impl RqlExecutor for Executor {
	fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> Result<Vec<Frame>> {
		Executor::rql(self, tx, rql, params)
	}
}

/// Populate a stack with parameters so they can be accessed as variables.
fn populate_symbols(symbols: &mut SymbolTable, params: &Params) -> Result<()> {
	match params {
		Params::Positional(values) => {
			for (index, value) in values.iter().enumerate() {
				let param_name = (index + 1).to_string();
				symbols.set(param_name, Variable::scalar(value.clone()), false)?;
			}
		}
		Params::Named(map) => {
			for (name, value) in map.iter() {
				symbols.set(name.clone(), Variable::scalar(value.clone()), false)?;
			}
		}
		Params::None => {}
	}
	Ok(())
}

/// Populate the `$identity` variable in the symbol table so policy bodies
/// (and user RQL) can reference `$identity.id`, `$identity.name`, and `$identity.roles`.
fn populate_identity(symbols: &mut SymbolTable, catalog: &Catalog, tx: &mut Transaction<'_>) -> Result<()> {
	let identity = tx.identity();
	if identity.is_privileged() {
		return Ok(());
	}
	if identity.is_anonymous() {
		let columns = Columns::single_row([
			("id", Value::IdentityId(identity)),
			("name", Value::none_of(Type::Utf8)),
			("roles", Value::List(vec![])),
		]);
		symbols.set("identity".to_string(), Variable::Columns(columns), false)?;
		return Ok(());
	}
	if let Some(user) = catalog.find_identity(tx, identity)? {
		let roles = catalog.find_role_names_for_identity(tx, identity)?;
		let role_values: Vec<Value> = roles.into_iter().map(Value::Utf8).collect();
		let columns = Columns::single_row([
			("id", Value::IdentityId(identity)),
			("name", Value::Utf8(user.name)),
			("roles", Value::List(role_values)),
		]);
		symbols.set("identity".to_string(), Variable::Columns(columns), false)?;
	}
	Ok(())
}

impl Executor {
	/// Execute RQL against an existing open transaction.
	///
	/// This is the universal RQL execution interface: it compiles and runs
	/// arbitrary RQL within whatever transaction variant the caller provides.
	#[instrument(name = "executor::rql", level = "debug", skip(self, tx, params), fields(rql = %rql))]
	pub fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> Result<Vec<Frame>> {
		let mut result = vec![];
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, &params)?;
		populate_identity(&mut symbols, &self.catalog, tx)?;

		let compiled = match self
			.compiler
			.compile_with_policy(tx, rql, |plans, bump, cat, tx| inject_read_policies(plans, bump, cat, tx))
		{
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("incremental compilation not supported in rql()")
			}
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, rql, params)? {
					return Ok(frames);
				}
				return Err(err);
			}
		};

		for compiled in compiled.iter() {
			result.clear();
			let mut vm = Vm::new(symbols);
			vm.run(&self.0, tx, &compiled.instructions, &params, &mut result)?;
			symbols = vm.symbols;
		}

		Ok(result)
	}

	#[instrument(name = "executor::admin", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn admin(&self, txn: &mut AdminTransaction, cmd: Admin<'_>) -> Result<Vec<Frame>> {
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, &cmd.params)?;

		populate_identity(&mut symbols, &self.catalog, &mut Transaction::Admin(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Admin(&mut *txn),
			"admin",
			true,
		)?;

		match self.compiler.compile_with_policy(
			&mut Transaction::Admin(txn),
			cmd.rql,
			|plans, bump, cat, tx| inject_read_policies(plans, bump, cat, tx),
		) {
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, cmd.rql, cmd.params)? {
					return Ok(frames);
				}
				return Err(err);
			}
			Ok(CompilationResult::Ready(compiled)) => {
				for compiled in compiled.iter() {
					result.clear();
					let mut tx = Transaction::Admin(txn);
					let mut vm = Vm::new(symbols);
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					symbols = vm.symbols;

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
			}
			Ok(CompilationResult::Incremental(mut state)) => {
				let policy = constrain_policy(|plans, bump, cat, tx| {
					inject_read_policies(plans, bump, cat, tx)
				});
				while let Some(compiled) = self.compiler.compile_next_with_policy(
					&mut Transaction::Admin(txn),
					&mut state,
					&policy,
				)? {
					result.clear();
					let mut tx = Transaction::Admin(txn);
					let mut vm = Vm::new(symbols);
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					symbols = vm.symbols;

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}

	#[instrument(name = "executor::test", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn test(&self, txn: &mut TestTransaction<'_>, cmd: Test<'_>) -> Result<Vec<Frame>> {
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, &cmd.params)?;

		populate_identity(&mut symbols, &self.catalog, &mut Transaction::Test(txn.reborrow()))?;

		let session_type = txn.session_type.clone();
		let session_default_deny = txn.session_default_deny;
		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Test(txn.reborrow()),
			&session_type,
			session_default_deny,
		)?;

		match self.compiler.compile_with_policy(
			&mut Transaction::Test(txn.reborrow()),
			cmd.rql,
			|plans, bump, cat, tx| inject_read_policies(plans, bump, cat, tx),
		) {
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, cmd.rql, cmd.params)? {
					return Ok(frames);
				}
				return Err(err);
			}
			Ok(CompilationResult::Ready(compiled)) => {
				for compiled in compiled.iter() {
					result.clear();
					let mut tx = Transaction::Test(txn.reborrow());
					let mut vm = Vm::new(symbols);
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					symbols = vm.symbols;

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
			}
			Ok(CompilationResult::Incremental(mut state)) => {
				let policy = constrain_policy(|plans, bump, cat, tx| {
					inject_read_policies(plans, bump, cat, tx)
				});
				while let Some(compiled) = self.compiler.compile_next_with_policy(
					&mut Transaction::Test(txn.reborrow()),
					&mut state,
					&policy,
				)? {
					result.clear();
					let mut tx = Transaction::Test(txn.reborrow());
					let mut vm = Vm::new(symbols);
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					symbols = vm.symbols;

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}

	#[instrument(name = "executor::subscription", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn subscription(&self, txn: &mut SubscriptionTransaction, cmd: Subscription<'_>) -> Result<Vec<Frame>> {
		// Pre-compilation validation: parse and check statement constraints
		let bump = Bump::new();
		let statements = parse_str(&bump, cmd.rql)?;

		if statements.len() != 1 {
			return Err(Error(subscription::single_statement_required(
				"Subscription endpoint requires exactly one statement",
			)));
		}

		let statement = &statements[0];
		if statement.nodes.len() != 1 || !statement.nodes[0].is_subscription_ddl() {
			return Err(Error(subscription::invalid_statement(
				"Subscription endpoint only supports CREATE SUBSCRIPTION or DROP SUBSCRIPTION",
			)));
		}

		// Proceed with standard compilation and execution
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, &cmd.params)?;

		populate_identity(&mut symbols, &self.catalog, &mut Transaction::Subscription(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Subscription(&mut *txn),
			"subscription",
			true,
		)?;

		let compiled = match self.compiler.compile_with_policy(
			&mut Transaction::Subscription(txn),
			cmd.rql,
			|plans, bump, cat, tx| inject_read_policies(plans, bump, cat, tx),
		) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("Single subscription statement should not require incremental compilation")
			}
			Err(err) => return Err(err),
		};

		for compiled in compiled.iter() {
			result.clear();
			let mut tx = Transaction::Subscription(txn);
			let mut vm = Vm::new(symbols);
			vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
			symbols = vm.symbols;

			if compiled.is_output {
				output_results.append(&mut result);
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}

	#[instrument(name = "executor::command", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn command(&self, txn: &mut CommandTransaction, cmd: Command<'_>) -> Result<Vec<Frame>> {
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, &cmd.params)?;

		populate_identity(&mut symbols, &self.catalog, &mut Transaction::Command(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Command(&mut *txn),
			"command",
			false,
		)?;

		let compiled = match self.compiler.compile_with_policy(
			&mut Transaction::Command(txn),
			cmd.rql,
			|plans, bump, cat, tx| inject_read_policies(plans, bump, cat, tx),
		) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("DDL statements require admin transactions, not command transactions")
			}
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if self.0.remote_registry.is_some() && remote::is_remote_query(&err) {
					return Err(Error(Diagnostic {
						code: "REMOTE_002".to_string(),
						message: "Write operations on remote namespaces are not supported"
							.to_string(),
						help: Some("Use the remote instance directly for write operations"
							.to_string()),
						..Default::default()
					}));
				}
				return Err(err);
			}
		};

		for compiled in compiled.iter() {
			result.clear();
			let mut tx = Transaction::Command(txn);
			let mut vm = Vm::new(symbols);
			vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
			symbols = vm.symbols;

			if compiled.is_output {
				output_results.append(&mut result);
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}

	/// Call a procedure by fully-qualified name (e.g., "banking.transfer_funds").
	#[instrument(name = "executor::call_procedure", level = "debug", skip(self, txn, params), fields(name = %name))]
	pub fn call_procedure(&self, txn: &mut CommandTransaction, name: &str, params: &Params) -> Result<Vec<Frame>> {
		// Compile and execute CALL <name>(<params>)
		let rql = format!("CALL {}()", name);
		let mut result = vec![];
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, params)?;
		populate_identity(&mut symbols, &self.catalog, &mut Transaction::Command(&mut *txn))?;

		let compiled = match self.compiler.compile(&mut Transaction::Command(txn), &rql)? {
			CompilationResult::Ready(compiled) => compiled,
			CompilationResult::Incremental(_) => {
				unreachable!("CALL statements should not require incremental compilation")
			}
		};

		for compiled in compiled.iter() {
			result.clear();
			let mut tx = Transaction::Command(txn);
			let mut vm = Vm::new(symbols);
			vm.run(&self.0, &mut tx, &compiled.instructions, params, &mut result)?;
			symbols = vm.symbols;
		}

		Ok(result)
	}

	#[instrument(name = "executor::query", level = "debug", skip(self, txn, qry), fields(rql = %qry.rql))]
	pub fn query(&self, txn: &mut QueryTransaction, qry: Query<'_>) -> Result<Vec<Frame>> {
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, &qry.params)?;

		populate_identity(&mut symbols, &self.catalog, &mut Transaction::Query(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Query(&mut *txn),
			"query",
			false,
		)?;

		let compiled = match self.compiler.compile_with_policy(
			&mut Transaction::Query(txn),
			qry.rql,
			|plans, bump, cat, tx| inject_read_policies(plans, bump, cat, tx),
		) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("DDL statements require admin transactions, not query transactions")
			}
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, qry.rql, qry.params)? {
					return Ok(frames);
				}
				return Err(err);
			}
		};

		for compiled in compiled.iter() {
			result.clear();
			let mut tx = Transaction::Query(txn);
			let mut vm = Vm::new(symbols);
			vm.run(&self.0, &mut tx, &compiled.instructions, &qry.params, &mut result)?;
			symbols = vm.symbols;

			if compiled.is_output {
				output_results.append(&mut result);
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}
}
