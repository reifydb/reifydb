// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc};

use bumpalo::Bump;
use reifydb_catalog::{catalog::Catalog, vtable::system::flow_operator_store::SystemFlowOperatorStore};
use reifydb_core::{
	error::diagnostic::subscription,
	execution::ExecutionResult,
	metric::{ExecutionMetrics, StatementMetric},
	value::column::columns::Columns,
};
use reifydb_metric_old::metric::MetricReader;
use reifydb_policy::inject_read_policies;
use reifydb_rql::{
	ast::parse_str,
	compiler::{CompilationResult, Compiled, constrain_policy},
	fingerprint::request::fingerprint_request,
};
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::{
	RqlExecutor, TestTransaction, Transaction, admin::AdminTransaction, command::CommandTransaction,
	query::QueryTransaction,
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
use crate::remote;
use crate::{
	Result,
	policy::PolicyEvaluator,
	vm::{
		Admin, Command, Query, Subscription, Test,
		services::{EngineConfig, Services},
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
		config: EngineConfig,
		flow_operator_store: SystemFlowOperatorStore,
		stats_reader: MetricReader<SingleStore>,
	) -> Self {
		Self(Arc::new(Services::new(catalog, config, flow_operator_store, stats_reader)))
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
		if let Some(ref registry) = self.0.remote_registry
			&& remote::is_remote_query(err)
			&& let Some(address) = remote::extract_remote_address(err)
		{
			let token = remote::extract_remote_token(err);
			return registry.forward_query(&address, rql, params, token.as_deref()).map(Some);
		}
		Ok(None)
	}
}

impl RqlExecutor for Executor {
	fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> Result<ExecutionResult> {
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

/// Execute a list of compiled units, tracking output frames separately.
type CompiledUnitsResult = (Vec<Frame>, Vec<Frame>, SymbolTable, Vec<StatementMetric>);

/// Returns (output_results, last_result, final_symbols, metrics).
fn execute_compiled_units(
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	compiled_list: &[Compiled],
	params: &Params,
	mut symbols: SymbolTable,
) -> Result<CompiledUnitsResult> {
	let mut result = vec![];
	let mut output_results: Vec<Frame> = Vec::new();
	let mut metrics = Vec::new();

	for compiled in compiled_list.iter() {
		result.clear();
		let mut vm = Vm::new(symbols);
		let start = services.runtime_context.clock.instant();
		vm.run(services, tx, &compiled.instructions, params, &mut result)?;
		let execute_duration = start.elapsed();
		symbols = vm.symbols;

		metrics.push(StatementMetric {
			fingerprint: compiled.fingerprint,
			normalized_rql: compiled.normalized_rql.clone(),
			compile_duration_us: 0, // Not tracked per-statement in Ready case yet
			execute_duration_us: execute_duration.as_micros() as u64,
			rows_affected: result.len() as u64, // Rough approximation
		});

		if compiled.is_output {
			output_results.append(&mut result);
		}
	}

	Ok((output_results, result, symbols, metrics))
}

/// Merge output_results and remaining results into the final result.
fn merge_results(mut output_results: Vec<Frame>, mut remaining: Vec<Frame>) -> Vec<Frame> {
	output_results.append(&mut remaining);
	output_results
}

impl Executor {
	/// Shared setup: create symbols and populate with params + identity.
	fn setup_symbols(&self, params: &Params, tx: &mut Transaction<'_>) -> Result<SymbolTable> {
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, params)?;
		populate_identity(&mut symbols, &self.catalog, tx)?;
		Ok(symbols)
	}

	/// Execute RQL against an existing open transaction.
	///
	/// This is the universal RQL execution interface: it compiles and runs
	/// arbitrary RQL within whatever transaction variant the caller provides.
	#[instrument(name = "executor::rql", level = "debug", skip(self, tx, params), fields(rql = %rql))]
	pub fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> Result<ExecutionResult> {
		let mut symbols = self.setup_symbols(&params, tx)?;

		let start_compile = self.0.runtime_context.clock.instant();
		let compiled_list = match self.compiler.compile_with_policy(tx, rql, inject_read_policies) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("incremental compilation not supported in rql()")
			}
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, rql, params)? {
					return Ok(ExecutionResult {
						frames,
						metrics: ExecutionMetrics::default(),
					});
				}
				return Err(err);
			}
		};
		let compile_duration = start_compile.elapsed();

		let mut result = vec![];
		let mut metrics = Vec::new();
		for compiled in compiled_list.iter() {
			result.clear();
			let mut vm = Vm::new(symbols);
			let start_execute = self.0.runtime_context.clock.instant();
			vm.run(&self.0, tx, &compiled.instructions, &params, &mut result)?;
			let execute_duration = start_execute.elapsed();
			symbols = vm.symbols;

			metrics.push(StatementMetric {
				fingerprint: compiled.fingerprint,
				normalized_rql: compiled.normalized_rql.clone(),
				compile_duration_us: compile_duration.as_micros() as u64 / compiled_list.len() as u64, /* Apportioned */
				execute_duration_us: execute_duration.as_micros() as u64,
				rows_affected: result.len() as u64,
			});
		}

		let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
		let request_fingerprint = fingerprint_request(&fps);

		Ok(ExecutionResult {
			frames: result,
			metrics: ExecutionMetrics {
				request_fingerprint,
				statements: metrics,
			},
		})
	}

	#[instrument(name = "executor::admin", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn admin(&self, txn: &mut AdminTransaction, cmd: Admin<'_>) -> Result<ExecutionResult> {
		let symbols = self.setup_symbols(&cmd.params, &mut Transaction::Admin(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Admin(&mut *txn),
			"admin",
			true,
		)?;

		match self.compiler.compile_with_policy(&mut Transaction::Admin(txn), cmd.rql, inject_read_policies) {
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, cmd.rql, cmd.params)? {
					return Ok(ExecutionResult {
						frames,
						metrics: ExecutionMetrics::default(),
					});
				}
				Err(err)
			}
			Ok(CompilationResult::Ready(compiled)) => {
				let (output, remaining, _, metrics) = execute_compiled_units(
					&self.0,
					&mut Transaction::Admin(txn),
					&compiled,
					&cmd.params,
					symbols,
				)?;
				let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
				Ok(ExecutionResult {
					frames: merge_results(output, remaining),
					metrics: ExecutionMetrics {
						request_fingerprint: fingerprint_request(&fps),
						statements: metrics,
					},
				})
			}
			Ok(CompilationResult::Incremental(mut state)) => {
				let policy = constrain_policy(|plans, bump, cat, tx| {
					inject_read_policies(plans, bump, cat, tx)
				});
				let mut result = vec![];
				let mut output_results: Vec<Frame> = Vec::new();
				let mut symbols = symbols;
				let mut metrics = Vec::new();
				while let Some(compiled) = self.compiler.compile_next_with_policy(
					&mut Transaction::Admin(txn),
					&mut state,
					&policy,
				)? {
					result.clear();
					let mut tx = Transaction::Admin(txn);
					let mut vm = Vm::new(symbols);
					let start_execute = self.0.runtime_context.clock.instant();
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					let execute_duration = start_execute.elapsed();
					symbols = vm.symbols;

					metrics.push(StatementMetric {
						fingerprint: compiled.fingerprint,
						normalized_rql: compiled.normalized_rql,
						compile_duration_us: 0, // Incremental compilation time not tracked yet
						execute_duration_us: execute_duration.as_micros() as u64,
						rows_affected: result.len() as u64,
					});

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
				let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
				Ok(ExecutionResult {
					frames: merge_results(output_results, result),
					metrics: ExecutionMetrics {
						request_fingerprint: fingerprint_request(&fps),
						statements: metrics,
					},
				})
			}
		}
	}

	#[instrument(name = "executor::test", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn test(&self, txn: &mut TestTransaction<'_>, cmd: Test<'_>) -> Result<ExecutionResult> {
		let symbols = self.setup_symbols(&cmd.params, &mut Transaction::Test(Box::new(txn.reborrow())))?;

		let session_type = txn.session_type.clone();
		let session_default_deny = txn.session_default_deny;
		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Test(Box::new(txn.reborrow())),
			&session_type,
			session_default_deny,
		)?;

		match self.compiler.compile_with_policy(
			&mut Transaction::Test(Box::new(txn.reborrow())),
			cmd.rql,
			inject_read_policies,
		) {
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, cmd.rql, cmd.params)? {
					return Ok(ExecutionResult {
						frames,
						metrics: ExecutionMetrics::default(),
					});
				}
				Err(err)
			}
			Ok(CompilationResult::Ready(compiled)) => {
				let (output, remaining, _, metrics) = execute_compiled_units(
					&self.0,
					&mut Transaction::Test(Box::new(txn.reborrow())),
					&compiled,
					&cmd.params,
					symbols,
				)?;
				let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
				Ok(ExecutionResult {
					frames: merge_results(output, remaining),
					metrics: ExecutionMetrics {
						request_fingerprint: fingerprint_request(&fps),
						statements: metrics,
					},
				})
			}
			Ok(CompilationResult::Incremental(mut state)) => {
				let policy = constrain_policy(|plans, bump, cat, tx| {
					inject_read_policies(plans, bump, cat, tx)
				});
				let mut result = vec![];
				let mut output_results: Vec<Frame> = Vec::new();
				let mut symbols = symbols;
				let mut metrics = Vec::new();
				while let Some(compiled) = self.compiler.compile_next_with_policy(
					&mut Transaction::Test(Box::new(txn.reborrow())),
					&mut state,
					&policy,
				)? {
					result.clear();
					let mut tx = Transaction::Test(Box::new(txn.reborrow()));
					let mut vm = Vm::new(symbols);
					let start_execute = self.0.runtime_context.clock.instant();
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					let execute_duration = start_execute.elapsed();
					symbols = vm.symbols;

					metrics.push(StatementMetric {
						fingerprint: compiled.fingerprint,
						normalized_rql: compiled.normalized_rql,
						compile_duration_us: 0,
						execute_duration_us: execute_duration.as_micros() as u64,
						rows_affected: result.len() as u64,
					});

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
				let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
				Ok(ExecutionResult {
					frames: merge_results(output_results, result),
					metrics: ExecutionMetrics {
						request_fingerprint: fingerprint_request(&fps),
						statements: metrics,
					},
				})
			}
		}
	}

	#[instrument(name = "executor::subscription", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn subscription(&self, txn: &mut QueryTransaction, cmd: Subscription<'_>) -> Result<ExecutionResult> {
		// Pre-compilation validation: parse and check statement constraints
		let bump = Bump::new();
		let statements = parse_str(&bump, cmd.rql)?;

		if statements.len() != 1 {
			return Err(Error(Box::new(subscription::single_statement_required(
				"Subscription endpoint requires exactly one statement",
			))));
		}

		let statement = &statements[0];
		if statement.nodes.len() != 1 || !statement.nodes[0].is_subscription_ddl() {
			return Err(Error(Box::new(subscription::invalid_statement(
				"Subscription endpoint only supports CREATE SUBSCRIPTION or DROP SUBSCRIPTION",
			))));
		}

		let symbols = self.setup_symbols(&cmd.params, &mut Transaction::Query(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Query(&mut *txn),
			"subscription",
			true,
		)?;

		let compiled = match self.compiler.compile_with_policy(
			&mut Transaction::Query(txn),
			cmd.rql,
			inject_read_policies,
		) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("Single subscription statement should not require incremental compilation")
			}
			Err(err) => return Err(err),
		};

		let (output, remaining, _, metrics) =
			execute_compiled_units(&self.0, &mut Transaction::Query(txn), &compiled, &cmd.params, symbols)?;
		let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
		Ok(ExecutionResult {
			frames: merge_results(output, remaining),
			metrics: ExecutionMetrics {
				request_fingerprint: fingerprint_request(&fps),
				statements: metrics,
			},
		})
	}

	#[instrument(name = "executor::command", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn command(&self, txn: &mut CommandTransaction, cmd: Command<'_>) -> Result<ExecutionResult> {
		let symbols = self.setup_symbols(&cmd.params, &mut Transaction::Command(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Command(&mut *txn),
			"command",
			false,
		)?;

		let compiled = match self.compiler.compile_with_policy(
			&mut Transaction::Command(txn),
			cmd.rql,
			inject_read_policies,
		) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("DDL statements require admin transactions, not command transactions")
			}
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if self.0.remote_registry.is_some() && remote::is_remote_query(&err) {
					return Err(Error(Box::new(Diagnostic {
						code: "REMOTE_002".to_string(),
						message: "Write operations on remote namespaces are not supported"
							.to_string(),
						help: Some("Use the remote instance directly for write operations"
							.to_string()),
						..Default::default()
					})));
				}
				return Err(err);
			}
		};

		let (output, remaining, _, metrics) = execute_compiled_units(
			&self.0,
			&mut Transaction::Command(txn),
			&compiled,
			&cmd.params,
			symbols,
		)?;
		let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
		Ok(ExecutionResult {
			frames: merge_results(output, remaining),
			metrics: ExecutionMetrics {
				request_fingerprint: fingerprint_request(&fps),
				statements: metrics,
			},
		})
	}

	/// Call a procedure by fully-qualified name (e.g., "banking.transfer_funds").
	#[instrument(name = "executor::call_procedure", level = "debug", skip(self, txn, params), fields(name = %name))]
	pub fn call_procedure(
		&self,
		txn: &mut CommandTransaction,
		name: &str,
		params: &Params,
	) -> Result<ExecutionResult> {
		let rql = format!("CALL {}()", name);
		let symbols = self.setup_symbols(params, &mut Transaction::Command(&mut *txn))?;

		let compiled = match self.compiler.compile(&mut Transaction::Command(txn), &rql)? {
			CompilationResult::Ready(compiled) => compiled,
			CompilationResult::Incremental(_) => {
				unreachable!("CALL statements should not require incremental compilation")
			}
		};

		let mut result = vec![];
		let mut metrics = Vec::new();
		let mut symbols = symbols;
		for compiled in compiled.iter() {
			result.clear();
			let mut tx = Transaction::Command(txn);
			let mut vm = Vm::new(symbols);
			let start_execute = self.0.runtime_context.clock.instant();
			vm.run(&self.0, &mut tx, &compiled.instructions, params, &mut result)?;
			let execute_duration = start_execute.elapsed();
			symbols = vm.symbols;

			metrics.push(StatementMetric {
				fingerprint: compiled.fingerprint,
				normalized_rql: compiled.normalized_rql.clone(),
				compile_duration_us: 0,
				execute_duration_us: execute_duration.as_micros() as u64,
				rows_affected: result.len() as u64,
			});
		}

		let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
		Ok(ExecutionResult {
			frames: result,
			metrics: ExecutionMetrics {
				request_fingerprint: fingerprint_request(&fps),
				statements: metrics,
			},
		})
	}

	#[instrument(name = "executor::query", level = "debug", skip(self, txn, qry), fields(rql = %qry.rql))]
	pub fn query(&self, txn: &mut QueryTransaction, qry: Query<'_>) -> Result<ExecutionResult> {
		let symbols = self.setup_symbols(&qry.params, &mut Transaction::Query(&mut *txn))?;

		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Query(&mut *txn),
			"query",
			false,
		)?;

		let compiled = match self.compiler.compile_with_policy(
			&mut Transaction::Query(txn),
			qry.rql,
			inject_read_policies,
		) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("DDL statements require admin transactions, not query transactions")
			}
			Err(err) => {
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(frames) = self.try_forward_remote_query(&err, qry.rql, qry.params)? {
					return Ok(ExecutionResult {
						frames,
						metrics: ExecutionMetrics::default(),
					});
				}
				return Err(err);
			}
		};

		let (output, remaining, _, metrics) =
			execute_compiled_units(&self.0, &mut Transaction::Query(txn), &compiled, &qry.params, symbols)?;
		let fps: Vec<_> = metrics.iter().map(|m| m.fingerprint).collect();
		Ok(ExecutionResult {
			frames: merge_results(output, remaining),
			metrics: ExecutionMetrics {
				request_fingerprint: fingerprint_request(&fps),
				statements: metrics,
			},
		})
	}
}
