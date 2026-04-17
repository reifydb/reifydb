// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, result::Result as StdResult, sync::Arc, time::Duration};

use bumpalo::Bump;
use reifydb_catalog::{catalog::Catalog, vtable::system::flow_operator_store::SystemFlowOperatorStore};
use reifydb_core::{
	error::diagnostic::subscription,
	execution::ExecutionResult,
	metric::{ExecutionMetrics, StatementMetric},
	value::column::columns::Columns,
};
use reifydb_metric::storage::metric::MetricReader;
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
#[cfg(not(reifydb_single_threaded))]
use reifydb_type::error::Diagnostic;
use reifydb_type::{
	error::Error,
	params::Params,
	value::{Value, frame::frame::Frame, r#type::Type},
};
use tracing::instrument;

#[cfg(not(reifydb_single_threaded))]
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
	#[cfg(not(reifydb_single_threaded))]
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
	fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> ExecutionResult {
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
		symbols.set("identity".to_string(), Variable::columns(columns), false)?;
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
		symbols.set("identity".to_string(), Variable::columns(columns), false)?;
	}
	Ok(())
}

/// Execute a list of compiled units, tracking output frames separately.
type CompiledUnitsResult = (Vec<Frame>, Vec<Frame>, SymbolTable, Vec<StatementMetric>);

/// Error from `execute_compiled_units` that preserves partial metrics.
struct ExecutionFailure {
	error: Error,
	partial_metrics: Vec<StatementMetric>,
}

/// Build `ExecutionMetrics` from a list of statement metrics.
fn build_metrics(statements: Vec<StatementMetric>) -> ExecutionMetrics {
	let fps: Vec<_> = statements.iter().map(|m| m.fingerprint).collect();
	ExecutionMetrics {
		fingerprint: fingerprint_request(&fps),
		statements,
	}
}

/// Returns (output_results, last_result, final_symbols, metrics).
fn execute_compiled_units(
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	compiled_list: &[Compiled],
	params: &Params,
	mut symbols: SymbolTable,
	compile_duration: Duration,
) -> StdResult<CompiledUnitsResult, ExecutionFailure> {
	let compile_duration_us = compile_duration.as_micros() as u64 / compiled_list.len().max(1) as u64;
	let mut result = vec![];
	let mut output_results: Vec<Frame> = Vec::new();
	let mut metrics = Vec::new();

	for compiled in compiled_list.iter() {
		result.clear();
		let mut vm = Vm::from_services(symbols, services, params, tx.identity());
		let start = services.runtime_context.clock.instant();
		let run_result = vm.run(services, tx, &compiled.instructions, &mut result);
		let execute_duration = start.elapsed();
		symbols = vm.symbols;

		metrics.push(StatementMetric {
			fingerprint: compiled.fingerprint,
			normalized_rql: compiled.normalized_rql.clone(),
			compile_duration_us,
			execute_duration_us: execute_duration.as_micros() as u64,
			rows_affected: if run_result.is_ok() {
				extract_rows_affected(&result)
			} else {
				0
			},
		});

		if let Err(error) = run_result {
			return Err(ExecutionFailure {
				error,
				partial_metrics: metrics,
			});
		}

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

/// Extract the actual rows-affected count from a DML result.
///
/// DML handlers (INSERT/UPDATE/DELETE) emit a single summary frame with a
/// column named "inserted", "updated", or "deleted" containing the count as
/// a `Uint8` value. When that pattern is detected, return the real count.
/// Otherwise fall back to the number of frames (correct for SELECT, DDL, etc.).
fn extract_rows_affected(result: &[Frame]) -> u64 {
	if result.len() == 1 {
		let frame = &result[0];
		for col in &frame.columns {
			match col.name.as_str() {
				"inserted" | "updated" | "deleted" => {
					if col.data.len() == 1
						&& let Value::Uint8(n) = col.data.get_value(0)
					{
						return n;
					}
				}
				_ => {}
			}
		}
	}
	result.len() as u64
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
	pub fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> ExecutionResult {
		let mut symbols = match self.setup_symbols(&params, tx) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		let start_compile = self.0.runtime_context.clock.instant();
		let compiled_list = match self.compiler.compile_with_policy(tx, rql, inject_read_policies) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("incremental compilation not supported in rql()")
			}
			Err(err) => {
				#[cfg(not(reifydb_single_threaded))]
				if let Ok(Some(frames)) = self.try_forward_remote_query(&err, rql, params) {
					return ExecutionResult {
						frames,
						error: None,
						metrics: ExecutionMetrics::default(),
					};
				}
				return ExecutionResult {
					frames: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let compile_duration = start_compile.elapsed();
		let compile_duration_us = compile_duration.as_micros() as u64 / compiled_list.len().max(1) as u64;

		let mut result = vec![];
		let mut metrics = Vec::new();
		for compiled in compiled_list.iter() {
			result.clear();
			let mut vm = Vm::from_services(symbols, &self.0, &params, tx.identity());
			let start_execute = self.0.runtime_context.clock.instant();
			let run_result = vm.run(&self.0, tx, &compiled.instructions, &mut result);
			let execute_duration = start_execute.elapsed();
			symbols = vm.symbols;

			metrics.push(StatementMetric {
				fingerprint: compiled.fingerprint,
				normalized_rql: compiled.normalized_rql.clone(),
				compile_duration_us,
				execute_duration_us: execute_duration.as_micros() as u64,
				rows_affected: if run_result.is_ok() {
					extract_rows_affected(&result)
				} else {
					0
				},
			});

			if let Err(e) = run_result {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: build_metrics(metrics),
				};
			}
		}

		ExecutionResult {
			frames: result,
			error: None,
			metrics: build_metrics(metrics),
		}
	}

	#[instrument(name = "executor::admin", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn admin(&self, txn: &mut AdminTransaction, cmd: Admin<'_>) -> ExecutionResult {
		let symbols = match self.setup_symbols(&cmd.params, &mut Transaction::Admin(&mut *txn)) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		if let Err(e) = PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Admin(&mut *txn),
			"admin",
			true,
		) {
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}

		let start_compile = self.0.runtime_context.clock.instant();
		match self.compiler.compile_with_policy(&mut Transaction::Admin(txn), cmd.rql, inject_read_policies) {
			Err(err) => {
				#[cfg(not(reifydb_single_threaded))]
				if let Ok(Some(frames)) = self.try_forward_remote_query(&err, cmd.rql, cmd.params) {
					return ExecutionResult {
						frames,
						error: None,
						metrics: ExecutionMetrics::default(),
					};
				}
				ExecutionResult {
					frames: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				}
			}
			Ok(CompilationResult::Ready(compiled)) => {
				let compile_duration = start_compile.elapsed();
				match execute_compiled_units(
					&self.0,
					&mut Transaction::Admin(txn),
					&compiled,
					&cmd.params,
					symbols,
					compile_duration,
				) {
					Ok((output, remaining, _, metrics)) => ExecutionResult {
						frames: merge_results(output, remaining),
						error: None,
						metrics: build_metrics(metrics),
					},
					Err(f) => ExecutionResult {
						frames: vec![],
						error: Some(f.error),
						metrics: build_metrics(f.partial_metrics),
					},
				}
			}
			Ok(CompilationResult::Incremental(mut state)) => {
				let policy = constrain_policy(|plans, bump, cat, tx| {
					inject_read_policies(plans, bump, cat, tx)
				});
				let mut result = vec![];
				let mut output_results: Vec<Frame> = Vec::new();
				let mut symbols = symbols;
				let mut metrics = Vec::new();
				loop {
					let start_incr = self.0.runtime_context.clock.instant();
					let next = match self.compiler.compile_next_with_policy(
						&mut Transaction::Admin(txn),
						&mut state,
						&policy,
					) {
						Ok(n) => n,
						Err(e) => {
							return ExecutionResult {
								frames: vec![],
								error: Some(e),
								metrics: build_metrics(metrics),
							};
						}
					};
					let compile_duration = start_incr.elapsed();

					let Some(compiled) = next else {
						break;
					};

					result.clear();
					let mut tx = Transaction::Admin(txn);
					let mut vm = Vm::from_services(symbols, &self.0, &cmd.params, tx.identity());
					let start_execute = self.0.runtime_context.clock.instant();
					let run_result = vm.run(&self.0, &mut tx, &compiled.instructions, &mut result);
					let execute_duration = start_execute.elapsed();
					symbols = vm.symbols;

					metrics.push(StatementMetric {
						fingerprint: compiled.fingerprint,
						normalized_rql: compiled.normalized_rql,
						compile_duration_us: compile_duration.as_micros() as u64,
						execute_duration_us: execute_duration.as_micros() as u64,
						rows_affected: if run_result.is_ok() {
							extract_rows_affected(&result)
						} else {
							0
						},
					});

					if let Err(e) = run_result {
						return ExecutionResult {
							frames: vec![],
							error: Some(e),
							metrics: build_metrics(metrics),
						};
					}

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
				ExecutionResult {
					frames: merge_results(output_results, result),
					error: None,
					metrics: build_metrics(metrics),
				}
			}
		}
	}

	#[instrument(name = "executor::test", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn test(&self, txn: &mut TestTransaction<'_>, cmd: Test<'_>) -> ExecutionResult {
		let symbols = match self.setup_symbols(&cmd.params, &mut Transaction::Test(Box::new(txn.reborrow()))) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		let session_type = txn.session_type.clone();
		let session_default_deny = txn.session_default_deny;
		if let Err(e) = PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Test(Box::new(txn.reborrow())),
			&session_type,
			session_default_deny,
		) {
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}

		let start_compile = self.0.runtime_context.clock.instant();
		match self.compiler.compile_with_policy(
			&mut Transaction::Test(Box::new(txn.reborrow())),
			cmd.rql,
			inject_read_policies,
		) {
			Err(err) => {
				#[cfg(not(reifydb_single_threaded))]
				if let Ok(Some(frames)) = self.try_forward_remote_query(&err, cmd.rql, cmd.params) {
					return ExecutionResult {
						frames,
						error: None,
						metrics: ExecutionMetrics::default(),
					};
				}
				ExecutionResult {
					frames: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				}
			}
			Ok(CompilationResult::Ready(compiled)) => {
				let compile_duration = start_compile.elapsed();
				match execute_compiled_units(
					&self.0,
					&mut Transaction::Test(Box::new(txn.reborrow())),
					&compiled,
					&cmd.params,
					symbols,
					compile_duration,
				) {
					Ok((output, remaining, _, metrics)) => ExecutionResult {
						frames: merge_results(output, remaining),
						error: None,
						metrics: build_metrics(metrics),
					},
					Err(f) => ExecutionResult {
						frames: vec![],
						error: Some(f.error),
						metrics: build_metrics(f.partial_metrics),
					},
				}
			}
			Ok(CompilationResult::Incremental(mut state)) => {
				let policy = constrain_policy(|plans, bump, cat, tx| {
					inject_read_policies(plans, bump, cat, tx)
				});
				let mut result = vec![];
				let mut output_results: Vec<Frame> = Vec::new();
				let mut symbols = symbols;
				let mut metrics = Vec::new();
				loop {
					let start_incr = self.0.runtime_context.clock.instant();
					let next = match self.compiler.compile_next_with_policy(
						&mut Transaction::Test(Box::new(txn.reborrow())),
						&mut state,
						&policy,
					) {
						Ok(n) => n,
						Err(e) => {
							return ExecutionResult {
								frames: vec![],
								error: Some(e),
								metrics: build_metrics(metrics),
							};
						}
					};
					let compile_duration = start_incr.elapsed();

					let Some(compiled) = next else {
						break;
					};

					result.clear();
					let mut tx = Transaction::Test(Box::new(txn.reborrow()));
					let mut vm = Vm::from_services(symbols, &self.0, &cmd.params, tx.identity());
					let start_execute = self.0.runtime_context.clock.instant();
					let run_result = vm.run(&self.0, &mut tx, &compiled.instructions, &mut result);
					let execute_duration = start_execute.elapsed();
					symbols = vm.symbols;

					metrics.push(StatementMetric {
						fingerprint: compiled.fingerprint,
						normalized_rql: compiled.normalized_rql,
						compile_duration_us: compile_duration.as_micros() as u64,
						execute_duration_us: execute_duration.as_micros() as u64,
						rows_affected: if run_result.is_ok() {
							extract_rows_affected(&result)
						} else {
							0
						},
					});

					if let Err(e) = run_result {
						return ExecutionResult {
							frames: vec![],
							error: Some(e),
							metrics: build_metrics(metrics),
						};
					}

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
				ExecutionResult {
					frames: merge_results(output_results, result),
					error: None,
					metrics: build_metrics(metrics),
				}
			}
		}
	}

	#[instrument(name = "executor::subscription", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn subscription(&self, txn: &mut QueryTransaction, cmd: Subscription<'_>) -> ExecutionResult {
		// Pre-compilation validation: parse and check statement constraints
		let bump = Bump::new();
		let statements = match parse_str(&bump, cmd.rql) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		if statements.len() != 1 {
			return ExecutionResult {
				frames: vec![],
				error: Some(Error(Box::new(subscription::single_statement_required(
					"Subscription endpoint requires exactly one statement",
				)))),
				metrics: ExecutionMetrics::default(),
			};
		}

		let statement = &statements[0];
		if statement.nodes.len() != 1 || !statement.nodes[0].is_subscription_ddl() {
			return ExecutionResult {
				frames: vec![],
				error: Some(Error(Box::new(subscription::invalid_statement(
					"Subscription endpoint only supports CREATE SUBSCRIPTION or DROP SUBSCRIPTION",
				)))),
				metrics: ExecutionMetrics::default(),
			};
		}

		let symbols = match self.setup_symbols(&cmd.params, &mut Transaction::Query(&mut *txn)) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		if let Err(e) = PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Query(&mut *txn),
			"subscription",
			true,
		) {
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}

		let start_compile = self.0.runtime_context.clock.instant();
		let compiled = match self.compiler.compile_with_policy(
			&mut Transaction::Query(txn),
			cmd.rql,
			inject_read_policies,
		) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("Single subscription statement should not require incremental compilation")
			}
			Err(err) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let compile_duration = start_compile.elapsed();

		match execute_compiled_units(
			&self.0,
			&mut Transaction::Query(txn),
			&compiled,
			&cmd.params,
			symbols,
			compile_duration,
		) {
			Ok((output, remaining, _, metrics)) => ExecutionResult {
				frames: merge_results(output, remaining),
				error: None,
				metrics: build_metrics(metrics),
			},
			Err(f) => ExecutionResult {
				frames: vec![],
				error: Some(f.error),
				metrics: build_metrics(f.partial_metrics),
			},
		}
	}

	#[instrument(name = "executor::command", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn command(&self, txn: &mut CommandTransaction, cmd: Command<'_>) -> ExecutionResult {
		let symbols = match self.setup_symbols(&cmd.params, &mut Transaction::Command(&mut *txn)) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		if let Err(e) = PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Command(&mut *txn),
			"command",
			false,
		) {
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}

		let start_compile = self.0.runtime_context.clock.instant();
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
				#[cfg(not(reifydb_single_threaded))]
				if self.0.remote_registry.is_some() && remote::is_remote_query(&err) {
					return ExecutionResult {
						frames: vec![],
						error: Some(Error(Box::new(Diagnostic {
							code: "REMOTE_002".to_string(),
							message: "Write operations on remote namespaces are not supported"
								.to_string(),
							help: Some("Use the remote instance directly for write operations"
								.to_string()),
							..Default::default()
						}))),
						metrics: ExecutionMetrics::default(),
					};
				}
				return ExecutionResult {
					frames: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let compile_duration = start_compile.elapsed();

		match execute_compiled_units(
			&self.0,
			&mut Transaction::Command(txn),
			&compiled,
			&cmd.params,
			symbols,
			compile_duration,
		) {
			Ok((output, remaining, _, metrics)) => ExecutionResult {
				frames: merge_results(output, remaining),
				error: None,
				metrics: build_metrics(metrics),
			},
			Err(f) => ExecutionResult {
				frames: vec![],
				error: Some(f.error),
				metrics: build_metrics(f.partial_metrics),
			},
		}
	}

	/// Call a procedure by fully-qualified name (e.g., "banking.transfer_funds").
	#[instrument(name = "executor::call_procedure", level = "debug", skip(self, txn, params), fields(name = %name))]
	pub fn call_procedure(&self, txn: &mut CommandTransaction, name: &str, params: &Params) -> ExecutionResult {
		let rql = format!("CALL {}()", name);
		let symbols = match self.setup_symbols(params, &mut Transaction::Command(&mut *txn)) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		let start_compile = self.0.runtime_context.clock.instant();
		let compiled = match self.compiler.compile(&mut Transaction::Command(txn), &rql) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("CALL statements should not require incremental compilation")
			}
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let compile_duration = start_compile.elapsed();
		let compile_duration_us = compile_duration.as_micros() as u64 / compiled.len().max(1) as u64;

		let mut result = vec![];
		let mut metrics = Vec::new();
		let mut symbols = symbols;
		for compiled in compiled.iter() {
			result.clear();
			let mut tx = Transaction::Command(txn);
			let mut vm = Vm::from_services(symbols, &self.0, params, tx.identity());
			let start_execute = self.0.runtime_context.clock.instant();
			let run_result = vm.run(&self.0, &mut tx, &compiled.instructions, &mut result);
			let execute_duration = start_execute.elapsed();
			symbols = vm.symbols;

			metrics.push(StatementMetric {
				fingerprint: compiled.fingerprint,
				normalized_rql: compiled.normalized_rql.clone(),
				compile_duration_us,
				execute_duration_us: execute_duration.as_micros() as u64,
				rows_affected: if run_result.is_ok() {
					extract_rows_affected(&result)
				} else {
					0
				},
			});

			if let Err(e) = run_result {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: build_metrics(metrics),
				};
			}
		}

		ExecutionResult {
			frames: result,
			error: None,
			metrics: build_metrics(metrics),
		}
	}

	#[instrument(name = "executor::query", level = "debug", skip(self, txn, qry), fields(rql = %qry.rql))]
	pub fn query(&self, txn: &mut QueryTransaction, qry: Query<'_>) -> ExecutionResult {
		let symbols = match self.setup_symbols(&qry.params, &mut Transaction::Query(&mut *txn)) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		if let Err(e) = PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Query(&mut *txn),
			"query",
			false,
		) {
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}

		let start_compile = self.0.runtime_context.clock.instant();
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
				#[cfg(not(reifydb_single_threaded))]
				if let Ok(Some(frames)) = self.try_forward_remote_query(&err, qry.rql, qry.params) {
					return ExecutionResult {
						frames,
						error: None,
						metrics: ExecutionMetrics::default(),
					};
				}
				return ExecutionResult {
					frames: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let compile_duration = start_compile.elapsed();

		let exec_result = execute_compiled_units(
			&self.0,
			&mut Transaction::Query(txn),
			&compiled,
			&qry.params,
			symbols,
			compile_duration,
		);

		match exec_result {
			Ok((output, remaining, _, metrics)) => ExecutionResult {
				frames: merge_results(output, remaining),
				error: None,
				metrics: build_metrics(metrics),
			},
			Err(f) => ExecutionResult {
				frames: vec![],
				error: Some(f.error),
				metrics: build_metrics(f.partial_metrics),
			},
		}
	}
}
