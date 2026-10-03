// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter, ops::Deref, result::Result as StdResult, sync::Arc};

use arrow_array::Array;
use bumpalo::Bump;
use reifydb_catalog::{
	catalog::Catalog, metrics::storage::metrics::MetricsReader, vtable::system::operator_libary::OperatorLibrary,
};
use reifydb_core::{
	error::diagnostic::subscription,
	execution::ExecutionResult,
	interface::catalog::{
		flow::FlowId,
		policy::SessionOp,
		subscription::{SubscribeOptions, SubscribeOutcome},
	},
	metrics::execution::{ExecutionMetrics, StatementMetrics},
	value::batch::single_row,
};
use reifydb_evaluate::stack::{SymbolTable, Variable};
use reifydb_flow::compiler::compile_subscription_flow_ephemeral;
use reifydb_policy::inject_from_policies;
use reifydb_rql::{
	ast::parse_str,
	compiler::{CompilationResult, Compiled, IncrementalCompilation, constrain_policy},
	fingerprint::request::fingerprint_request,
	query::QueryPlan,
};
use reifydb_runtime::context::clock::Instant;
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::{
	RqlExecutor, TestTransaction, Transaction, admin::AdminTransaction, command::CommandTransaction,
	query::QueryTransaction,
};
#[cfg(not(reifydb_single_threaded))]
use reifydb_value::error::Diagnostic;
use reifydb_value::{
	error::Error,
	params::Params,
	value::{
		Value, column_view::ColumnView, duration::Duration, frame::frame::Frame, identity::IdentityKind,
		system_columns::SystemColumn, value_type::ValueType,
	},
};
use tracing::instrument;

#[cfg(not(reifydb_single_threaded))]
use crate::remote;
use crate::{
	Result,
	policy::PolicyEvaluator,
	subscription::{SubscriptionContext, SubscriptionServiceRef},
	vm::{
		Admin, Command, Query, Test,
		services::{EngineConfig, Services},
		vm::Vm,
	},
};

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
		operator_library: OperatorLibrary,
		metrics_reader: MetricsReader<SingleStore>,
	) -> Self {
		Self(Arc::new(Services::new(catalog, config, operator_library, metrics_reader)))
	}

	pub fn services(&self) -> &Arc<Services> {
		&self.0
	}

	pub fn from_services(services: Arc<Services>) -> Self {
		Self(services)
	}

	#[cfg(test)]
	pub fn testing() -> Self {
		Self(Services::testing())
	}

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

fn populate_identity(symbols: &mut SymbolTable, catalog: &Catalog, tx: &mut Transaction<'_>) -> Result<()> {
	let identity = tx.identity();
	if identity.is_privileged() {
		return Ok(());
	}
	let attributes = catalog.list_identity_attributes(tx)?;
	if identity.is_anonymous() {
		let mut fields = vec![
			("id".to_string(), Value::IdentityId(identity)),
			("name".to_string(), Value::none_of(ValueType::Utf8)),
			("roles".to_string(), Value::List(vec![])),
			("kind".to_string(), Value::Utf8(IdentityKind::Anonymous.as_str().to_string())),
		];
		for attribute in &attributes {
			fields.push((attribute.name.clone(), Value::none_of(attribute.value_type.clone())));
		}
		let batch = single_row(fields.iter().map(|(name, value)| (name.as_str(), value.clone())))?;
		symbols.set("identity".to_string(), Variable::columns(batch), false)?;
		return Ok(());
	}
	if let Some(user) = catalog.find_identity(tx, identity)? {
		let roles = catalog.find_role_names_for_identity(tx, identity)?;
		let role_values: Vec<Value> = roles.into_iter().map(Value::Utf8).collect();
		let values = catalog.find_identity_attribute_values(tx, identity)?;
		let kind = user.resolved_kind();
		let mut fields = vec![
			("id".to_string(), Value::IdentityId(identity)),
			("name".to_string(), Value::Utf8(user.name)),
			("roles".to_string(), Value::List(role_values)),
			("kind".to_string(), Value::Utf8(kind.as_str().to_string())),
		];
		for attribute in &attributes {
			let value = values
				.iter()
				.find(|v| v.attribute == attribute.id)
				.map(|v| v.value.clone())
				.unwrap_or_else(|| Value::none_of(attribute.value_type.clone()));
			fields.push((attribute.name.clone(), value));
		}
		let batch = single_row(fields.iter().map(|(name, value)| (name.as_str(), value.clone())))?;
		symbols.set("identity".to_string(), Variable::columns(batch), false)?;
	}
	Ok(())
}

type CompiledUnitsResult = (Emitted, Emitted, bool, SymbolTable, Vec<StatementMetrics>);

#[derive(Default)]
struct Emitted {
	frames: Vec<Frame>,
	named_system_columns: Vec<Vec<SystemColumn>>,
}

impl Emitted {
	#[cfg(not(reifydb_single_threaded))]
	fn forwarded(frames: Vec<Frame>) -> Self {
		Self {
			named_system_columns: vec![SystemColumn::ALL.to_vec(); frames.len()],
			frames,
		}
	}

	fn append(&mut self, frames: &mut Vec<Frame>, named: &[SystemColumn]) {
		self.named_system_columns.extend(iter::repeat_n(named.to_vec(), frames.len()));
		self.frames.append(frames);
	}

	fn into_result(self, metrics: ExecutionMetrics) -> ExecutionResult {
		ExecutionResult {
			frames: self.frames,
			named_system_columns: self.named_system_columns,
			error: None,
			metrics,
		}
	}
}

struct ExecutionFailure {
	error: Error,
	partial_metrics: Vec<StatementMetrics>,
}

fn build_metrics(statements: Vec<StatementMetrics>) -> ExecutionMetrics {
	let fps: Vec<_> = statements.iter().map(|m| m.fingerprint).collect();
	ExecutionMetrics {
		fingerprint: fingerprint_request(&fps),
		statements,
		..Default::default()
	}
}

struct RunUnitOutcome {
	symbols: SymbolTable,
	run_result: Result<()>,
	execute_duration: Duration,
}

#[instrument(
	name = "vm::run",
	level = "debug",
	skip_all,
	fields(fingerprint = ?compiled.fingerprint, instr_count = compiled.instructions.len()),
)]
fn run_compiled_unit(
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	compiled: &Compiled,
	params: &Params,
	symbols: SymbolTable,
	result: &mut Vec<Frame>,
) -> RunUnitOutcome {
	let mut vm = Vm::from_services(symbols, services, params, tx.identity());
	let start = services.runtime_context.clock.instant();
	let run_result = vm.run(services, tx, &compiled.instructions, result);
	let execute_duration = Duration::from_std(start.elapsed());
	RunUnitOutcome {
		symbols: vm.symbols,
		run_result,
		execute_duration,
	}
}

#[instrument(
	name = "executor::execute_units",
	level = "debug",
	skip_all,
	fields(unit_count = compiled_list.len()),
)]
fn execute_compiled_units(
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	compiled_list: &[Compiled],
	params: &Params,
	mut symbols: SymbolTable,
	compile_duration: Duration,
) -> StdResult<CompiledUnitsResult, ExecutionFailure> {
	let compile_duration_per_unit = Duration::from_micros_infallible(
		compile_duration.to_std().as_micros() as u64 / compiled_list.len().max(1) as u64,
	);
	let mut result = vec![];
	let mut output_results = Emitted::default();
	let mut saw_output = false;
	let mut metrics = Vec::new();

	for compiled in compiled_list.iter() {
		result.clear();
		let outcome = run_compiled_unit(services, tx, compiled, params, symbols, &mut result);
		symbols = outcome.symbols;

		let rows_affected = match &outcome.run_result {
			Ok(()) => match extract_rows_affected(&result) {
				Ok(n) => n,
				Err(error) => {
					return Err(ExecutionFailure {
						error,
						partial_metrics: metrics,
					});
				}
			},
			Err(_) => 0,
		};
		metrics.push(StatementMetrics {
			fingerprint: compiled.fingerprint,
			normalized_rql: compiled.normalized_rql.clone(),
			compile_duration: compile_duration_per_unit,
			execute_duration: outcome.execute_duration,
			rows_affected,
		});

		if let Err(error) = outcome.run_result {
			return Err(ExecutionFailure {
				error,
				partial_metrics: metrics,
			});
		}

		if compiled.is_output {
			saw_output = true;
			output_results.append(&mut result, &compiled.named_system_columns);
		}
	}

	let mut last = Emitted::default();
	if let Some(compiled) = compiled_list.last() {
		last.append(&mut result, &compiled.named_system_columns);
	}
	Ok((output_results, last, saw_output, symbols, metrics))
}

fn select_frames(saw_output: bool, output: Emitted, last: Emitted) -> Emitted {
	if saw_output {
		output
	} else {
		last
	}
}

#[inline]
fn error_result(error: Error, metrics: ExecutionMetrics) -> ExecutionResult {
	ExecutionResult {
		frames: vec![],
		named_system_columns: vec![],
		error: Some(error),
		metrics,
	}
}

fn extract_rows_affected(result: &[Frame]) -> Result<u64> {
	if result.len() == 1 {
		let batch = &result[0].batch;
		for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
			match field.name().as_str() {
				"inserted" | "updated" | "deleted" => {
					if array.len() == 1
						&& let Value::Uint8(n) =
							ColumnView::try_from((array, field.as_ref()))?.get_value(0)
					{
						return Ok(n);
					}
				}
				_ => {}
			}
		}
	}
	Ok(result.len() as u64)
}

impl Executor {
	#[instrument(name = "executor::setup_symbols", level = "debug", skip_all)]
	fn setup_symbols(&self, params: &Params, tx: &mut Transaction<'_>) -> Result<SymbolTable> {
		let mut symbols = SymbolTable::new();
		populate_symbols(&mut symbols, params)?;
		populate_identity(&mut symbols, &self.catalog, tx)?;
		Ok(symbols)
	}

	#[instrument(name = "executor::compile", level = "debug", skip(self, tx), fields(rql = %rql))]
	fn compile_query(&self, tx: &mut Transaction<'_>, rql: &str) -> Result<CompilationResult> {
		self.compiler.compile_with_policy(tx, rql, inject_from_policies)
	}

	#[instrument(name = "executor::rql", level = "debug", skip(self, tx, params), fields(rql = %rql))]
	pub fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> ExecutionResult {
		let symbols = match self.setup_symbols(&params, tx) {
			Ok(s) => s,
			Err(e) => return error_result(e, ExecutionMetrics::default()),
		};

		let start_compile = self.0.runtime_context.clock.instant();
		let compiled_list = match self.compile_query(tx, rql) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("incremental compilation not supported in rql()")
			}
			Err(err) => return self.handle_rql_compile_error(err, rql, params),
		};
		let compile_duration = Duration::from_std(start_compile.elapsed());

		match execute_compiled_units(&self.0, tx, &compiled_list, &params, symbols, compile_duration) {
			Ok((output, last, saw_output, _, metrics)) => {
				select_frames(saw_output, output, last).into_result(build_metrics(metrics))
			}
			Err(f) => error_result(f.error, build_metrics(f.partial_metrics)),
		}
	}

	#[inline]
	#[cfg_attr(reifydb_single_threaded, allow(unused_variables))]
	fn handle_rql_compile_error(&self, err: Error, rql: &str, params: Params) -> ExecutionResult {
		#[cfg(not(reifydb_single_threaded))]
		if let Ok(Some(frames)) = self.try_forward_remote_query(&err, rql, params) {
			return Emitted::forwarded(frames).into_result(ExecutionMetrics::default());
		}
		error_result(err, ExecutionMetrics::default())
	}

	#[instrument(name = "executor::admin", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn admin(&self, txn: &mut AdminTransaction, cmd: Admin<'_>) -> ExecutionResult {
		let symbols = match self.setup_symbols(&cmd.params, &mut Transaction::Admin(&mut *txn)) {
			Ok(s) => s,
			Err(e) => return error_result(e, ExecutionMetrics::default()),
		};
		if let Err(e) = self.enforce_admin_policy(&symbols, txn) {
			return error_result(e, ExecutionMetrics::default());
		}
		let start_compile = self.0.runtime_context.clock.instant();
		match self.compile_query(&mut Transaction::Admin(txn), cmd.rql) {
			Err(err) => self.handle_admin_compile_error(err, cmd.rql, cmd.params),
			Ok(CompilationResult::Ready(compiled)) => {
				self.execute_admin_ready(txn, compiled, &cmd.params, symbols, start_compile)
			}
			Ok(CompilationResult::Incremental(state)) => {
				self.execute_admin_incremental(txn, state, &cmd.params, symbols)
			}
		}
	}

	#[inline]
	fn enforce_admin_policy(&self, symbols: &SymbolTable, txn: &mut AdminTransaction) -> Result<()> {
		PolicyEvaluator::new(&self.0, symbols).enforce_session_policy(
			&mut Transaction::Admin(txn),
			SessionOp::Admin,
			true,
		)
	}

	#[inline]
	#[cfg_attr(reifydb_single_threaded, allow(unused_variables))]
	fn handle_admin_compile_error(&self, err: Error, rql: &str, params: Params) -> ExecutionResult {
		#[cfg(not(reifydb_single_threaded))]
		if let Ok(Some(frames)) = self.try_forward_remote_query(&err, rql, params) {
			return Emitted::forwarded(frames).into_result(ExecutionMetrics::default());
		}
		error_result(err, ExecutionMetrics::default())
	}

	#[inline]
	fn execute_admin_ready(
		&self,
		txn: &mut AdminTransaction,
		compiled: Arc<Vec<Compiled>>,
		params: &Params,
		symbols: SymbolTable,
		start_compile: Instant,
	) -> ExecutionResult {
		let compile_duration = Duration::from_std(start_compile.elapsed());
		match execute_compiled_units(
			&self.0,
			&mut Transaction::Admin(txn),
			&compiled,
			params,
			symbols,
			compile_duration,
		) {
			Ok((output, last, saw_output, _, metrics)) => {
				select_frames(saw_output, output, last).into_result(build_metrics(metrics))
			}
			Err(f) => ExecutionResult {
				frames: vec![],
				named_system_columns: vec![],
				error: Some(f.error),
				metrics: build_metrics(f.partial_metrics),
			},
		}
	}

	fn execute_admin_incremental(
		&self,
		txn: &mut AdminTransaction,
		mut state: IncrementalCompilation,
		params: &Params,
		symbols: SymbolTable,
	) -> ExecutionResult {
		let policy = constrain_policy(inject_from_policies);
		let mut result = vec![];
		let mut output_results = Emitted::default();
		let mut last_named: Vec<SystemColumn> = Vec::new();
		let mut saw_output = false;
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
				Err(e) => return error_result(e, build_metrics(metrics)),
			};
			let compile_duration = Duration::from_std(start_incr.elapsed());

			let Some(compiled) = next else {
				break;
			};

			result.clear();
			let mut tx = Transaction::Admin(txn);
			let mut vm = Vm::from_services(symbols, &self.0, params, tx.identity());
			let start_execute = self.0.runtime_context.clock.instant();
			let run_result = vm.run(&self.0, &mut tx, &compiled.instructions, &mut result);
			let execute_duration = Duration::from_std(start_execute.elapsed());
			symbols = vm.symbols;

			let rows_affected = match &run_result {
				Ok(()) => match extract_rows_affected(&result) {
					Ok(n) => n,
					Err(e) => return error_result(e, build_metrics(metrics)),
				},
				Err(_) => 0,
			};
			metrics.push(StatementMetrics {
				fingerprint: compiled.fingerprint,
				normalized_rql: compiled.normalized_rql,
				compile_duration,
				execute_duration,
				rows_affected,
			});

			if let Err(e) = run_result {
				return error_result(e, build_metrics(metrics));
			}

			if compiled.is_output {
				saw_output = true;
				output_results.append(&mut result, &compiled.named_system_columns);
			}
			last_named = compiled.named_system_columns;
		}
		let mut last = Emitted::default();
		last.append(&mut result, &last_named);
		select_frames(saw_output, output_results, last).into_result(build_metrics(metrics))
	}

	#[instrument(name = "executor::test", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn test(&self, txn: &mut TestTransaction<'_>, cmd: Test<'_>) -> ExecutionResult {
		let symbols = match self.setup_symbols(&cmd.params, &mut Transaction::Test(Box::new(txn.reborrow()))) {
			Ok(s) => s,
			Err(e) => return error_result(e, ExecutionMetrics::default()),
		};
		if let Err(e) = self.enforce_test_policy(&symbols, txn) {
			return error_result(e, ExecutionMetrics::default());
		}
		let start_compile = self.0.runtime_context.clock.instant();
		match self.compiler.compile_with_policy(
			&mut Transaction::Test(Box::new(txn.reborrow())),
			cmd.rql,
			inject_from_policies,
		) {
			Err(err) => self.handle_test_compile_error(err, cmd.rql, cmd.params),
			Ok(CompilationResult::Ready(compiled)) => {
				self.execute_test_ready(txn, compiled, &cmd.params, symbols, start_compile)
			}
			Ok(CompilationResult::Incremental(state)) => {
				self.execute_test_incremental(txn, state, &cmd.params, symbols)
			}
		}
	}

	#[inline]
	fn enforce_test_policy(&self, symbols: &SymbolTable, txn: &mut TestTransaction<'_>) -> Result<()> {
		let session_type = txn.session_type;
		let session_default_deny = txn.session_default_deny;
		PolicyEvaluator::new(&self.0, symbols).enforce_session_policy(
			&mut Transaction::Test(Box::new(txn.reborrow())),
			session_type,
			session_default_deny,
		)
	}

	#[inline]
	#[cfg_attr(reifydb_single_threaded, allow(unused_variables))]
	fn handle_test_compile_error(&self, err: Error, rql: &str, params: Params) -> ExecutionResult {
		#[cfg(not(reifydb_single_threaded))]
		if let Ok(Some(frames)) = self.try_forward_remote_query(&err, rql, params) {
			return Emitted::forwarded(frames).into_result(ExecutionMetrics::default());
		}
		error_result(err, ExecutionMetrics::default())
	}

	#[inline]
	fn execute_test_ready(
		&self,
		txn: &mut TestTransaction<'_>,
		compiled: Arc<Vec<Compiled>>,
		params: &Params,
		symbols: SymbolTable,
		start_compile: Instant,
	) -> ExecutionResult {
		let compile_duration = Duration::from_std(start_compile.elapsed());
		match execute_compiled_units(
			&self.0,
			&mut Transaction::Test(Box::new(txn.reborrow())),
			&compiled,
			params,
			symbols,
			compile_duration,
		) {
			Ok((output, last, saw_output, _, metrics)) => {
				select_frames(saw_output, output, last).into_result(build_metrics(metrics))
			}
			Err(f) => ExecutionResult {
				frames: vec![],
				named_system_columns: vec![],
				error: Some(f.error),
				metrics: build_metrics(f.partial_metrics),
			},
		}
	}

	fn execute_test_incremental(
		&self,
		txn: &mut TestTransaction<'_>,
		mut state: IncrementalCompilation,
		params: &Params,
		symbols: SymbolTable,
	) -> ExecutionResult {
		let policy = constrain_policy(inject_from_policies);
		let mut result = vec![];
		let mut output_results = Emitted::default();
		let mut last_named: Vec<SystemColumn> = Vec::new();
		let mut saw_output = false;
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
				Err(e) => return error_result(e, build_metrics(metrics)),
			};
			let compile_duration = Duration::from_std(start_incr.elapsed());

			let Some(compiled) = next else {
				break;
			};

			result.clear();
			let mut tx = Transaction::Test(Box::new(txn.reborrow()));
			let mut vm = Vm::from_services(symbols, &self.0, params, tx.identity());
			let start_execute = self.0.runtime_context.clock.instant();
			let run_result = vm.run(&self.0, &mut tx, &compiled.instructions, &mut result);
			let execute_duration = Duration::from_std(start_execute.elapsed());
			symbols = vm.symbols;

			let rows_affected = match &run_result {
				Ok(()) => match extract_rows_affected(&result) {
					Ok(n) => n,
					Err(e) => return error_result(e, build_metrics(metrics)),
				},
				Err(_) => 0,
			};
			metrics.push(StatementMetrics {
				fingerprint: compiled.fingerprint,
				normalized_rql: compiled.normalized_rql,
				compile_duration,
				execute_duration,
				rows_affected,
			});

			if let Err(e) = run_result {
				return error_result(e, build_metrics(metrics));
			}

			if compiled.is_output {
				saw_output = true;
				output_results.append(&mut result, &compiled.named_system_columns);
			}
			last_named = compiled.named_system_columns;
		}
		let mut last = Emitted::default();
		last.append(&mut result, &last_named);
		select_frames(saw_output, output_results, last).into_result(build_metrics(metrics))
	}

	#[instrument(name = "executor::subscribe", level = "debug", skip(self, txn, params, options), fields(query = %query))]
	pub fn subscribe(
		&self,
		txn: &mut QueryTransaction,
		query: &str,
		params: Params,
		options: SubscribeOptions,
	) -> Result<SubscribeOutcome> {
		if options.hydration.max_rows == Some(0) {
			return Err(Error(Box::new(subscription::hydration_max_rows_zero())));
		}
		if let Some(throttle) = options.throttle.filter(Duration::is_negative) {
			return Err(Error(Box::new(subscription::negative_throttle(throttle))));
		}
		if let Some(linger) = options.linger.filter(Duration::is_negative) {
			return Err(Error(Box::new(subscription::negative_linger(linger))));
		}
		let bump = Bump::new();
		let mut statements = parse_str(&bump, query)?;
		if statements.len() != 1 {
			return Err(Error(Box::new(subscription::single_statement_required(
				"Subscription endpoint requires exactly one statement",
			))));
		}

		let symbols = self.setup_symbols(&params, &mut Transaction::Query(&mut *txn))?;
		PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Query(&mut *txn),
			SessionOp::Subscription,
			true,
		)?;

		let mut tx = Transaction::Query(txn);
		let Some((plan, named_system_columns)) = self.compiler.compile_query_plan_with_policy(
			&bump,
			&mut tx,
			statements.remove(0),
			inject_from_policies,
		)?
		else {
			return Err(Error(Box::new(subscription::single_statement_required(
				"Subscription endpoint requires exactly one statement",
			))));
		};

		let plan = match plan {
			QueryPlan::RemoteScan(remote) => {
				return Ok(SubscribeOutcome::Remote {
					address: remote.address,
					body: remote.remote_rql,
					token: remote.token,
				});
			}
			plan => plan,
		};

		let sub_service = self.ioc.resolve::<SubscriptionServiceRef>()?;
		let id = sub_service.next_id();
		let flow_dag = compile_subscription_flow_ephemeral(
			&self.catalog,
			&self.routines,
			&mut tx,
			plan,
			id,
			FlowId(id.0),
		)?;
		let ctx = SubscriptionContext {
			id,
			identity: tx.identity(),
			symbols,
			params,
			named_system_columns,
		};
		sub_service.register_subscription(flow_dag, options.hydration.enabled, ctx, &mut tx)?;
		Ok(SubscribeOutcome::Local {
			id,
		})
	}

	#[instrument(name = "executor::command", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn command(&self, txn: &mut CommandTransaction, cmd: Command<'_>) -> ExecutionResult {
		let symbols = match self.setup_symbols(&cmd.params, &mut Transaction::Command(&mut *txn)) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					named_system_columns: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		if let Err(e) = PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Command(&mut *txn),
			SessionOp::Command,
			false,
		) {
			return ExecutionResult {
				frames: vec![],
				named_system_columns: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}

		let start_compile = self.0.runtime_context.clock.instant();
		let compiled = match self.compile_query(&mut Transaction::Command(txn), cmd.rql) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("DDL statements require admin transactions, not command transactions")
			}
			Err(err) => {
				#[cfg(not(reifydb_single_threaded))]
				if self.0.remote_registry.is_some() && remote::is_remote_query(&err) {
					return ExecutionResult {
						frames: vec![],
						named_system_columns: vec![],
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
					named_system_columns: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let compile_duration = Duration::from_std(start_compile.elapsed());

		match execute_compiled_units(
			&self.0,
			&mut Transaction::Command(txn),
			&compiled,
			&cmd.params,
			symbols,
			compile_duration,
		) {
			Ok((output, last, saw_output, _, metrics)) => {
				select_frames(saw_output, output, last).into_result(build_metrics(metrics))
			}
			Err(f) => ExecutionResult {
				frames: vec![],
				named_system_columns: vec![],
				error: Some(f.error),
				metrics: build_metrics(f.partial_metrics),
			},
		}
	}

	#[instrument(name = "executor::query", level = "debug", skip(self, txn, qry), fields(rql = %qry.rql))]
	pub fn query(&self, txn: &mut QueryTransaction, qry: Query<'_>) -> ExecutionResult {
		let probe_setup = std::time::Instant::now();
		let symbols = match self.setup_symbols(&qry.params, &mut Transaction::Query(&mut *txn)) {
			Ok(s) => s,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					named_system_columns: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};

		if let Err(e) = PolicyEvaluator::new(&self.0, &symbols).enforce_session_policy(
			&mut Transaction::Query(&mut *txn),
			SessionOp::Query,
			false,
		) {
			return ExecutionResult {
				frames: vec![],
				named_system_columns: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}

		crate::probe::add(&crate::probe::SETUP_NS, probe_setup);
		let probe_compile = std::time::Instant::now();
		let start_compile = self.0.runtime_context.clock.instant();
		let compiled = match self.compile_query(&mut Transaction::Query(txn), qry.rql) {
			Ok(CompilationResult::Ready(compiled)) => compiled,
			Ok(CompilationResult::Incremental(_)) => {
				unreachable!("DDL statements require admin transactions, not query transactions")
			}
			Err(err) => {
				#[cfg(not(reifydb_single_threaded))]
				if let Ok(Some(frames)) = self.try_forward_remote_query(&err, qry.rql, qry.params) {
					return Emitted::forwarded(frames).into_result(ExecutionMetrics::default());
				}
				return ExecutionResult {
					frames: vec![],
					named_system_columns: vec![],
					error: Some(err),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let compile_duration = Duration::from_std(start_compile.elapsed());
		crate::probe::add(&crate::probe::COMPILE_NS, probe_compile);

		let probe_execute = std::time::Instant::now();
		let exec_result = execute_compiled_units(
			&self.0,
			&mut Transaction::Query(txn),
			&compiled,
			&qry.params,
			symbols,
			compile_duration,
		);
		crate::probe::add(&crate::probe::EXECUTE_NS, probe_execute);
		crate::probe::finish_query();

		match exec_result {
			Ok((output, last, saw_output, _, metrics)) => {
				select_frames(saw_output, output, last).into_result(build_metrics(metrics))
			}
			Err(f) => ExecutionResult {
				frames: vec![],
				named_system_columns: vec![],
				error: Some(f.error),
				metrics: build_metrics(f.partial_metrics),
			},
		}
	}
}
