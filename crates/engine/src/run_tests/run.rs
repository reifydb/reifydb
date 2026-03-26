// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use reifydb_core::{
	internal_error,
	testing::{CapturedEvent, HandlerInvocation},
	value::column::columns::Columns,
};
use reifydb_rql::{
	compiler::CompilationResult,
	nodes::{RunTestsNode, RunTestsScope},
};
use reifydb_transaction::transaction::{TestTransaction, Transaction};
use reifydb_type::{
	params::Params,
	value::{Value, duration::Duration as RqlDuration, frame::frame::Frame},
};

use crate::{
	Result,
	run_tests::result::{TestOutcome, classify_outcome},
	vm::{services::Services, stack::Variable, vm::Vm},
};

/// Run a single test invocation (body compiled + executed with given params).
/// If `named_vars` is provided, injects them as variables before execution.
/// Returns (outcome, message).
fn run_single(
	vm: &mut Vm,
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	body: &str,
	params: &Params,
	named_vars: Option<&HashMap<String, Value>>,
) -> (String, String) {
	match services.compiler.compile(txn, body) {
		Ok(compiled) => match compiled {
			CompilationResult::Ready(compiled_list) => {
				let saved_ip = vm.ip;
				let mut exec_error = None;

				// Inject named variables into the symbol table
				if let Some(vars) = named_vars {
					for (name, value) in vars {
						if let Err(e) = vm.symbols.set(
							name.clone(),
							Variable::scalar(value.clone()),
							false,
						) {
							return ("error".to_string(), format!("{}", e));
						}
					}
				}

				for compiled_unit in compiled_list.iter() {
					vm.ip = 0;
					let mut test_result = Vec::new();
					if let Err(e) = vm.run(
						services,
						txn,
						&compiled_unit.instructions,
						params,
						&mut test_result,
					) {
						exec_error = Some(e);
						break;
					}
				}

				vm.ip = saved_ip;

				match classify_outcome(match exec_error {
					None => Ok(()),
					Some(ref e) => Err(e),
				}) {
					TestOutcome::Pass => ("pass".to_string(), String::new()),
					TestOutcome::Fail(msg) => ("fail".to_string(), msg),
					TestOutcome::Error(msg) => ("error".to_string(), msg),
				}
			}
			CompilationResult::Incremental(_) => {
				("error".to_string(), "test body requires incremental compilation".to_string())
			}
		},
		Err(e) => ("error".to_string(), format!("{}", e)),
	}
}

/// Resolve params data from a cases string by compiling `FROM <source>` and executing it.
fn resolve_params(vm: &mut Vm, services: &Arc<Services>, txn: &mut Transaction<'_>, source: &str) -> Result<Frame> {
	let query = format!("FROM {}", source);
	let compiled = services.compiler.compile(txn, &query)?;
	match compiled {
		CompilationResult::Ready(compiled_list) => {
			let saved_ip = vm.ip;
			let mut frames = Vec::new();

			for compiled_unit in compiled_list.iter() {
				vm.ip = 0;
				vm.run(services, txn, &compiled_unit.instructions, &Params::None, &mut frames)?;
			}

			vm.ip = saved_ip;

			match frames.into_iter().last() {
				Some(frame) => Ok(frame),
				None => Err(internal_error!("params source produced no output")),
			}
		}
		CompilationResult::Incremental(_) => {
			Err(internal_error!("params source requires incremental compilation"))
		}
	}
}

/// Format a row label like `[x=1, expected=1]` for display in test names.
fn format_row_label(col_names: &[String], row_values: &[Value]) -> String {
	let pairs: Vec<String> =
		col_names.iter().zip(row_values.iter()).map(|(name, val)| format!("{}={}", name, val)).collect();
	format!("[{}]", pairs.join(", "))
}

pub(crate) fn run_tests(
	vm: &mut Vm,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	plan: RunTestsNode,
	params: &Params,
) -> Result<Columns> {
	let txn = match tx {
		Transaction::Admin(txn) => txn,
		Transaction::Test(t) => &mut *t.inner,
		_ => {
			return Err(internal_error!("RUN TESTS requires an admin transaction"));
		}
	};

	// Stack-allocated test state — passed into Transaction::Test by reference
	let mut events: Vec<CapturedEvent> = Vec::new();
	let mut handler_invocations: Vec<HandlerInvocation> = Vec::new();
	let mut event_seq: u64 = 0;
	let mut handler_seq: u64 = 0;

	let tests = match &plan.scope {
		RunTestsScope::All => services.catalog.list_all_tests(&mut Transaction::Admin(&mut *txn))?,
		RunTestsScope::Namespace(ns) => {
			services.catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut *txn), ns.def().id())?
		}
		RunTestsScope::Single(ns, name) => {
			match services.catalog.find_test_by_name(
				&mut Transaction::Admin(&mut *txn),
				ns.def().id(),
				name,
			)? {
				Some(test) => vec![test],
				None => vec![],
			}
		}
	};

	if tests.is_empty() {
		return Ok(Columns::single_row([
			("name", Value::Utf8("(no tests found)".to_string())),
			("namespace", Value::Utf8("".to_string())),
			("outcome", Value::Utf8("skip".to_string())),
			("duration", Value::Duration(RqlDuration::zero())),
			("message", Value::Utf8("".to_string())),
		]));
	}

	let mut result_columns = Columns::empty();

	for test in &tests {
		let ns_name = services
			.catalog
			.find_namespace(&mut Transaction::Admin(&mut *txn), test.namespace)
			.ok()
			.flatten()
			.map(|ns| ns.name().to_string())
			.unwrap_or_else(|| format!("{}", test.namespace.0));

		match &test.cases {
			None => {
				// Non-parameterized: single run
				events.clear();
				handler_invocations.clear();
				_ = mem::replace(&mut event_seq, 0);
				_ = mem::replace(&mut handler_seq, 0);

				let start = services.runtime_context.clock.instant();
				let savepoint = txn.savepoint();
				let baseline = txn.accumulator_len();
				let (outcome, message) = run_single(
					vm,
					services,
					&mut Transaction::Test(TestTransaction {
						inner: &mut *txn,
						baseline,
						events: &mut events,
						handler_invocations: &mut handler_invocations,
						event_seq: &mut event_seq,
						handler_seq: &mut handler_seq,
					}),
					&test.body,
					params,
					None,
				);
				txn.restore_savepoint(savepoint);
				let elapsed = start.elapsed();
				let duration = RqlDuration::from_nanoseconds(elapsed.as_nanos() as i64);

				let row = Columns::single_row([
					("name", Value::Utf8(test.name.clone())),
					("namespace", Value::Utf8(ns_name.clone())),
					("outcome", Value::Utf8(outcome)),
					("duration", Value::Duration(duration)),
					("message", Value::Utf8(message)),
				]);

				if result_columns.is_empty() {
					result_columns = row;
				} else {
					result_columns.append_columns(row)?;
				}
			}
			Some(source) => {
				// Parameterized: resolve params, iterate rows
				let cases_frame =
					resolve_params(vm, services, &mut Transaction::Admin(&mut *txn), source)?;

				let col_names: Vec<String> =
					cases_frame.columns.iter().map(|c| c.name.clone()).collect();

				let row_count = cases_frame.columns.first().map_or(0, |c| c.data.len());

				for row_idx in 0..row_count {
					let row_values: Vec<Value> =
						cases_frame.columns.iter().map(|c| c.data.get_value(row_idx)).collect();
					let row_label = format_row_label(&col_names, &row_values);

					// Build named variables from column names + row values
					let mut named_vars = HashMap::new();
					for (name, value) in col_names.iter().zip(row_values.into_iter()) {
						named_vars.insert(name.clone(), value);
					}

					events.clear();
					handler_invocations.clear();
					event_seq = 0;
					handler_seq = 0;

					let start = services.runtime_context.clock.instant();
					let savepoint = txn.savepoint();
					let baseline = txn.accumulator_len();
					let (outcome, message) = run_single(
						vm,
						services,
						&mut Transaction::Test(TestTransaction {
							inner: &mut *txn,
							baseline,
							events: &mut events,
							handler_invocations: &mut handler_invocations,
							event_seq: &mut event_seq,
							handler_seq: &mut handler_seq,
						}),
						&test.body,
						params,
						Some(&named_vars),
					);
					txn.restore_savepoint(savepoint);
					let elapsed = start.elapsed();
					let duration = RqlDuration::from_nanoseconds(elapsed.as_nanos() as i64);

					let display_name = format!("{} {}", test.name, row_label);

					let row = Columns::single_row([
						("name", Value::Utf8(display_name)),
						("namespace", Value::Utf8(ns_name.clone())),
						("outcome", Value::Utf8(outcome)),
						("duration", Value::Duration(duration)),
						("message", Value::Utf8(message)),
					]);

					if result_columns.is_empty() {
						result_columns = row;
					} else {
						result_columns.append_columns(row)?;
					}
				}
			}
		}
	}

	Ok(result_columns)
}
