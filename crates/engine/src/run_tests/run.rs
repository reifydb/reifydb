// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_core::{
	interface::catalog::policy::SessionOp,
	internal_error,
	testing::{CapturedEvent, CapturedInvocation},
	value::batch::{concat, single_row},
};
use reifydb_evaluate::stack::Variable;
use reifydb_rql::{
	compiler::CompilationResult,
	nodes::{RunTestsNode, RunTestsScope},
};
use reifydb_transaction::transaction::{TestTransaction, Transaction};
use reifydb_value::value::{
	Value, column_view::ColumnView, duration::Duration, frame::frame::Frame, system_columns::user_columns,
};

use crate::{
	Result,
	run_tests::result::{TestOutcome, classify_outcome},
	vm::{services::Services, vm::Vm},
};

fn run_single(
	vm: &mut Vm,
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	body: &str,
	named_vars: Option<&HashMap<String, Value>>,
) -> (String, String) {
	match services.compiler.compile(txn, body) {
		Ok(compiled) => match compiled {
			CompilationResult::Ready(compiled_list) => {
				let saved_ip = vm.ip;
				let mut exec_error = None;

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
					if let Err(e) =
						vm.run(services, txn, &compiled_unit.instructions, &mut test_result)
					{
						exec_error = Some(e);
						break;
					}
				}

				vm.ip = saved_ip;

				if let Some(ref mut e) = exec_error {
					e.with_rql(body.to_string());
				}

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
		Err(mut e) => {
			e.with_rql(body.to_string());
			("error".to_string(), format!("{}", e))
		}
	}
}

fn resolve_params(vm: &mut Vm, services: &Arc<Services>, txn: &mut Transaction<'_>, source: &str) -> Result<Frame> {
	let query = format!("FROM {}", source);
	let compiled = services.compiler.compile(txn, &query)?;
	match compiled {
		CompilationResult::Ready(compiled_list) => {
			let saved_ip = vm.ip;
			let mut frames = Vec::new();

			for compiled_unit in compiled_list.iter() {
				vm.ip = 0;
				vm.run(services, txn, &compiled_unit.instructions, &mut frames)?;
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
) -> Result<RecordBatch> {
	let txn = match tx {
		Transaction::Admin(txn) => txn,
		Transaction::Test(t) => &mut *t.inner,
		_ => {
			return Err(internal_error!("RUN TESTS requires an admin transaction"));
		}
	};

	let mut events: Vec<CapturedEvent> = Vec::new();
	let mut invocations: Vec<CapturedInvocation> = Vec::new();
	let mut event_seq: u64 = 0;
	let mut handler_seq: u64 = 0;

	let mut tests = match &plan.scope {
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
	tests.sort_by(|a, b| a.name.cmp(&b.name));

	if tests.is_empty() {
		return single_row([
			("name", Value::Utf8("(no tests found)".to_string())),
			("namespace", Value::Utf8("".to_string())),
			("outcome", Value::Utf8("skip".to_string())),
			("duration", Value::Duration(Duration::zero())),
			("message", Value::Utf8("".to_string())),
		]);
	}

	let mut result_rows = Vec::new();

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
				events.clear();
				invocations.clear();
				_ = mem::replace(&mut event_seq, 0);
				_ = mem::replace(&mut handler_seq, 0);

				let start = services.runtime_context.clock.instant();
				let mut test_txn = TestTransaction::new(
					&mut *txn,
					&mut events,
					&mut invocations,
					&mut event_seq,
					&mut handler_seq,
					SessionOp::Admin,
					true,
				);
				let (outcome, message) = run_single(
					vm,
					services,
					&mut Transaction::Test(Box::new(test_txn.reborrow())),
					&test.body,
					None,
				);
				test_txn.restore();
				let elapsed = start.elapsed();
				let duration = Duration::from_nanoseconds(elapsed.as_nanos() as i64)?;

				result_rows.push(single_row([
					("name", Value::Utf8(test.name.clone())),
					("namespace", Value::Utf8(ns_name.clone())),
					("outcome", Value::Utf8(outcome)),
					("duration", Value::Duration(duration)),
					("message", Value::Utf8(message)),
				])?);
			}
			Some(source) => {
				let cases_frame =
					resolve_params(vm, services, &mut Transaction::Admin(&mut *txn), source)?;

				let case_columns = user_columns(&cases_frame.batch)
					.map(|(field, array)| ColumnView::try_from((array, field.as_ref())))
					.collect::<Result<Vec<_>>>()?;
				let col_names: Vec<String> =
					case_columns.iter().map(|c| c.field.name().clone()).collect();

				let row_count = case_columns.first().map_or(0, |c| c.len());

				for row_idx in 0..row_count {
					let row_values: Vec<Value> =
						case_columns.iter().map(|c| c.get_value(row_idx)).collect();
					let row_label = format_row_label(&col_names, &row_values);

					let mut named_vars = HashMap::new();
					for (name, value) in col_names.iter().zip(row_values) {
						named_vars.insert(name.clone(), value);
					}

					events.clear();
					invocations.clear();
					event_seq = 0;
					handler_seq = 0;

					let start = services.runtime_context.clock.instant();
					let mut test_txn = TestTransaction::new(
						&mut *txn,
						&mut events,
						&mut invocations,
						&mut event_seq,
						&mut handler_seq,
						SessionOp::Admin,
						true,
					);
					let (outcome, message) = run_single(
						vm,
						services,
						&mut Transaction::Test(Box::new(test_txn.reborrow())),
						&test.body,
						Some(&named_vars),
					);
					test_txn.restore();
					let elapsed = start.elapsed();
					let duration = Duration::from_nanoseconds(elapsed.as_nanos() as i64)?;

					let display_name = format!("{} {}", test.name, row_label);

					result_rows.push(single_row([
						("name", Value::Utf8(display_name)),
						("namespace", Value::Utf8(ns_name.clone())),
						("outcome", Value::Utf8(outcome)),
						("duration", Value::Duration(duration)),
						("message", Value::Utf8(message)),
					])?);
				}
			}
		}
	}

	concat(&result_rows)
}
