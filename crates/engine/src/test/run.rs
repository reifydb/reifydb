// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Instant};

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_rql::{
	compiler::CompilationResult,
	nodes::{RunTestsNode, RunTestsScope},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	params::Params,
	value::{Value, duration::Duration as RqlDuration},
};

use crate::{
	Result,
	test::result::{TestOutcome, classify_outcome},
	vm::{services::Services, vm::Vm},
};

pub(crate) fn run_tests(
	vm: &mut Vm,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	plan: RunTestsNode,
	params: &Params,
) -> Result<Columns> {
	let txn = match tx {
		Transaction::Admin(txn) => txn,
		_ => {
			return Err(internal_error!("RUN TESTS requires an admin transaction"));
		}
	};

	let tests = match &plan.scope {
		RunTestsScope::All => services.catalog.list_all_tests(&mut Transaction::Admin(&mut *txn))?,
		RunTestsScope::Namespace(ns) => {
			services.catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut *txn), ns.def().id)?
		}
		RunTestsScope::Single(ns, name) => {
			match services.catalog.find_test_by_name(
				&mut Transaction::Admin(&mut *txn),
				ns.def().id,
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

	for test_def in &tests {
		let ns_name = services
			.catalog
			.find_namespace(&mut Transaction::Admin(&mut *txn), test_def.namespace)
			.ok()
			.flatten()
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("{}", test_def.namespace.0));

		let start = Instant::now();

		// Compile and execute the test body, following the migrate pattern
		let outcome;
		let message;

		match services.compiler.compile(&mut Transaction::Admin(&mut *txn), &test_def.body) {
			Ok(compiled) => match compiled {
				CompilationResult::Ready(compiled_list) => {
					let saved_ip = vm.ip;
					let mut exec_error = None;

					for compiled_unit in compiled_list.iter() {
						vm.ip = 0;
						let mut test_result = Vec::new();
						if let Err(e) = vm.run(
							services,
							&mut Transaction::Admin(&mut *txn),
							&compiled_unit.instructions,
							params,
							&mut test_result,
						) {
							exec_error = Some(e);
							break;
						}
					}

					vm.ip = saved_ip;

					let test_outcome = match exec_error {
						None => classify_outcome(Ok(())),
						Some(ref e) => classify_outcome(Err(e)),
					};
					match &test_outcome {
						TestOutcome::Pass => {
							outcome = "pass".to_string();
							message = String::new();
						}
						TestOutcome::Fail(msg) => {
							outcome = "fail".to_string();
							message = msg.clone();
						}
						TestOutcome::Error(msg) => {
							outcome = "error".to_string();
							message = msg.clone();
						}
					}
				}
				CompilationResult::Incremental(_) => {
					outcome = "error".to_string();
					message = "test body requires incremental compilation".to_string();
				}
			},
			Err(e) => {
				outcome = "error".to_string();
				message = format!("{}", e);
			}
		}

		let elapsed = start.elapsed();
		let duration = RqlDuration::from_nanoseconds(elapsed.as_nanos() as i64);

		let row = Columns::single_row([
			("name", Value::Utf8(test_def.name.clone())),
			("namespace", Value::Utf8(ns_name)),
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

	Ok(result_columns)
}
