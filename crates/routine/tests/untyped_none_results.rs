// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::function::math::{add::basic::Add, clamp::Clamp, power::Power};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{fragment::Fragment, value::identity::IdentityId};

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("math"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn call(function: impl Function, args: Vec<ColumnBuffer>) -> ColumnBuffer {
	let row_count = args.first().map_or(0, ColumnBuffer::len);
	let columns = Columns::new(
		args.into_iter()
			.enumerate()
			.map(|(i, data)| ColumnWithName::new(Fragment::internal(format!("arg{i}")), data))
			.collect(),
	);
	let result = function.call(&mut ctx(row_count), &columns).unwrap();
	result.data_at(0).clone()
}

#[test]
fn math_on_only_untyped_nones_answers_a_none_column() {
	// Otherwise the answer is an any column of nones that a typed branch beside it can not merge with.
	let cases = [
		("add", call(Add::new(), vec![ColumnBuffer::none(2), ColumnBuffer::none(2)])),
		("power", call(Power::new(), vec![ColumnBuffer::none(2), ColumnBuffer::none(2)])),
		(
			"clamp",
			call(Clamp::new(), vec![ColumnBuffer::none(2), ColumnBuffer::none(2), ColumnBuffer::none(2)]),
		),
	];

	let wrong: Vec<String> = cases
		.iter()
		.filter(|(_, result)| !result.is_none() || result.len() != 2)
		.map(|(name, result)| format!("{name}: {:?} of {} rows", result.get_type(), result.len()))
		.collect();
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
