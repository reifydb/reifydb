// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::none;
use reifydb_routine::function::math::{add::basic::Add, clamp::Clamp, power::Power};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{column_view::ColumnView, identity::IdentityId},
};

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("math"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn call(function: impl Function, args: Vec<(FieldRef, ArrayRef)>) -> (FieldRef, ArrayRef) {
	let row_count = args.first().map_or(0, |(_, array)| array.len());
	function.call(&mut ctx(row_count), &args).unwrap()
}

#[test]
fn math_on_only_untyped_nones_answers_a_none_column() {
	// Otherwise the answer is an any column of nones that a typed branch beside it can not merge with.
	let cases = [
		("add", call(Add::new(), vec![none("arg0", 2), none("arg1", 2)])),
		("power", call(Power::new(), vec![none("arg0", 2), none("arg1", 2)])),
		("clamp", call(Clamp::new(), vec![none("arg0", 2), none("arg1", 2), none("arg2", 2)])),
	];

	let wrong: Vec<String> = cases
		.iter()
		.map(|(name, result)| (name, ColumnView::try_from(result).unwrap()))
		.filter(|(_, result)| !result.is_none() || result.len() != 2)
		.map(|(name, result)| format!("{name}: {:?} of {} rows", result.get_type(), result.len()))
		.collect();
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
