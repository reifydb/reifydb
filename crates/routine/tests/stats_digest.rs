// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::LazyLock};

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::GroupId};
use reifydb_routine::function::stats::digest::StatsDigest;
use reifydb_routine_abi::{Accumulator, Function, LiteralArgument, LiteralKind, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{fragment::Fragment, value::identity::IdentityId};

fn ctx() -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("stats::digest"),
		identity: IdentityId::root(),
		row_count: 0,
		runtime_context: &RUNTIME,
	}
}

fn accumulator() -> Box<dyn Accumulator> {
	let literal = LiteralArgument {
		kind: LiteralKind::Number,
		fragment: Fragment::internal("0.01"),
	};
	StatsDigest::new().accumulator(&mut ctx(), &[literal]).unwrap().expect("stats::digest must be an aggregate")
}

fn add(accumulator: &mut Box<dyn Accumulator>, value: f64) -> usize {
	let column = ColumnWithName::new(Fragment::internal("x"), ColumnBuffer::float8(vec![value]));
	accumulator.update(&Columns::new(vec![column]), &vec![(GroupId(0), vec![0])]).unwrap();
	accumulator.heap_size()
}

#[test]
fn heap_size_grows_by_one_bucket_only_when_a_value_opens_a_new_bucket() {
	// Query memory must see digest growth, and a repeated value must not be charged as a new bucket.
	let mut accumulator = accumulator();

	let first = add(&mut accumulator, 1.5);
	let repeated = add(&mut accumulator, 1.5);
	let outlier = add(&mut accumulator, 1.0e9);

	let bucket = mem::size_of::<i32>() + mem::size_of::<u64>();
	assert_eq!(repeated, first, "a value in an occupied bucket adds no heap");
	assert_eq!(outlier, first + bucket, "a far value adds exactly one bucket");
}
