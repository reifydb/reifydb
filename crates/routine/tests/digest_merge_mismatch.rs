// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::{factory::from_one, view::group_by::GroupId};
use reifydb_routine::function::stats::digest::StatsDigest;
use reifydb_routine_abi::{Accumulator, Function, context::FunctionContext, error::RoutineError};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, digest::Digest, identity::IdentityId, value_type::ValueType},
};

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
	StatsDigest::new().accumulator(&mut ctx(), &[]).unwrap().expect("stats::digest is an aggregate")
}

fn digest_column(inner: ValueType, accuracy: u32, value: Value) -> Vec<(FieldRef, ArrayRef)> {
	let mut digest = Digest::new(inner, accuracy).unwrap();
	digest.add_value(&value).unwrap();
	vec![from_one("x", Value::Digest(Box::new(digest)))]
}

fn one_group() -> Vec<(GroupId, Vec<usize>)> {
	vec![(GroupId(0), vec![0])]
}

fn assert_merge_mismatch(result: Result<(), RoutineError>, message: &str) {
	match result {
		Err(RoutineError::Wrapped(err)) => {
			assert_eq!(err.code, "AGGREGATE_017", "got: {err:?}");
			assert_eq!(err.message, message, "the message must name both digest types in merge order");
		}
		other => panic!("expected AGGREGATE_017, got {other:?}"),
	}
}

#[test]
fn merging_digests_with_different_accuracy_fails() {
	// One output column can never hold two digest types, so a second accuracy into the same group must be rejected.
	let mut acc = accumulator();
	acc.update(&digest_column(ValueType::Float8, 10_000, Value::float8(1.5)), &one_group()).unwrap();

	assert_merge_mismatch(
		acc.update(&digest_column(ValueType::Float8, 50_000, Value::float8(7.0)), &one_group()),
		"cannot merge Digest(Float8, 0.01) with Digest(Float8, 0.05)",
	);
}

#[test]
fn merging_digests_with_different_input_types_fails() {
	// An int4 digest and a float8 digest must not merge, or the output column would hold two digest types.
	let mut acc = accumulator();
	acc.update(&digest_column(ValueType::Int4, 10_000, Value::Int4(3)), &one_group()).unwrap();

	assert_merge_mismatch(
		acc.update(&digest_column(ValueType::Float8, 10_000, Value::float8(7.0)), &one_group()),
		"cannot merge Digest(Int4, 0.01) with Digest(Float8, 0.01)",
	);
}
