// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::LazyLock};

use reifydb_core::value::column::{
	factory::{float8, from_many},
	view::group_by::GroupId,
};
use reifydb_routine::function::stats::digest::StatsDigest;
use reifydb_routine_abi::{Accumulator, Function, LiteralArgument, LiteralKind, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	error::Error,
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
	let literal = LiteralArgument {
		kind: LiteralKind::Number,
		fragment: Fragment::internal("0.01"),
	};
	StatsDigest::new().accumulator(&mut ctx(), &[literal]).unwrap().expect("stats::digest must be an aggregate")
}

fn add(accumulator: &mut Box<dyn Accumulator>, value: f64) -> usize {
	let column = float8("x", vec![value]);
	accumulator.update(&[column], &vec![(GroupId(0), vec![0])]).unwrap();
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

fn merging_accumulator() -> Box<dyn Accumulator> {
	StatsDigest::new().accumulator(&mut ctx(), &[]).unwrap().expect("stats::digest must be an aggregate")
}

fn digest(inner: ValueType, accuracy: u32, value: Value) -> Value {
	let mut digest = Digest::new(inner, accuracy).unwrap();
	digest.add_value(&value).unwrap();
	Value::Digest(Box::new(digest))
}

fn merge(accumulator: &mut Box<dyn Accumulator>, digest: Value) -> Result<(), Error> {
	let column = from_many("x", digest, 1);
	accumulator.update(&[column], &vec![(GroupId(0), vec![0])]).map_err(Error::from)
}

#[test]
fn merging_digests_with_different_accuracy_fails() {
	// One output column can never hold two digest types, so the second accuracy must be refused by name.
	let mut accumulator = merging_accumulator();

	merge(&mut accumulator, digest(ValueType::Float8, 10_000, Value::float8(1.0))).unwrap();
	let err = merge(&mut accumulator, digest(ValueType::Float8, 50_000, Value::float8(2.0))).unwrap_err();

	assert_eq!(err.0.code, "AGGREGATE_017", "got: {err:?}");
	assert_eq!(err.0.message, "cannot merge Digest(Float8, 0.01) with Digest(Float8, 0.05)");
}

#[test]
fn merging_digests_with_different_input_types_fails() {
	// An int4 digest and a float8 digest must not merge, or the output column would hold two digest types.
	let mut accumulator = merging_accumulator();

	merge(&mut accumulator, digest(ValueType::Int4, 10_000, Value::Int4(1))).unwrap();
	let err = merge(&mut accumulator, digest(ValueType::Float8, 10_000, Value::float8(2.0))).unwrap_err();

	assert_eq!(err.0.code, "AGGREGATE_017", "got: {err:?}");
	assert_eq!(err.0.message, "cannot merge Digest(Int4, 0.01) with Digest(Float8, 0.01)");
}
