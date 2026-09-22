// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns};
use reifydb_routine::function::{default_in_process_functions, stats::approx_percentile::ApproxPercentile};
use reifydb_routine_abi::{Function, Routine, context::FunctionContext, error::RoutineError, registry::Routines};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, digest::Digest, duration::Duration, identity::IdentityId, value_type::ValueType},
};

const ACCURACY: u32 = 10_000;

fn ctx(row_count: usize) -> FunctionContext<'static> {
	static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
	FunctionContext {
		fragment: Fragment::internal("stats::approx_percentile"),
		identity: IdentityId::root(),
		row_count,
		runtime_context: &RUNTIME,
	}
}

fn digest_type(inner: ValueType) -> ValueType {
	ValueType::Digest {
		inner: Box::new(inner),
		accuracy: ACCURACY,
	}
}

fn digest_of(inner: ValueType, values: impl IntoIterator<Item = Value>) -> Digest {
	let mut digest = Digest::new(inner, ACCURACY).unwrap();
	for value in values {
		digest.add_value(&value).unwrap();
	}
	digest
}

fn float_digest(values: impl IntoIterator<Item = f64>) -> Digest {
	digest_of(ValueType::Float8, values.into_iter().map(Value::float8))
}

fn duration_digest(millis: impl IntoIterator<Item = i64>) -> Digest {
	digest_of(
		ValueType::Duration,
		millis.into_iter().map(|ms| Value::Duration(Duration::from_milliseconds(ms).unwrap())),
	)
}

fn digest_column(inner: ValueType, rows: &[Option<Digest>]) -> ColumnBuffer {
	let mut builder = ColumnBuilder::with_capacity(digest_type(inner), rows.len());
	for row in rows {
		match row {
			Some(digest) => builder.push_value(Value::Digest(Box::new(digest.clone()))),
			None => builder.push_none(),
		}
	}
	builder.finish()
}

fn column(values: impl IntoIterator<Item = Value>, ty: ValueType) -> ColumnBuffer {
	let values: Vec<Value> = values.into_iter().collect();
	let mut builder = ColumnBuilder::with_capacity(ty, values.len());
	for value in values {
		builder.push_value(value);
	}
	builder.finish()
}

fn call(args: Vec<ColumnBuffer>) -> Result<ColumnBuffer, RoutineError> {
	let row_count = args.first().map_or(0, ColumnBuffer::len);
	let columns = Columns::new(
		args.into_iter()
			.enumerate()
			.map(|(i, data)| ColumnWithName::new(Fragment::internal(format!("arg{i}")), data))
			.collect(),
	);
	let result = ApproxPercentile::new().call(&mut ctx(row_count), &columns)?;
	assert_eq!(result.len(), 1, "a scalar function must return exactly one column");
	Ok(result.data_at(0).clone())
}

fn reason(err: RoutineError) -> String {
	match err {
		RoutineError::FunctionExecutionFailed {
			reason,
			..
		} => reason,
		other => panic!("expected an execution failure, got {other:?}"),
	}
}

#[test]
fn any_p_per_row_equals_the_digest_oracle_for_a_float8_digest() {
	// Each row must read its own digest with its own p; reading one digest or one p for all rows fails this.
	let low = float_digest((1..=1000).map(f64::from));
	let high = float_digest((1..=1000).map(|v| f64::from(v) * 1000.0 - 250_000.0));
	let ps = [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 0.999, 1.0];
	let digests: Vec<Option<Digest>> = ps
		.iter()
		.enumerate()
		.map(|(i, _)| {
			Some(if i % 2 == 0 {
				low.clone()
			} else {
				high.clone()
			})
		})
		.collect();

	let out = call(vec![
		digest_column(ValueType::Float8, &digests),
		column(ps.iter().map(|p| Value::float8(*p)), ValueType::Float8),
	])
	.unwrap();

	assert_eq!(out.get_type(), ValueType::Float8);
	for (row, p) in ps.iter().enumerate() {
		let expected = digests[row].as_ref().unwrap().percentile_value(*p).unwrap();
		assert_eq!(out.get_value(row), expected, "row {row} with p {p}");
	}
	assert_ne!(out.get_value(2), out.get_value(3), "rows with different digests and p must not share an answer");
}

#[test]
fn any_p_per_row_equals_the_digest_oracle_for_a_duration_digest_and_returns_duration() {
	// A Duration digest must answer in Duration, never as a raw Float8 count of nanoseconds.
	let fast = duration_digest(1..=500);
	let slow = duration_digest((1..=500).map(|ms| ms * 60_000));
	let ps = [0.0, 0.1, 0.5, 0.9, 0.99, 1.0];
	let digests: Vec<Option<Digest>> = ps
		.iter()
		.enumerate()
		.map(|(i, _)| {
			Some(if i % 3 == 0 {
				slow.clone()
			} else {
				fast.clone()
			})
		})
		.collect();

	let out = call(vec![
		digest_column(ValueType::Duration, &digests),
		column(ps.iter().map(|p| Value::float8(*p)), ValueType::Float8),
	])
	.unwrap();

	assert_eq!(out.get_type(), ValueType::Duration);
	for (row, p) in ps.iter().enumerate() {
		let expected = digests[row].as_ref().unwrap().percentile_value(*p).unwrap();
		assert!(matches!(expected, Value::Duration(_)), "the oracle must give a Duration");
		assert_eq!(out.get_value(row), expected, "row {row} with p {p}");
	}
}

#[test]
fn an_integer_p_column_is_read_as_a_number() {
	// A p of 1 or 0 written without a decimal point is an integer column and must still work.
	let digest = float_digest([1.0, 2.0, 3.0, 4.0]);
	let digests = vec![Some(digest.clone()), Some(digest.clone())];

	let out = call(vec![
		digest_column(ValueType::Float8, &digests),
		column([Value::Int1(0), Value::Int1(1)], ValueType::Int1),
	])
	.unwrap();

	assert_eq!(out.get_value(0), digest.percentile_value(0.0).unwrap());
	assert_eq!(out.get_value(1), digest.percentile_value(1.0).unwrap());
}

#[test]
fn p_outside_zero_to_one_is_an_error_not_a_clamp() {
	// Clamping 1.5 to 1 would silently answer a different question.
	let digests = vec![Some(float_digest([1.0, 2.0])), Some(float_digest([1.0, 2.0]))];
	for bad in [1.5, -0.01, f64::INFINITY] {
		let err = call(vec![
			digest_column(ValueType::Float8, &digests),
			column([Value::float8(0.5), Value::float8(bad)], ValueType::Float8),
		])
		.unwrap_err();
		assert_eq!(reason(err), "p must be between 0 and 1", "p {bad}");
	}
	let err =
		call(vec![digest_column(ValueType::Float8, &digests[..1]), column([Value::Int1(2)], ValueType::Int1)])
			.unwrap_err();
	assert_eq!(reason(err), "p must be between 0 and 1");
}

#[test]
fn a_none_digest_or_a_none_p_gives_none_for_that_row_only() {
	// A none row must not hide the neighbours' answers or turn into an error from its placeholder p.
	let digest = duration_digest([10, 20, 30]);
	let digests = vec![Some(digest.clone()), None, Some(digest.clone()), Some(digest.clone())];
	let mut ps = column([Value::float8(0.5), Value::float8(0.5)], ValueType::Float8).into_builder();
	ps.push_none();
	ps.push_value(Value::float8(1.0));
	let ps = ps.finish();

	let out = call(vec![digest_column(ValueType::Duration, &digests), ps]).unwrap();

	assert_eq!(out.len(), 4);
	assert_eq!(out.get_value(0), digest.percentile_value(0.5).unwrap());
	assert!(!out.is_defined(1), "a none digest must give none");
	assert!(!out.is_defined(2), "a none p must give none");
	assert_eq!(out.get_value(3), digest.percentile_value(1.0).unwrap());
	assert_eq!(out.get_type(), ValueType::Option(Box::new(ValueType::Duration)));
}

#[test]
fn a_none_p_that_is_out_of_range_underneath_is_not_checked() {
	// The value under a none is a placeholder, so range checking it would reject valid input.
	let digest = float_digest([1.0, 2.0]);
	let ps = ColumnBuffer::Option {
		inner: Box::new(column([Value::float8(7.0)], ValueType::Float8)),
		bitvec: BooleanBuffer::from(vec![false]),
	};

	let out = call(vec![digest_column(ValueType::Float8, &[Some(digest)]), ps]).unwrap();

	assert!(!out.is_defined(0));
}

#[test]
fn an_untyped_none_p_gives_a_none_column_typed_by_the_digest() {
	// A literal none p carries no type, so the output type must still come from the digest.
	let digests = vec![Some(duration_digest([1, 2])), Some(duration_digest([3]))];

	let out = call(vec![digest_column(ValueType::Duration, &digests), ColumnBuffer::none_typed(ValueType::Any, 2)])
		.unwrap();

	assert_eq!(out.len(), 2);
	assert!(!out.is_defined(0) && !out.is_defined(1));
	assert_eq!(out.get_type(), ValueType::Option(Box::new(ValueType::Duration)));
}

#[test]
fn an_empty_digest_gives_a_none_typed_like_a_filled_one() {
	let digests = vec![Some(duration_digest([])), Some(duration_digest([40]))];

	let out = call(vec![
		digest_column(ValueType::Duration, &digests),
		column([Value::float8(0.5), Value::float8(0.5)], ValueType::Float8),
	])
	.unwrap();

	assert!(!out.is_defined(0), "an empty digest has no percentile");
	assert_eq!(out.get_value(1), digests[1].as_ref().unwrap().percentile_value(0.5).unwrap());
	assert_eq!(out.get_type(), ValueType::Option(Box::new(ValueType::Duration)));
}

#[test]
fn a_raw_input_is_an_error_with_or_without_accuracy() {
	// Outside window and aggregate no digest is built, so a raw column must not be read as one.
	let raw = column([Value::float8(1.0)], ValueType::Float8);
	let p = column([Value::float8(0.5)], ValueType::Float8);
	let accuracy = column([Value::float8(0.01)], ValueType::Float8);

	let err = call(vec![raw.clone(), p.clone()]).unwrap_err();
	assert!(reason(err).contains("needs an accuracy"));
	let err = call(vec![raw, p, accuracy]).unwrap_err();
	assert!(reason(err).contains("only supported inside window or aggregate"));
}

#[test]
fn an_accuracy_argument_next_to_a_digest_input_is_an_error() {
	// The digest type already fixes the accuracy, so a second one could only disagree with it.
	let digests = vec![Some(float_digest([1.0]))];
	let err = call(vec![
		digest_column(ValueType::Float8, &digests),
		column([Value::float8(0.5)], ValueType::Float8),
		column([Value::float8(0.01)], ValueType::Float8),
	])
	.unwrap_err();
	assert_eq!(reason(err), "accuracy comes from the digest type, remove the argument");
}

#[test]
fn a_non_numeric_p_and_a_wrong_arity_are_errors() {
	let digests = vec![Some(float_digest([1.0]))];
	let err = call(vec![
		digest_column(ValueType::Float8, &digests),
		column([Value::Utf8("half".to_string())], ValueType::Utf8),
	])
	.unwrap_err();
	assert!(
		matches!(
			err,
			RoutineError::FunctionInvalidArgumentType {
				argument_index: 1,
				..
			}
		),
		"got {err:?}"
	);

	let err =
		ApproxPercentile::new().arity().check(&Fragment::internal("stats::approx_percentile"), 1).unwrap_err();
	assert!(
		matches!(
			err,
			RoutineError::FunctionArityMismatch {
				expected: 2,
				actual: 1,
				..
			}
		),
		"got {err:?}"
	);
}

#[test]
fn return_type_is_duration_only_for_a_duration_digest() {
	let f = ApproxPercentile::new();
	assert_eq!(f.return_type(&[digest_type(ValueType::Duration), ValueType::Float8]), ValueType::Duration);
	assert_eq!(
		f.return_type(&[ValueType::Option(Box::new(digest_type(ValueType::Duration)))]),
		ValueType::Duration
	);
	assert_eq!(f.return_type(&[digest_type(ValueType::Int4), ValueType::Float8]), ValueType::Float8);
}

#[test]
fn the_function_is_registered_under_its_stats_name() {
	let routines = default_in_process_functions(Routines::builder()).configure();
	let function =
		routines.get_function("stats::approx_percentile").expect("stats::approx_percentile is not registered");
	assert_eq!(function.info().name, "stats::approx_percentile");
}
