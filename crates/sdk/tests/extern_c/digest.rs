// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_sdk::{
	common::extern_wasm::marshal::marshal_columns_to_bytes, flow::operator::extern_c::binding::arena::Arena,
};
use reifydb_testing_sdk::harness::ExternCOperatorHarnessBuilder;
use reifydb_value::{
	error::Diagnostic,
	fragment::Fragment,
	value::{Value, datetime::DateTime, digest::Digest, value_type::ValueType},
};

use super::common::PassthroughOperator;

fn digest_value() -> Value {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in [1.0, 2.0, 3.0] {
		digest.add_value(&Value::float8(value)).unwrap();
	}
	Value::Digest(Box::new(digest))
}

fn digest_type() -> ValueType {
	digest_value().get_type()
}

fn digest_buffer() -> ColumnBuffer {
	let (buffer, _) = ColumnBuffer::none_typed(digest_type(), 0).into_unwrap_option();
	let mut builder = buffer.into_builder();
	builder.push_value(digest_value());
	builder.finish()
}

fn columns(name: &str, buffer: ColumnBuffer) -> Columns {
	Columns::new(vec![ColumnWithName::new(Fragment::internal(name), buffer)])
}

fn scalar_columns() -> Columns {
	columns("a", ColumnBuffer::int4(vec![1]))
}

fn change(diffs: Vec<Diff>) -> Change {
	let mut all = Diffs::new();
	for diff in diffs {
		all.push(diff);
	}
	Change::from_flow(OperatorId(1), ChangeVersion::from(CommitVersion(1)), all, DateTime::default())
}

fn marshal_change_error(change: &Change) -> Diagnostic {
	let Err(err) = Arena::new().marshal_change(change) else {
		panic!("marshalling a change that carries a digest column must fail");
	};
	err.diagnostic()
}

fn assert_extern_001(diagnostic: &Diagnostic, column: &str) {
	assert_eq!(diagnostic.code, "EXTERN_001", "got: {diagnostic:?}");
	assert!(
		diagnostic.message.contains(&format!("'{column}'")),
		"the message must name the column, got: {}",
		diagnostic.message
	);
	assert!(
		diagnostic.message.contains(&digest_type().to_string()),
		"the message must name the digest type, got: {}",
		diagnostic.message
	);
}

#[test]
fn marshal_change_with_a_digest_insert_reports_extern_001() {
	// The wire descriptor has no slot for inner type or accuracy, so a digest can never reach a guest intact.
	let diagnostic = marshal_change_error(&change(vec![Diff::insert(columns("d", digest_buffer()))]));

	assert_extern_001(&diagnostic, "d");
}

#[test]
fn marshal_change_with_an_optional_digest_column_reports_extern_001() {
	// An Option wrapper must not hide the digest from the check, since the marshaller unwraps it before encoding.
	let mut builder = ColumnBuffer::none_typed(digest_type(), 1).into_builder();
	builder.push_value(digest_value());
	let buffer = builder.finish();

	let diagnostic = marshal_change_error(&change(vec![Diff::insert(columns("o", buffer))]));

	assert_extern_001(&diagnostic, "o");
}

#[test]
fn marshal_change_with_a_digest_only_in_the_pre_of_an_update_reports_extern_001() {
	// Checking only post would let the pre side of an update reach the panicking encoder.
	let diagnostic =
		marshal_change_error(&change(vec![Diff::update(columns("d", digest_buffer()), scalar_columns())]));

	assert_extern_001(&diagnostic, "d");
}

#[test]
fn marshal_change_with_a_digest_in_a_later_remove_diff_reports_extern_001() {
	// Every diff must be checked, otherwise a scalar first diff would let a later digest reach the encoder.
	let diagnostic = marshal_change_error(&change(vec![
		Diff::insert(scalar_columns()),
		Diff::remove(columns("d", digest_buffer())),
	]));

	assert_extern_001(&diagnostic, "d");
}

#[test]
fn marshal_columns_to_bytes_with_a_digest_column_reports_extern_001() {
	// The wasm byte format also lacks the inner type and accuracy, so it must refuse instead of panicking.
	let input = Columns::new(vec![
		ColumnWithName::new(Fragment::internal("a"), ColumnBuffer::int4(vec![1])),
		ColumnWithName::new(Fragment::internal("d"), digest_buffer()),
	]);

	let diagnostic = marshal_columns_to_bytes(&input).unwrap_err().diagnostic();

	assert_extern_001(&diagnostic, "d");
}

#[test]
fn harness_apply_with_a_digest_column_fails_naming_the_digest_type() {
	// The harness drives the same marshal path as the host, so the error must surface through apply, not abort.
	let mut harness = ExternCOperatorHarnessBuilder::<PassthroughOperator>::new()
		.with_node_id(OperatorId(1))
		.build()
		.expect("build harness");

	let Err(err) = harness.apply(change(vec![Diff::insert(columns("d", digest_buffer()))])) else {
		panic!("applying a change that carries a digest column must fail");
	};

	let message = err.to_string();
	assert!(message.contains("EXTERN_001"), "the message must carry the code, got: {message}");
	assert!(message.contains(&digest_type().to_string()), "the message must name the digest type, got: {message}");
}
