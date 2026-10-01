// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
	},
	value::{
		batch::batch,
		column::{
			builder::ColumnBuilder,
			factory::{self, rename},
			nulls::split_nulls,
		},
	},
};
use reifydb_sdk::{
	common::extern_wasm::marshal::marshal_columns_to_bytes, flow::operator::extern_c::binding::arena::Arena,
};
use reifydb_value::{
	error::Diagnostic,
	value::{Value, column_view::ColumnView, datetime::DateTime, digest::Digest, value_type::ValueType},
};

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

fn digest_buffer() -> (FieldRef, ArrayRef) {
	let (buffer, _) = split_nulls(factory::none_typed("c", digest_type(), 0)).unwrap();
	let mut builder = ColumnBuilder::from_view(&ColumnView::try_from(&buffer).unwrap());
	builder.push_value(digest_value());
	builder.finish("c")
}

fn columns(name: &str, buffer: (FieldRef, ArrayRef)) -> RecordBatch {
	batch(vec![rename(buffer, name)]).unwrap()
}

fn scalar_columns() -> RecordBatch {
	columns("a", factory::int4("c", vec![1]))
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
	let none = factory::none_typed("c", digest_type(), 1);
	let mut builder = ColumnBuilder::from_view(&ColumnView::try_from(&none).unwrap());
	builder.push_value(digest_value());
	let buffer = builder.finish("c");

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
	let input = vec![factory::int4("a", vec![1]), rename(digest_buffer(), "d")];

	let diagnostic = marshal_columns_to_bytes(&input, 1, &[]).unwrap_err().diagnostic();

	assert_extern_001(&diagnostic, "d");
}
