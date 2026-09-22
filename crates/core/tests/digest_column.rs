// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{
	ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns, view::group_by::GroupKeyDict,
};
use reifydb_value::value::{
	Value,
	digest::Digest,
	duration::Duration,
	frame::{data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};

const ACCURACY: u32 = 10_000;

fn digest_type(inner: ValueType, accuracy: u32) -> ValueType {
	ValueType::Digest {
		inner: Box::new(inner),
		accuracy,
	}
}

fn float_digest(values: impl IntoIterator<Item = f64>) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, ACCURACY).unwrap();
	for value in values {
		digest.add_value(&Value::float8(value)).unwrap();
	}
	digest
}

fn duration_digest(millis: impl IntoIterator<Item = i64>) -> Digest {
	let mut digest = Digest::new(ValueType::Duration, ACCURACY).unwrap();
	for ms in millis {
		digest.add_value(&Value::Duration(Duration::from_milliseconds(ms).unwrap())).unwrap();
	}
	digest
}

fn digest_value(digest: &Digest) -> Value {
	Value::Digest(Box::new(digest.clone()))
}

fn column_of(ty: ValueType, rows: &[Option<Digest>]) -> ColumnBuffer {
	let mut builder = ColumnBuilder::with_capacity(ty, rows.len());
	for row in rows {
		match row {
			Some(digest) => builder.push_value(digest_value(digest)),
			None => builder.push_none(),
		}
	}
	builder.finish()
}

fn rows() -> Vec<Option<Digest>> {
	vec![Some(float_digest([1.0, 2.0, 3.0])), None, Some(float_digest([])), Some(float_digest([-4.0, 1e6]))]
}

fn values(column: &ColumnBuffer) -> Vec<Value> {
	(0..column.len()).map(|row| column.get_value(row)).collect()
}

fn expected(rows: &[Option<Digest>]) -> Vec<Value> {
	rows.iter()
		.map(|row| match row {
			Some(digest) => digest_value(digest),
			None => Value::none_of(digest_type(ValueType::Float8, ACCURACY)),
		})
		.collect()
}

#[test]
fn push_keeps_every_digest_and_none_with_the_column_type() {
	// A digest column that loses its inner type or accuracy can no longer be merged or encoded.
	let rows = rows();
	let column = column_of(digest_type(ValueType::Float8, ACCURACY), &rows);

	assert_eq!(values(&column), expected(&rows));
	assert_eq!(column.get_type(), ValueType::Option(Box::new(digest_type(ValueType::Float8, ACCURACY))));
	assert!(column.is_defined(0) && !column.is_defined(1) && column.is_defined(2));
}

#[test]
fn take_slice_gather_filter_and_reorder_keep_digests_equal() {
	let rows = rows();
	let column = column_of(digest_type(ValueType::Float8, ACCURACY), &rows);
	let all = expected(&rows);

	assert_eq!(values(&column.take(2)), all[..2].to_vec());
	assert_eq!(values(&column.slice(1, 4)), all[1..4].to_vec());
	assert_eq!(values(&column.gather(&[3, 0, 1])), vec![all[3].clone(), all[0].clone(), all[1].clone()]);

	let mut filtered = column.clone();
	filtered.filter(&BooleanBuffer::from(vec![true, true, false, true])).unwrap();
	assert_eq!(values(&filtered), vec![all[0].clone(), all[1].clone(), all[3].clone()]);

	let mut reordered = column.clone();
	reordered.reorder(&[2, 3, 1, 0]);
	assert_eq!(values(&reordered), vec![all[2].clone(), all[3].clone(), all[1].clone(), all[0].clone()]);
	assert_eq!(reordered.get_type(), column.get_type(), "transforms must keep the digest params");
}

#[test]
fn extend_appends_digests_from_a_column_with_the_same_params() {
	let first = vec![Some(float_digest([1.0]))];
	let second = rows();
	let mut column = column_of(digest_type(ValueType::Float8, ACCURACY), &first);

	column.extend(column_of(digest_type(ValueType::Float8, ACCURACY), &second)).unwrap();

	let mut all = first.clone();
	all.extend(second);
	assert_eq!(values(&column), expected(&all));
}

#[test]
fn extend_refuses_a_digest_column_with_another_accuracy_or_inner_type() {
	// Mixing params in one column would make a later merge combine incompatible buckets.
	let mut column = column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(float_digest([1.0]))]);
	let mut coarse = Digest::new(ValueType::Float8, 50_000).unwrap();
	coarse.add_value(&Value::float8(1.0)).unwrap();
	let other_accuracy = column_of(digest_type(ValueType::Float8, 50_000), &[Some(coarse)]);
	assert!(column.extend(other_accuracy).is_err());

	let other_inner = column_of(digest_type(ValueType::Duration, ACCURACY), &[Some(duration_digest([1]))]);
	assert!(column.extend(other_inner).is_err());
	assert_eq!(column.len(), 1, "a refused extend must not leave rows behind");
}

#[test]
#[should_panic(expected = "cannot push a Digest(Duration, 10000) into a Digest(Float8, 10000) column")]
fn a_digest_pushed_into_a_column_with_other_params_panics_with_both_types() {
	// Pushing silently would store a digest the column type misdescribes.
	let mut builder = ColumnBuilder::with_capacity(digest_type(ValueType::Float8, ACCURACY), 1);
	builder.push_value(digest_value(&duration_digest([1])));
}

#[test]
fn from_many_repeats_one_digest_for_every_row() {
	let digest = duration_digest([5, 6, 7]);
	let column = ColumnBuffer::from_many(digest_value(&digest), 3);

	assert_eq!(column.get_type(), digest_type(ValueType::Duration, ACCURACY));
	assert_eq!(values(&column), vec![digest_value(&digest); 3]);
}

#[test]
fn heap_size_grows_with_occupied_buckets_not_only_with_rows() {
	// Query memory limits count this, so a digest holding many buckets must cost more than one holding few.
	let few = Columns::new(vec![ColumnWithName::new(
		"d",
		column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(float_digest([1.0]))]),
	)]);
	let many_digest = float_digest((1..=5000).map(|v| f64::from(v) * 1.5));
	assert!(many_digest.bucket_count() > 100);
	let many = Columns::new(vec![ColumnWithName::new(
		"d",
		column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(many_digest.clone())]),
	)]);

	assert!(
		many.heap_size() >= few.heap_size() + (many_digest.bucket_count() - 1) * 12,
		"heap size {} of a {}-bucket digest is not above {} of a one-bucket digest by the bucket storage",
		many.heap_size(),
		many_digest.bucket_count(),
		few.heap_size()
	);
}

#[test]
fn frame_conversion_keeps_digests_nones_and_params_in_both_directions() {
	let rows = rows();
	let columns = Columns::new(vec![ColumnWithName::new(
		"d",
		column_of(digest_type(ValueType::Float8, ACCURACY), &rows),
	)]);

	let frame = Frame::from(columns.clone());
	let FrameColumnData::Option {
		inner,
		..
	} = &frame.columns[0].data
	else {
		panic!("a digest column with a none must convert to an option frame column");
	};
	assert!(matches!(
		inner.as_ref(),
		FrameColumnData::Digest {
			accuracy: ACCURACY,
			..
		}
	));

	let back = Columns::from(frame);
	assert_eq!(values(&back[0]), expected(&rows));
	assert_eq!(back[0].get_type(), columns[0].get_type());
}

#[test]
fn a_digest_renders_as_its_count_in_display_and_as_string() {
	// Rendering the raw buckets would flood a result table, so only the count is shown.
	let digest = float_digest([1.0, 2.0, 3.0]);
	assert_eq!(digest_value(&digest).to_string(), "digest(n: 3)");

	let column = column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(digest), None]);
	assert_eq!(column.as_string(0), "digest(n: 3)");

	let rendered = Frame::from(Columns::new(vec![ColumnWithName::new("d", column)])).to_string();
	assert!(rendered.contains("digest(n: 3)"), "rendered frame lacks the digest count:\n{rendered}");
}

#[test]
fn grouping_by_a_digest_column_is_an_error_not_a_panic() {
	// A digest has no key encoding, so grouping must stop with an error the query can report.
	let columns = Columns::new(vec![ColumnWithName::new(
		"d",
		column_of(
			digest_type(ValueType::Float8, ACCURACY),
			&[Some(float_digest([1.0])), Some(float_digest([2.0]))],
		),
	)]);
	let err = columns.group_by_ids(&["d"], &mut GroupKeyDict::new()).unwrap_err();
	assert_eq!(err.diagnostic().code, "SERDE_003");

	// The none row takes the typed-none key path, which must fail the same way.
	let with_none = Columns::new(vec![ColumnWithName::new(
		"d",
		column_of(digest_type(ValueType::Float8, ACCURACY), &[None, Some(float_digest([2.0]))]),
	)]);
	let err = with_none.group_by_ids(&["d"], &mut GroupKeyDict::new()).unwrap_err();
	assert_eq!(err.diagnostic().code, "SERDE_003");
}
