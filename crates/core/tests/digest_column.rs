// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::FieldRef;
use arrow_select::filter::filter_record_batch;
use reifydb_core::value::{
	batch::{batch, group_by, heap_size, take_rows},
	column::{builder::ColumnBuilder, factory, view::group_by::GroupKeyDict},
};
use reifydb_value::value::{
	Value,
	column_view::{ColumnView, ViewData},
	digest::Digest,
	duration::Duration,
	frame::frame::Frame,
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

fn column_of(ty: ValueType, rows: &[Option<Digest>]) -> (FieldRef, ArrayRef) {
	let mut builder = ColumnBuilder::with_capacity(ty, rows.len());
	for row in rows {
		match row {
			Some(digest) => builder.push_value(digest_value(digest)),
			None => builder.push_none(),
		}
	}
	builder.finish("d")
}

fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
	ColumnView::try_from(column).unwrap()
}

fn first(batch: &RecordBatch) -> (FieldRef, ArrayRef) {
	(batch.schema_ref().fields()[0].clone(), batch.column(0).clone())
}

fn rows() -> Vec<Option<Digest>> {
	vec![Some(float_digest([1.0, 2.0, 3.0])), None, Some(float_digest([])), Some(float_digest([-4.0, 1e6]))]
}

fn values(column: &(FieldRef, ArrayRef)) -> Vec<Value> {
	let column = view(column);
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
	let column_view = view(&column);

	assert_eq!(values(&column), expected(&rows));
	assert_eq!(column_view.get_type(), ValueType::Option(Box::new(digest_type(ValueType::Float8, ACCURACY))));
	assert!(column_view.is_defined(0) && !column_view.is_defined(1) && column_view.is_defined(2));
}

#[test]
fn take_slice_gather_filter_and_reorder_keep_digests_equal() {
	let rows = rows();
	let column = column_of(digest_type(ValueType::Float8, ACCURACY), &rows);
	let rows_batch = batch(vec![column.clone()]).unwrap();
	let all = expected(&rows);

	assert_eq!(values(&first(&rows_batch.slice(0, 2))), all[..2].to_vec());
	assert_eq!(values(&first(&rows_batch.slice(1, 3))), all[1..4].to_vec());
	assert_eq!(
		values(&first(&take_rows(&rows_batch, &[3, 0, 1]).unwrap())),
		vec![all[3].clone(), all[0].clone(), all[1].clone()]
	);

	let filtered =
		first(&filter_record_batch(&rows_batch, &BooleanArray::from(vec![true, true, false, true])).unwrap());
	assert_eq!(values(&filtered), vec![all[0].clone(), all[1].clone(), all[3].clone()]);

	let reordered = first(&take_rows(&rows_batch, &[2, 3, 1, 0]).unwrap());
	assert_eq!(values(&reordered), vec![all[2].clone(), all[3].clone(), all[1].clone(), all[0].clone()]);
	assert_eq!(view(&reordered).get_type(), view(&column).get_type(), "transforms must keep the digest params");
}

#[test]
fn extend_appends_digests_from_a_column_with_the_same_params() {
	let first = vec![Some(float_digest([1.0]))];
	let second = rows();
	let column = column_of(digest_type(ValueType::Float8, ACCURACY), &first);
	let mut builder = ColumnBuilder::from_view(&view(&column));

	builder.extend(&view(&column_of(digest_type(ValueType::Float8, ACCURACY), &second))).unwrap();
	let column = builder.finish("d");

	let mut all = first.clone();
	all.extend(second);
	assert_eq!(values(&column), expected(&all));
}

#[test]
fn extend_refuses_a_digest_column_with_another_accuracy_or_inner_type() {
	// Mixing params in one column would make a later merge combine incompatible buckets.
	let column = column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(float_digest([1.0]))]);
	let mut builder = ColumnBuilder::from_view(&view(&column));
	let mut coarse = Digest::new(ValueType::Float8, 50_000).unwrap();
	coarse.add_value(&Value::float8(1.0)).unwrap();
	let other_accuracy = column_of(digest_type(ValueType::Float8, 50_000), &[Some(coarse)]);
	assert!(builder.extend(&view(&other_accuracy)).is_err());

	let other_inner = column_of(digest_type(ValueType::Duration, ACCURACY), &[Some(duration_digest([1]))]);
	assert!(builder.extend(&view(&other_inner)).is_err());
	assert_eq!(builder.len(), 1, "a refused extend must not leave rows behind");
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
	let column = factory::from_many("d", digest_value(&digest), 3);

	assert_eq!(view(&column).get_type(), digest_type(ValueType::Duration, ACCURACY));
	assert_eq!(values(&column), vec![digest_value(&digest); 3]);
}

#[test]
fn heap_size_grows_with_occupied_buckets_not_only_with_rows() {
	// Query memory limits count this, so a digest holding many buckets must cost more than one holding few.
	let few =
		batch(vec![column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(float_digest([1.0]))])]).unwrap();
	let many_digest = float_digest((1..=5000).map(|v| f64::from(v) * 1.5));
	assert!(many_digest.bucket_count() > 100);
	let many =
		batch(vec![column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(many_digest.clone())])]).unwrap();

	let few_size = heap_size(&few).unwrap();
	let many_size = heap_size(&many).unwrap();
	assert!(
		many_size >= few_size + (many_digest.bucket_count() - 1) * 12,
		"heap size {} of a {}-bucket digest is not above {} of a one-bucket digest by the bucket storage",
		many_size,
		many_digest.bucket_count(),
		few_size
	);
}

#[test]
fn frame_conversion_keeps_digests_nones_and_params_in_both_directions() {
	let rows = rows();
	let columns = batch(vec![column_of(digest_type(ValueType::Float8, ACCURACY), &rows)]).unwrap();

	let frame = Frame::from(columns.clone());
	let column = frame.try_column("d").unwrap();
	assert!(column.is_nullable(), "a digest column with a none must convert to an option frame column");
	assert!(matches!(
		column.data,
		ViewData::Digest {
			accuracy: ACCURACY,
			..
		}
	));

	let back = frame.batch.clone();
	assert_eq!(values(&first(&back)), expected(&rows));
	assert_eq!(view(&first(&back)).get_type(), view(&first(&columns)).get_type());
}

#[test]
fn a_digest_renders_as_its_count_in_display_and_as_string() {
	// Rendering the raw buckets would flood a result table, so only the count is shown.
	let digest = float_digest([1.0, 2.0, 3.0]);
	assert_eq!(digest_value(&digest).to_string(), "digest(n: 3)");

	let column = column_of(digest_type(ValueType::Float8, ACCURACY), &[Some(digest), None]);
	assert_eq!(view(&column).as_string(0), "digest(n: 3)");

	let rendered = Frame::from(batch(vec![column]).unwrap()).to_string();
	assert!(rendered.contains("digest(n: 3)"), "rendered frame lacks the digest count:\n{rendered}");
}

#[test]
fn grouping_by_a_digest_column_is_an_error_not_a_panic() {
	// A digest has no key encoding, so grouping must stop with an error the query can report.
	let columns = batch(vec![column_of(
		digest_type(ValueType::Float8, ACCURACY),
		&[Some(float_digest([1.0])), Some(float_digest([2.0]))],
	)])
	.unwrap();
	let err = group_by(&columns, &["d"], &mut GroupKeyDict::new()).unwrap_err();
	assert_eq!(err.diagnostic().code, "SERDE_003");

	// The none row takes the typed-none key path, which must fail the same way.
	let with_none =
		batch(vec![column_of(digest_type(ValueType::Float8, ACCURACY), &[None, Some(float_digest([2.0]))])])
			.unwrap();
	let err = group_by(&with_none, &["d"], &mut GroupKeyDict::new()).unwrap_err();
	assert_eq!(err.diagnostic().code, "SERDE_003");
}
