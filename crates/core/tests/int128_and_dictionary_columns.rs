// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use arrow_select::filter::filter_record_batch;
use reifydb_core::value::{
	batch::{batch, head, take_rows},
	column::{builder::ColumnBuilder, factory, nulls::with_nulls, scatter::scatter_merge},
};
use reifydb_value::value::{
	Value,
	column_view::{ColumnView, ViewData},
	container::{
		dictionary_array::{self, dictionary_array},
		wide_int_array::wides,
	},
	dictionary::{DictionaryEntryId, DictionaryId},
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};

const ENTRIES: [DictionaryEntryId; 4] = [
	DictionaryEntryId::U1(3),
	DictionaryEntryId::U2(u16::MAX),
	DictionaryEntryId::U8(u64::MAX),
	DictionaryEntryId::U16(u128::MAX),
];

const INTS: [i128; 3] = [i128::MIN, 0, i128::MAX];

const UINTS: [u128; 4] = [0, 1 << 64, 1 << 127, u128::MAX];

fn tagged(entries: &[DictionaryEntryId], id: u64) -> (FieldRef, ArrayRef) {
	named(
		"c",
		FieldType {
			value_type: Some(ValueType::DictionaryId),
			dictionary_id: Some(DictionaryId(id)),
			..FieldType::default()
		},
		Arc::new(dictionary_array(entries.iter().copied())),
	)
}

fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
	ColumnView::try_from(column).unwrap()
}

fn one_column(column: &(FieldRef, ArrayRef)) -> RecordBatch {
	batch(vec![column.clone()]).unwrap()
}

fn only_column(rows: &RecordBatch) -> (FieldRef, ArrayRef) {
	(rows.schema_ref().fields()[0].clone(), rows.column(0).clone())
}

fn filter_column(column: &(FieldRef, ArrayRef), mask: Vec<bool>) -> (FieldRef, ArrayRef) {
	only_column(&filter_record_batch(&one_column(column), &BooleanArray::from(mask)).unwrap())
}

fn take_column(column: &(FieldRef, ArrayRef), indices: &[usize]) -> (FieldRef, ArrayRef) {
	only_column(&take_rows(&one_column(column), indices).unwrap())
}

fn slice_column(column: &(FieldRef, ArrayRef), start: usize, end: usize) -> (FieldRef, ArrayRef) {
	(column.0.clone(), column.1.slice(start, end - start))
}

fn dictionary_parts(column: &(FieldRef, ArrayRef)) -> (Vec<DictionaryEntryId>, Option<DictionaryId>) {
	let column = view(column);
	match &column.data {
		ViewData::DictionaryId {
			container,
			dictionary_id,
		} => (dictionary_array::iter(container).collect(), *dictionary_id),
		_ => panic!("expected a dictionary id column, got {:?}", column.get_type()),
	}
}

fn head_column(column: &(FieldRef, ArrayRef), n: usize) -> (FieldRef, ArrayRef) {
	only_column(&head(&one_column(column), n))
}

fn assert_int16(column: &(FieldRef, ArrayRef), expected: &[i128]) {
	let column = view(column);
	match &column.data {
		ViewData::Int16(array) => assert_eq!(wides::<i128>(array), expected),
		_ => panic!("expected an int16 column, got {:?}", column.get_type()),
	}
}

fn assert_uint16(column: &(FieldRef, ArrayRef), expected: &[u128]) {
	let column = view(column);
	match &column.data {
		ViewData::Uint16(array) => assert_eq!(wides::<u128>(array), expected),
		_ => panic!("expected a uint16 column, got {:?}", column.get_type()),
	}
}

#[test]
fn dictionary_id_survives_every_buffer_op() {
	// A column that loses its dictionary id can no longer be decoded, so every op must carry it.
	let id = Some(DictionaryId(7));
	let buffer = tagged(&ENTRIES, 7);
	assert_eq!(dictionary_parts(&buffer.clone()), (ENTRIES.to_vec(), id));

	let filtered = filter_column(&buffer, vec![true, false, true, true]);
	assert_eq!(dictionary_parts(&filtered), (vec![ENTRIES[0], ENTRIES[2], ENTRIES[3]], id));

	let reordered = take_column(&buffer, &[3, 1]);
	assert_eq!(dictionary_parts(&reordered), (vec![ENTRIES[3], ENTRIES[1]], id));
	let error = take_rows(&one_column(&buffer), &[3, 1, 9]).unwrap_err();
	assert_eq!(error.diagnostic().message, "row index 9 out of range for a column of 4 rows");

	assert_eq!(dictionary_parts(&slice_column(&buffer, 1, 3)), (ENTRIES[1..3].to_vec(), id));
	assert_eq!(dictionary_parts(&head_column(&buffer, 2)), (ENTRIES[..2].to_vec(), id));
	assert_eq!(dictionary_parts(&take_column(&buffer, &[2, 2, 0])), (vec![ENTRIES[2], ENTRIES[2], ENTRIES[0]], id));
}

#[test]
fn extend_keeps_the_receiving_columns_dictionary_id() {
	// The receiving column owns the dictionary; taking the appended column's id would remap every row.
	let mut extended = ColumnBuilder::from_view(&view(&tagged(&ENTRIES, 7)));
	extended.extend(&view(&tagged(&ENTRIES[1..2], 99))).unwrap();
	let extended = extended.finish("c");
	assert_eq!(dictionary_parts(&extended), ([ENTRIES.as_slice(), &ENTRIES[1..2]].concat(), Some(DictionaryId(7))));
}

#[test]
fn dictionary_id_survives_the_builder_round_trip() {
	// A builder that drops the id turns every appended batch into an undecodable column.
	let id = Some(DictionaryId(7));
	let mut builder = ColumnBuilder::from_view(&view(&tagged(&ENTRIES, 7)));
	builder.push_value(Value::DictionaryId(DictionaryEntryId::U4(5)));
	builder.push(DictionaryEntryId::U2(6));
	builder.extend(&view(&tagged(&ENTRIES[..1], 99))).unwrap();
	let finished = builder.finish("c");
	let pushed = [DictionaryEntryId::U4(5), DictionaryEntryId::U2(6)];
	assert_eq!(dictionary_parts(&finished), ([ENTRIES.as_slice(), &pushed, &ENTRIES[..1]].concat(), id));
	assert_eq!(dictionary_parts(&ColumnBuilder::like(&view(&finished), 4).finish("c")), (vec![], id));

	let mut fresh = ColumnBuilder::with_capacity(ValueType::DictionaryId, 2);
	fresh.set_dictionary_id(DictionaryId(3));
	fresh.push(ENTRIES[3]);
	assert_eq!(dictionary_parts(&fresh.finish("c")), (vec![ENTRIES[3]], Some(DictionaryId(3))));
}

#[test]
fn option_wrapped_dictionary_column_keeps_its_id() {
	// A nullable dictionary column must keep its id through every op, never drop it with the nones.
	let id = Some(DictionaryId(7));
	let nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false, true, true]));
	let buffer = with_nulls(tagged(&ENTRIES, 7), nulls).unwrap();

	let filtered = filter_column(&buffer, vec![false, true, true, true]);
	assert_eq!(dictionary_parts(&filtered), (ENTRIES[1..].to_vec(), id));

	let reordered = take_column(&buffer, &[3, 0]);
	assert_eq!(dictionary_parts(&reordered), (vec![ENTRIES[3], ENTRIES[0]], id));

	assert_eq!(dictionary_parts(&slice_column(&buffer, 0, 2)), (ENTRIES[..2].to_vec(), id));
	assert_eq!(dictionary_parts(&slice_column(&buffer, 0, 3)), (ENTRIES[..3].to_vec(), id));

	let mut builder = ColumnBuilder::from_view(&view(&buffer));
	builder.push_none();
	builder.push_value(Value::DictionaryId(ENTRIES[0]));
	let finished = builder.finish("c");
	assert_eq!(
		dictionary_parts(&finished),
		([ENTRIES.as_slice(), &[DictionaryEntryId::default(), ENTRIES[0]]].concat(), id)
	);
	assert_eq!(view(&finished).get_value(4), Value::none_of(ValueType::DictionaryId));
	assert_eq!(view(&finished).get_value(5), Value::DictionaryId(ENTRIES[0]));
}

#[test]
fn shared_or_offset_dictionary_buffer_copies_whole_rows_into_the_builder() {
	// Copying 16 byte rows out of a shared buffer shifts every entry and trips the whole row check.
	let id = Some(DictionaryId(7));
	let buffer = tagged(&ENTRIES, 7);

	let shared_column = buffer.clone();
	let mut shared = ColumnBuilder::from_view(&view(&shared_column));
	shared.push(DictionaryEntryId::U1(1));
	assert_eq!(
		dictionary_parts(&shared.finish("c")),
		([ENTRIES.as_slice(), &[DictionaryEntryId::U1(1)]].concat(), id)
	);
	assert_eq!(dictionary_parts(&buffer), (ENTRIES.to_vec(), id));

	let offset = slice_column(&buffer, 1, 3);
	let prefix = slice_column(&buffer, 0, 2);
	drop(buffer);
	drop(shared_column);

	let mut offset = ColumnBuilder::from_view(&view(&offset));
	offset.push(ENTRIES[0]);
	assert_eq!(dictionary_parts(&offset.finish("c")), (vec![ENTRIES[1], ENTRIES[2], ENTRIES[0]], id));

	let mut prefix = ColumnBuilder::from_view(&view(&prefix));
	prefix.push(ENTRIES[3]);
	assert_eq!(dictionary_parts(&prefix.finish("c")), (vec![ENTRIES[0], ENTRIES[1], ENTRIES[3]], id));
}

#[test]
fn int16_keeps_its_values_and_data_type_through_every_op() {
	// Every op must keep the ordered rows exact; a zero filler row would read back as i128::MIN.
	let buffer = factory::int16("c", INTS);
	assert_int16(&buffer, &INTS);

	assert_int16(&filter_column(&buffer, vec![true, false, true]), &[i128::MIN, i128::MAX]);

	assert_int16(&take_column(&buffer, &[2, 0]), &[i128::MAX, i128::MIN]);
	let error = take_rows(&one_column(&buffer), &[2, 0, 7]).unwrap_err();
	assert_eq!(error.diagnostic().message, "row index 7 out of range for a column of 3 rows");

	assert_int16(&slice_column(&buffer, 1, 3), &INTS[1..]);
	assert_int16(&head_column(&buffer, 1), &INTS[..1]);
	assert_int16(&take_column(&buffer, &[2, 2]), &[i128::MAX, i128::MAX]);

	let mut extended = ColumnBuilder::from_view(&view(&buffer));
	extended.extend(&view(&factory::int16("c", [1]))).unwrap();
	assert_int16(&extended.finish("c"), &[i128::MIN, 0, i128::MAX, 1]);

	let merged = scatter_merge(
		&view(&buffer),
		&view(&factory::int16("c", [1, 2, 3])),
		&BooleanBuffer::from(vec![true, false, true]),
		&BooleanBuffer::from(vec![false, true, false]),
		3,
		"c",
	)
	.unwrap();
	assert_int16(&merged, &[i128::MIN, 2, i128::MAX]);

	let mut builder = ColumnBuilder::from_view(&view(&buffer));
	builder.push_value(Value::Int16(5));
	builder.push(7i8);
	builder.push(9u64);
	assert_int16(&builder.finish("c"), &[i128::MIN, 0, i128::MAX, 5, 7, 9]);

	let mut fresh = ColumnBuilder::with_capacity(ValueType::Int16, 1);
	fresh.push(i128::MIN);
	assert_int16(&fresh.finish("c"), &[i128::MIN]);

	assert_int16(&factory::none_typed("c", ValueType::Int16, 2), &[0, 0]);
	assert_int16(&factory::int16_with_bitvec("c", [4, 0], vec![true, false]), &[4, 0]);
	assert_eq!(view(&buffer).get_as::<i128>(2), Ok(Some(i128::MAX)));
	assert_eq!(view(&buffer).get_value(0), Value::Int16(i128::MIN));
}

#[test]
fn uint16_keeps_values_above_i128_max_and_its_data_type() {
	// Values at and above 2^127 must never pick up a sign on any op.
	let buffer = factory::uint16("c", UINTS);
	assert_uint16(&buffer, &UINTS);

	assert_uint16(&filter_column(&buffer, vec![false, true, false, true]), &[1 << 64, u128::MAX]);

	assert_uint16(&take_column(&buffer, &[3, 2]), &[u128::MAX, 1 << 127]);
	let error = take_rows(&one_column(&buffer), &[3, 2, 8]).unwrap_err();
	assert_eq!(error.diagnostic().message, "row index 8 out of range for a column of 4 rows");

	assert_uint16(&slice_column(&buffer, 2, 4), &UINTS[2..]);
	assert_uint16(&head_column(&buffer, 2), &UINTS[..2]);
	assert_uint16(&take_column(&buffer, &[3, 0]), &[u128::MAX, 0]);

	let mut extended = ColumnBuilder::from_view(&view(&buffer));
	extended.extend(&view(&factory::uint16("c", [u128::MAX - 1]))).unwrap();
	assert_uint16(&extended.finish("c"), &[0, 1 << 64, 1 << 127, u128::MAX, u128::MAX - 1]);

	let merged = scatter_merge(
		&view(&buffer),
		&view(&factory::uint16("c", [5, 6, 7, 8])),
		&BooleanBuffer::from(vec![false, true, false, true]),
		&BooleanBuffer::from(vec![true, false, true, false]),
		4,
		"c",
	)
	.unwrap();
	assert_uint16(&merged, &[5, 1 << 64, 7, u128::MAX]);

	let mut builder = ColumnBuilder::from_view(&view(&buffer));
	builder.push_value(Value::Uint16(u128::MAX));
	builder.push(3u8);
	builder.push(-1i8);
	assert_uint16(&builder.finish("c"), &[0, 1 << 64, 1 << 127, u128::MAX, u128::MAX, 3, 0]);

	let mut fresh = ColumnBuilder::with_capacity(ValueType::Uint16, 1);
	fresh.push(u128::MAX);
	assert_uint16(&fresh.finish("c"), &[u128::MAX]);

	assert_uint16(&factory::none_typed("c", ValueType::Uint16, 2), &[0, 0]);
	assert_uint16(&factory::uint16_with_bitvec("c", [u128::MAX, 0], vec![true, false]), &[u128::MAX, 0]);
	assert_eq!(view(&buffer).get_value(3), Value::Uint16(u128::MAX));
	assert_eq!(view(&buffer).get_as::<u128>(3), Ok(Some(u128::MAX)));
	assert_eq!(view(&buffer).get_as::<i128>(3).unwrap_err().code, "CONV_004");
	assert_eq!(view(&buffer).get_as::<u64>(1).unwrap_err().code, "CONV_004");
	assert_eq!(view(&buffer).as_string(3), u128::MAX.to_string());
}
