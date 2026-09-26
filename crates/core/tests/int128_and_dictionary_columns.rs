// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_core::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};
use reifydb_value::value::{
	Value,
	container::{
		dictionary_array::{self, dictionary_array},
		wide_int_array::wides,
	},
	dictionary::{DictionaryEntryId, DictionaryId},
	frame::data::FrameColumnData,
	value_type::ValueType,
};

const ENTRIES: [DictionaryEntryId; 4] = [
	DictionaryEntryId::U1(3),
	DictionaryEntryId::U2(u16::MAX),
	DictionaryEntryId::U8(u64::MAX),
	DictionaryEntryId::U16(u128::MAX),
];

const INTS: [i128; 3] = [i128::MIN, 0, i128::MAX];

const UINTS: [u128; 4] = [0, 1 << 64, 1 << 127, u128::MAX];

fn tagged(entries: &[DictionaryEntryId], id: u64) -> ColumnBuffer {
	ColumnBuffer::DictionaryId {
		container: dictionary_array(entries.iter().copied()),
		dictionary_id: Some(DictionaryId(id)),
	}
}

fn dictionary_parts(buffer: &ColumnBuffer) -> (Vec<DictionaryEntryId>, Option<DictionaryId>) {
	match buffer {
		ColumnBuffer::DictionaryId {
			container,
			dictionary_id,
		} => (dictionary_array::iter(container).collect(), *dictionary_id),
		other => panic!("expected a dictionary id column, got {:?}", other.get_type()),
	}
}

fn assert_int16(buffer: &ColumnBuffer, expected: &[i128]) {
	match buffer {
		ColumnBuffer::Int16(array) => assert_eq!(wides::<i128>(array), expected),
		other => panic!("expected an int16 column, got {:?}", other.get_type()),
	}
}

fn assert_uint16(buffer: &ColumnBuffer, expected: &[u128]) {
	match buffer {
		ColumnBuffer::Uint16(array) => assert_eq!(wides::<u128>(array), expected),
		other => panic!("expected a uint16 column, got {:?}", other.get_type()),
	}
}

fn round_trips(buffer: &ColumnBuffer) -> Vec<ColumnBuffer> {
	let frame = FrameColumnData::from(buffer.clone());
	let frame_bytes = postcard::to_stdvec(&frame).unwrap();
	let frame_json = serde_json::to_string(&frame).unwrap();
	let column_bytes = postcard::to_stdvec(buffer).unwrap();
	let column_json = serde_json::to_string(buffer).unwrap();
	vec![
		ColumnBuffer::from(frame),
		ColumnBuffer::from(postcard::from_bytes::<FrameColumnData>(&frame_bytes).unwrap()),
		ColumnBuffer::from(serde_json::from_str::<FrameColumnData>(&frame_json).unwrap()),
		postcard::from_bytes::<ColumnBuffer>(&column_bytes).unwrap(),
		serde_json::from_str::<ColumnBuffer>(&column_json).unwrap(),
	]
}

#[test]
fn dictionary_id_survives_every_buffer_op() {
	// A column that loses its dictionary id can no longer be decoded, so every op must carry it.
	let id = Some(DictionaryId(7));
	let buffer = tagged(&ENTRIES, 7);
	assert_eq!(dictionary_parts(&buffer.clone()), (ENTRIES.to_vec(), id));

	let mut filtered = buffer.clone();
	filtered.filter(&BooleanBuffer::from(vec![true, false, true, true])).unwrap();
	assert_eq!(dictionary_parts(&filtered), (vec![ENTRIES[0], ENTRIES[2], ENTRIES[3]], id));

	let mut reordered = buffer.clone();
	reordered.reorder(&[3, 1, 9]);
	assert_eq!(dictionary_parts(&reordered), (vec![ENTRIES[3], ENTRIES[1], DictionaryEntryId::default()], id));

	assert_eq!(dictionary_parts(&buffer.slice(1, 3)), (ENTRIES[1..3].to_vec(), id));
	assert_eq!(dictionary_parts(&buffer.take(2)), (ENTRIES[..2].to_vec(), id));
	assert_eq!(dictionary_parts(&buffer.gather(&[2, 2, 0])), (vec![ENTRIES[2], ENTRIES[2], ENTRIES[0]], id));

	for decoded in round_trips(&buffer) {
		assert_eq!(dictionary_parts(&decoded), (ENTRIES.to_vec(), id));
	}
}

#[test]
fn extend_keeps_the_receiving_columns_dictionary_id() {
	// The receiving column owns the dictionary; taking the appended column's id would remap every row.
	let mut extended = tagged(&ENTRIES, 7);
	extended.extend(tagged(&ENTRIES[1..2], 99)).unwrap();
	assert_eq!(dictionary_parts(&extended), ([ENTRIES.as_slice(), &ENTRIES[1..2]].concat(), Some(DictionaryId(7))));
}

#[test]
fn dictionary_id_survives_the_builder_round_trip() {
	// A builder that drops the id turns every appended batch into an undecodable column.
	let id = Some(DictionaryId(7));
	let mut builder = tagged(&ENTRIES, 7).into_builder();
	builder.push_value(Value::DictionaryId(DictionaryEntryId::U4(5)));
	builder.push(DictionaryEntryId::U2(6));
	builder.extend(tagged(&ENTRIES[..1], 99)).unwrap();
	let finished = builder.finish();
	let pushed = [DictionaryEntryId::U4(5), DictionaryEntryId::U2(6)];
	assert_eq!(dictionary_parts(&finished), ([ENTRIES.as_slice(), &pushed, &ENTRIES[..1]].concat(), id));
	assert_eq!(dictionary_parts(&ColumnBuilder::like(&finished, 4).finish()), (vec![], id));

	let mut fresh = ColumnBuilder::with_capacity(ValueType::DictionaryId, 2);
	fresh.set_dictionary_id(DictionaryId(3));
	fresh.push(ENTRIES[3]);
	assert_eq!(dictionary_parts(&fresh.finish()), (vec![ENTRIES[3]], Some(DictionaryId(3))));
}

#[test]
fn option_wrapped_dictionary_column_keeps_its_id() {
	// A nullable dictionary column must keep its id through every op, never drop it with the nones.
	let id = Some(DictionaryId(7));
	let buffer =
		tagged(&ENTRIES, 7).with_nulls(NullBuffer::new(BooleanBuffer::from(vec![true, false, true, true])));

	let mut filtered = buffer.clone();
	filtered.filter(&BooleanBuffer::from(vec![false, true, true, true])).unwrap();
	assert_eq!(dictionary_parts(&filtered), (ENTRIES[1..].to_vec(), id));

	let mut reordered = buffer.clone();
	reordered.reorder(&[3, 0]);
	assert_eq!(dictionary_parts(&reordered), (vec![ENTRIES[3], ENTRIES[0]], id));

	assert_eq!(dictionary_parts(&buffer.slice(0, 2)), (ENTRIES[..2].to_vec(), id));
	assert_eq!(dictionary_parts(&buffer.take(3)), (ENTRIES[..3].to_vec(), id));

	let mut builder = buffer.into_builder();
	builder.push_none();
	builder.push_value(Value::DictionaryId(ENTRIES[0]));
	let finished = builder.finish();
	assert_eq!(
		dictionary_parts(&finished),
		([ENTRIES.as_slice(), &[DictionaryEntryId::default(), ENTRIES[0]]].concat(), id)
	);
	assert_eq!(finished.get_value(4), Value::none_of(ValueType::DictionaryId));
	assert_eq!(finished.get_value(5), Value::DictionaryId(ENTRIES[0]));
}

#[test]
fn shared_or_offset_dictionary_buffer_copies_whole_rows_into_the_builder() {
	// Copying 16 byte rows out of a shared buffer shifts every entry and trips the whole row check.
	let id = Some(DictionaryId(7));
	let buffer = tagged(&ENTRIES, 7);

	let mut shared = buffer.clone().into_builder();
	shared.push(DictionaryEntryId::U1(1));
	assert_eq!(
		dictionary_parts(&shared.finish()),
		([ENTRIES.as_slice(), &[DictionaryEntryId::U1(1)]].concat(), id)
	);
	assert_eq!(dictionary_parts(&buffer), (ENTRIES.to_vec(), id));

	let offset = buffer.slice(1, 3);
	let prefix = buffer.take(2);
	drop(buffer);

	let mut offset = offset.into_builder();
	offset.push(ENTRIES[0]);
	assert_eq!(dictionary_parts(&offset.finish()), (vec![ENTRIES[1], ENTRIES[2], ENTRIES[0]], id));

	let mut prefix = prefix.into_builder();
	prefix.push(ENTRIES[3]);
	assert_eq!(dictionary_parts(&prefix.finish()), (vec![ENTRIES[0], ENTRIES[1], ENTRIES[3]], id));
}

#[test]
fn int16_keeps_its_values_and_data_type_through_every_op() {
	// Every op must keep the ordered rows exact; a zero filler row would read back as i128::MIN.
	let buffer = ColumnBuffer::int16(INTS);
	assert_int16(&buffer, &INTS);

	let mut filtered = buffer.clone();
	filtered.filter(&BooleanBuffer::from(vec![true, false, true])).unwrap();
	assert_int16(&filtered, &[i128::MIN, i128::MAX]);

	let mut reordered = buffer.clone();
	reordered.reorder(&[2, 0, 7]);
	assert_int16(&reordered, &[i128::MAX, i128::MIN, 0]);

	assert_int16(&buffer.slice(1, 3), &INTS[1..]);
	assert_int16(&buffer.take(1), &INTS[..1]);
	assert_int16(&buffer.gather(&[2, 2]), &[i128::MAX, i128::MAX]);

	let mut extended = buffer.clone();
	extended.extend(ColumnBuffer::int16([1])).unwrap();
	assert_int16(&extended, &[i128::MIN, 0, i128::MAX, 1]);

	let merged = buffer.scatter_merge(
		&ColumnBuffer::int16([1, 2, 3]),
		&BooleanBuffer::from(vec![true, false, true]),
		&BooleanBuffer::from(vec![false, true, false]),
		3,
	);
	assert_int16(&merged, &[i128::MIN, 2, i128::MAX]);

	for decoded in round_trips(&buffer) {
		assert_int16(&decoded, &INTS);
	}

	let mut builder = buffer.clone().into_builder();
	builder.push_value(Value::Int16(5));
	builder.push(7i8);
	builder.push(9u64);
	assert_int16(&builder.finish(), &[i128::MIN, 0, i128::MAX, 5, 7, 9]);

	let mut fresh = ColumnBuilder::with_capacity(ValueType::Int16, 1);
	fresh.push(i128::MIN);
	assert_int16(&fresh.finish(), &[i128::MIN]);

	assert_int16(&ColumnBuffer::none_typed(ValueType::Int16, 2), &[0, 0]);
	assert_int16(&ColumnBuffer::int16_with_bitvec([4, 0], vec![true, false]), &[4, 0]);
	assert_eq!(buffer.get_as::<i128>(2), Ok(Some(i128::MAX)));
	assert_eq!(buffer.get_value(0), Value::Int16(i128::MIN));
}

#[test]
fn uint16_keeps_values_above_i128_max_and_its_data_type() {
	// Values at and above 2^127 must never pick up a sign on any op.
	let buffer = ColumnBuffer::uint16(UINTS);
	assert_uint16(&buffer, &UINTS);

	let mut filtered = buffer.clone();
	filtered.filter(&BooleanBuffer::from(vec![false, true, false, true])).unwrap();
	assert_uint16(&filtered, &[1 << 64, u128::MAX]);

	let mut reordered = buffer.clone();
	reordered.reorder(&[3, 2, 8]);
	assert_uint16(&reordered, &[u128::MAX, 1 << 127, 0]);

	assert_uint16(&buffer.slice(2, 4), &UINTS[2..]);
	assert_uint16(&buffer.take(2), &UINTS[..2]);
	assert_uint16(&buffer.gather(&[3, 0]), &[u128::MAX, 0]);

	let mut extended = buffer.clone();
	extended.extend(ColumnBuffer::uint16([u128::MAX - 1])).unwrap();
	assert_uint16(&extended, &[0, 1 << 64, 1 << 127, u128::MAX, u128::MAX - 1]);

	let merged = buffer.scatter_merge(
		&ColumnBuffer::uint16([5, 6, 7, 8]),
		&BooleanBuffer::from(vec![false, true, false, true]),
		&BooleanBuffer::from(vec![true, false, true, false]),
		4,
	);
	assert_uint16(&merged, &[5, 1 << 64, 7, u128::MAX]);

	for decoded in round_trips(&buffer) {
		assert_uint16(&decoded, &UINTS);
	}

	let mut builder = buffer.clone().into_builder();
	builder.push_value(Value::Uint16(u128::MAX));
	builder.push(3u8);
	builder.push(-1i8);
	assert_uint16(&builder.finish(), &[0, 1 << 64, 1 << 127, u128::MAX, u128::MAX, 3, 0]);

	let mut fresh = ColumnBuilder::with_capacity(ValueType::Uint16, 1);
	fresh.push(u128::MAX);
	assert_uint16(&fresh.finish(), &[u128::MAX]);

	assert_uint16(&ColumnBuffer::none_typed(ValueType::Uint16, 2), &[0, 0]);
	assert_uint16(&ColumnBuffer::uint16_with_bitvec([u128::MAX, 0], vec![true, false]), &[u128::MAX, 0]);
	assert_eq!(buffer.get_value(3), Value::Uint16(u128::MAX));
	assert_eq!(buffer.get_as::<u128>(3), Ok(Some(u128::MAX)));
	assert_eq!(buffer.get_as::<i128>(3).unwrap_err().code, "CONV_004");
	assert_eq!(buffer.get_as::<u64>(1).unwrap_err().code, "CONV_004");
	assert_eq!(buffer.as_string(3), u128::MAX.to_string());
}
