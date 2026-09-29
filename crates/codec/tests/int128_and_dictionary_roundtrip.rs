// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::repeat_n, slice::from_ref, sync::Arc};

use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, make_array};
use arrow_buffer::NullBuffer;
use arrow_schema::Schema;
use reifydb_codec::frame::{
	decode::decode_frames,
	encode::encode_frames,
	format::{Encoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE},
	options::EncodeOptions,
};
use reifydb_value::value::{
	Value,
	column_view::ColumnView,
	container::{
		dictionary_array::{self, dictionary_array},
		fixed_array,
		wide_int_array::{wide_array, wides},
	},
	dictionary::DictionaryEntryId,
	frame::frame::Frame,
	value_type::{ValueType, field::named},
};

const FIRST_COLUMN_ENCODING_BYTE: usize = MESSAGE_HEADER_SIZE + FRAME_HEADER_SIZE + 1;

const NUMERIC_ENCODINGS: [Encoding; 4] = [Encoding::Plain, Encoding::Rle, Encoding::Delta, Encoding::DeltaRle];

const INT16_EXTREMES: [i128; 4] = [i128::MIN, -1, 0, i128::MAX];

const UINT16_BOUNDARIES: [u128; 4] = [0, 1 << 64, 1 << 127, u128::MAX];

const PAD: usize = 3;

fn frame(value_type: ValueType, array: ArrayRef) -> Frame {
	let (field, array) = named("c", value_type.into(), array);
	Frame::from(RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap())
}

fn view(frame: &Frame) -> ColumnView<'_> {
	ColumnView::try_from((frame.batch.column(0), frame.batch.schema_ref().field(0))).unwrap()
}

fn round_trip(frame: &Frame, encoding: Encoding) -> Frame {
	let encoded = encode_frames(from_ref(frame), &EncodeOptions::forced(encoding)).expect("encode failed");
	// A silent fallback to plain would leave the forced encoding untested.
	assert_eq!(encoded[FIRST_COLUMN_ENCODING_BYTE], encoding as u8, "encoder fell back from {encoding:?}");
	let mut frames = decode_frames(&encoded).expect("decode failed");
	assert_eq!(frames.len(), 1);
	frames.remove(0)
}

fn repeat_each<T: Copy>(values: &[T], times: usize) -> Vec<T> {
	values.iter().flat_map(|&value| repeat_n(value, times)).collect()
}

fn padded<T: Copy>(pad: T, values: &[T]) -> Vec<T> {
	let mut rows = vec![pad; PAD];
	rows.extend_from_slice(values);
	rows.extend(repeat_n(pad, PAD));
	rows
}

fn int16_shapes(encoding: Encoding) -> Vec<Vec<i128>> {
	// Each shape must be one the forced encoding accepts, otherwise the round trip refuses the fallback.
	match encoding {
		Encoding::Rle => vec![repeat_each(&INT16_EXTREMES, 4)],
		Encoding::Delta | Encoding::DeltaRle => {
			vec![vec![i128::MAX - 1, i128::MAX, i128::MIN, i128::MIN + 1], vec![-2, -1, 0, 1]]
		}
		_ => vec![INT16_EXTREMES.to_vec()],
	}
}

fn uint16_shapes(encoding: Encoding) -> Vec<Vec<u128>> {
	// Each shape must be one the forced encoding accepts, otherwise the round trip refuses the fallback.
	match encoding {
		Encoding::Rle => vec![repeat_each(&UINT16_BOUNDARIES, 4)],
		Encoding::Delta | Encoding::DeltaRle => vec![
			vec![u128::MAX - 1, u128::MAX, 0, 1],
			vec![(1 << 64) - 1, 1 << 64, (1 << 64) + 1, (1 << 64) + 2],
			vec![(1 << 127) - 1, 1 << 127, (1 << 127) + 1, (1 << 127) + 2],
		],
		_ => vec![UINT16_BOUNDARIES.to_vec()],
	}
}

fn fixed(frame: &Frame, expected: ValueType) -> &FixedSizeBinaryArray {
	let found = view(frame).get_type();
	assert_eq!(found, expected, "expected a {expected:?} column, found {found:?}");
	frame.batch.column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("a fixed size binary array")
}

fn int16s(frame: &Frame) -> &FixedSizeBinaryArray {
	fixed(frame, ValueType::Int16)
}

fn uint16s(frame: &Frame) -> &FixedSizeBinaryArray {
	fixed(frame, ValueType::Uint16)
}

fn dictionary_column(container: FixedSizeBinaryArray) -> Frame {
	frame(ValueType::DictionaryId, Arc::new(container))
}

fn dictionary_entries(frame: &Frame) -> Vec<DictionaryEntryId> {
	dictionary_array::iter(fixed(frame, ValueType::DictionaryId)).collect()
}

fn with_none_at_row_one(value_type: ValueType, inner: FixedSizeBinaryArray) -> Frame {
	let defined: Vec<bool> = (0..inner.len()).map(|row| row != 1).collect();
	let data = inner.into_data().into_builder().nulls(Some(NullBuffer::from(defined))).build().unwrap();
	frame(ValueType::Option(Box::new(value_type)), make_array(data))
}

fn row_values(frame: &Frame) -> Vec<Value> {
	let view = view(frame);
	(0..view.len()).map(|row| view.get_value(row)).collect()
}

#[test]
fn int16_extremes_round_trip_through_every_encoding() {
	// A narrowing or wrapping encoder would change a value only a 128 bit column can hold.
	for encoding in NUMERIC_ENCODINGS {
		for values in int16_shapes(encoding) {
			let decoded =
				round_trip(&frame(ValueType::Int16, Arc::new(wide_array(values.clone()))), encoding);
			assert_eq!(wides::<i128>(int16s(&decoded)), values, "{encoding:?}");
		}
	}
}

#[test]
fn uint16_width_boundaries_round_trip_through_every_encoding() {
	// Rows at 2^64, 2^127 or u128::MAX would be clipped or pick up a sign through the signed 256 bit native.
	for encoding in NUMERIC_ENCODINGS {
		for values in uint16_shapes(encoding) {
			let decoded =
				round_trip(&frame(ValueType::Uint16, Arc::new(wide_array(values.clone()))), encoding);
			assert_eq!(wides::<u128>(uint16s(&decoded)), values, "{encoding:?}");
		}
	}
}

#[test]
fn dictionary_ids_round_trip_each_width_at_zero_and_its_max() {
	// Writing a row at the wrong width would truncate the max or change the variant on the way back.
	let columns = [
		vec![DictionaryEntryId::U1(0), DictionaryEntryId::U1(u8::MAX)],
		vec![DictionaryEntryId::U2(0), DictionaryEntryId::U2(u16::MAX)],
		vec![DictionaryEntryId::U4(0), DictionaryEntryId::U4(u32::MAX)],
		vec![DictionaryEntryId::U8(0), DictionaryEntryId::U8(u64::MAX)],
		vec![DictionaryEntryId::U16(0), DictionaryEntryId::U16(u128::MAX)],
	];
	for entries in columns {
		let decoded = round_trip(&dictionary_column(dictionary_array(entries.clone())), Encoding::Plain);
		assert_eq!(dictionary_entries(&decoded), entries);
	}
}

#[test]
fn mixed_width_dictionary_ids_keep_every_value_at_the_widest_width() {
	// A row written at its own width inside a column read at the widest width would shift every later row.
	let up_to_u4 = [DictionaryEntryId::U1(u8::MAX), DictionaryEntryId::U4(u32::MAX), DictionaryEntryId::U2(0)];
	let decoded = round_trip(&dictionary_column(dictionary_array(up_to_u4)), Encoding::Plain);
	assert_eq!(
		dictionary_entries(&decoded),
		[DictionaryEntryId::U4(u8::MAX as u32), DictionaryEntryId::U4(u32::MAX), DictionaryEntryId::U4(0)]
	);
	let up_to_u16 = [
		DictionaryEntryId::U1(u8::MAX),
		DictionaryEntryId::U16(u128::MAX),
		DictionaryEntryId::U2(0),
		DictionaryEntryId::U8(u64::MAX),
		DictionaryEntryId::U4(u32::MAX),
		DictionaryEntryId::U1(0),
	];
	let widened: Vec<DictionaryEntryId> =
		up_to_u16.iter().map(|entry| DictionaryEntryId::U16(entry.to_u128())).collect();
	let decoded = round_trip(&dictionary_column(dictionary_array(up_to_u16)), Encoding::Plain);
	assert_eq!(dictionary_entries(&decoded), widened);
}

#[test]
fn option_wrapped_columns_keep_their_none_row_through_every_encoding() {
	// Dropping the none row, or losing its inner type, would shift a value or return an untyped none.
	for encoding in NUMERIC_ENCODINGS {
		for values in int16_shapes(encoding) {
			let column = with_none_at_row_one(ValueType::Int16, wide_array(values));
			let decoded = round_trip(&column, encoding);
			assert_eq!(row_values(&decoded), row_values(&column), "{encoding:?}");
		}
		for values in uint16_shapes(encoding) {
			let column = with_none_at_row_one(ValueType::Uint16, wide_array(values));
			let decoded = round_trip(&column, encoding);
			assert_eq!(row_values(&decoded), row_values(&column), "{encoding:?}");
		}
	}
	let entries = [DictionaryEntryId::U4(0), DictionaryEntryId::U4(7), DictionaryEntryId::U4(u32::MAX)];
	let column = with_none_at_row_one(ValueType::DictionaryId, dictionary_array(entries));
	let decoded = round_trip(&column, Encoding::Plain);
	assert_eq!(row_values(&decoded), row_values(&column));
}

#[test]
fn sliced_columns_round_trip_only_their_window() {
	// An encoder reading from the start of the shared buffer instead of the slice offset would emit the padding.
	for encoding in NUMERIC_ENCODINGS {
		for values in int16_shapes(encoding) {
			let sliced = fixed_array::slice(&wide_array(padded(42, &values)), PAD, PAD + values.len());
			let decoded = round_trip(&frame(ValueType::Int16, Arc::new(sliced)), encoding);
			assert_eq!(wides::<i128>(int16s(&decoded)), values, "{encoding:?}");
		}
		for values in uint16_shapes(encoding) {
			let sliced = fixed_array::slice(&wide_array(padded(42, &values)), PAD, PAD + values.len());
			let decoded = round_trip(&frame(ValueType::Uint16, Arc::new(sliced)), encoding);
			assert_eq!(wides::<u128>(uint16s(&decoded)), values, "{encoding:?}");
		}
	}
	let entries = [DictionaryEntryId::U1(0), DictionaryEntryId::U1(u8::MAX), DictionaryEntryId::U1(7)];
	let all = dictionary_array(padded(DictionaryEntryId::U16(u128::MAX), &entries));
	let sliced = fixed_array::slice(&all, PAD, PAD + entries.len());
	let decoded = round_trip(&dictionary_column(sliced), Encoding::Plain);
	assert_eq!(dictionary_entries(&decoded), entries);
}
