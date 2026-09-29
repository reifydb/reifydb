// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Write as _, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use postcard::{from_bytes, to_stdvec};
use reifydb_column::{
	encoding::{Encoding, canonical::CanonicalEncoding},
	persist::PersistedArray,
};
use reifydb_core::value::{
	batch::{batch, concat_columns, filter, head, take_rows},
	column::{
		builder::ColumnBuilder,
		cast::{cast_column_data, convert::TargetConvert},
		data::{Column, canonical::Canonical},
		factory,
		nulls::with_nulls,
	},
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		blob::Blob,
		column_view::ColumnView,
		container::digest_array::digest_array,
		date::Date,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		digest::Digest,
		duration::Duration,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::{
			ValueType,
			field::{FieldType, from_field, named},
		},
	},
};
use uuid::Uuid;

struct Pin {
	name: &'static str,
	chunk_postcard: &'static str,
}

const PINS: &[Pin] = &[
	Pin {
		name: "zero_nones_int4_from_cast",
		chunk_postcard: "000117050000000503080a0c00",
	},
	Pin {
		name: "zero_nones_utf8_from_cast",
		chunk_postcard: "000117080000000d02016102626300",
	},
	Pin {
		name: "zero_nones_bool_from_cast",
		chunk_postcard: "000117000000000001050300",
	},
	Pin {
		name: "zero_nones_int4_from_filter",
		chunk_postcard: "000117050000000502020600",
	},
	Pin {
		name: "zero_nones_int4_from_slice",
		chunk_postcard: "000117050000000502060801010302",
	},
	Pin {
		name: "zero_rows_int4_from_builder",
		chunk_postcard: "00011705000000050000",
	},
	Pin {
		name: "all_none_bool",
		chunk_postcard: "000117000000000001000301010003",
	},
	Pin {
		name: "all_none_utf8",
		chunk_postcard: "000117080000000d0300000001010003",
	},
	Pin {
		name: "all_none_uuid4",
		chunk_postcard: "00011713000000130310000000000000000000000000000000001000000000000000000000000000000000100000000000000000000000000000000001010003",
	},
	Pin {
		name: "all_none_uuid7",
		chunk_postcard: "00011714000000140310000000000000000000000000000000001000000000000000000000000000000000100000000000000000000000000000000001010003",
	},
	Pin {
		name: "all_none_date",
		chunk_postcard: "0001170e0000000e0300000001010003",
	},
	Pin {
		name: "all_none_datetime",
		chunk_postcard: "0001170f0000000f0300000001010003",
	},
	Pin {
		name: "all_none_time",
		chunk_postcard: "00011710000000100300000001010003",
	},
	Pin {
		name: "all_none_duration",
		chunk_postcard: "00011711000000110300000000000000000001010003",
	},
	Pin {
		name: "all_none_float8",
		chunk_postcard: "00011702000000020300000000000000000000000000000000000000000000000001010003",
	},
	Pin {
		name: "all_none_blob",
		chunk_postcard: "00011715000000150300000001010003",
	},
	Pin {
		name: "option_bool",
		chunk_postcard: "000117000000000001050301010503",
	},
	Pin {
		name: "option_float4",
		chunk_postcard: "0001170100000001030000c03f00000000ffff7fff01010503",
	},
	Pin {
		name: "option_float8",
		chunk_postcard: "00011702000000020300000000000002c00000000000000000ffffffffffffef7f01010503",
	},
	Pin {
		name: "option_int1",
		chunk_postcard: "00011703000000030380007f01010503",
	},
	Pin {
		name: "option_int2",
		chunk_postcard: "000117040000000403ffff0300feff0301010503",
	},
	Pin {
		name: "option_int8",
		chunk_postcard: "000117060000000603ffffffffffffffffff0100feffffffffffffffff0101010503",
	},
	Pin {
		name: "option_uint1",
		chunk_postcard: "0001170900000008030100ff01010503",
	},
	Pin {
		name: "option_uint2",
		chunk_postcard: "0001170a00000009030100ffff0301010503",
	},
	Pin {
		name: "option_uint4",
		chunk_postcard: "0001170b0000000a030100ffffffff0f01010503",
	},
	Pin {
		name: "option_uint8",
		chunk_postcard: "0001170c0000000b030100ffffffffffffffffff0101010503",
	},
	Pin {
		name: "option_date",
		chunk_postcard: "0001170e0000000e03c98e0300dcc30201010503",
	},
	Pin {
		name: "option_datetime",
		chunk_postcard: "0001170f0000000f030200aab490cedc9bb9e73001010503",
	},
	Pin {
		name: "option_time",
		chunk_postcard: "00011710000000100384dcdea0ad6c00ffffbb8ac9d21301010503",
	},
	Pin {
		name: "option_duration",
		chunk_postcard: "0001171100000011030204060000001b3d0001010503",
	},
	Pin {
		name: "option_uuid4",
		chunk_postcard: "000117130000001302100123456789ab4cde8f0123456789abcd100000000000000000000000000000000001010102",
	},
	Pin {
		name: "option_uuid7",
		chunk_postcard: "0001171400000014021000000000000670008000000000000000100000000000000000000000000000000001010102",
	},
	Pin {
		name: "option_blob",
		chunk_postcard: "0001171500000015030300ff07000001010503",
	},
	Pin {
		name: "option_tuple",
		chunk_postcard: "000117180000001702011d0206020901790001010102",
	},
	Pin {
		name: "option_typed_list",
		chunk_postcard: "0001171a050000011a051702011b02060206040001010102",
	},
	Pin {
		name: "option_dictionary_id_with_dictionary",
		chunk_postcard: "0001171900012a00180302070000030901010503",
	},
	Pin {
		name: "sliced_option_int4_at_offset_8",
		chunk_postcard: "000117050000000505a001b40100dc01f00101011b05",
	},
	Pin {
		name: "sliced_option_int4_at_offset_3",
		chunk_postcard: "0001170500000005043c00647801010d04",
	},
	Pin {
		name: "sliced_option_utf8_at_offset_3",
		chunk_postcard: "000117080000000d0302646400016601010503",
	},
	Pin {
		name: "sliced_option_bool_at_offset_3",
		chunk_postcard: "000117000000000001060301010603",
	},
	Pin {
		name: "sliced_option_any_at_offset_3",
		chunk_postcard: "0001171800000017030001010101070c01010603",
	},
	Pin {
		name: "placeholder_int4",
		chunk_postcard: "00011705000000050402c601060801010d04",
	},
	Pin {
		name: "placeholder_utf8",
		chunk_postcard: "000117080000000d030161027a7a016301010503",
	},
	Pin {
		name: "placeholder_int4_take",
		chunk_postcard: "00011705000000050302c6010601010503",
	},
	Pin {
		name: "placeholder_int4_filter",
		chunk_postcard: "00011705000000050302c6010801010503",
	},
	Pin {
		name: "placeholder_int4_reorder",
		chunk_postcard: "00011705000000050308c6010201010503",
	},
	Pin {
		name: "placeholder_int4_extend_by_bare",
		chunk_postcard: "00011705000000050602c60106080a0c01013d06",
	},
	Pin {
		name: "bare_int4_extend_by_placeholder",
		chunk_postcard: "0001170500000005060a0c02c601060801013706",
	},
	Pin {
		name: "untyped_none_extend_by_int4",
		chunk_postcard: "00011705000000050400000a0c01010c04",
	},
	Pin {
		name: "option_digest_null_slot",
		chunk_postcard: "0001171d02904e000000190201100103904e000000018c01010200012e010001010102",
	},
];

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		write!(out, "{byte:02x}").unwrap();
	}
	out
}

fn unhex(text: &str) -> Vec<u8> {
	(0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn chunk_hex(canonical: &Canonical) -> String {
	let persisted = CanonicalEncoding::FIXED.persist(&Column::from_canonical(canonical.clone())).unwrap();
	hex(&to_stdvec(&persisted).unwrap())
}

fn decode_chunk(pinned: &str, ty: &ValueType) -> Result<Arc<Canonical>, String> {
	let persisted: PersistedArray = from_bytes(&unhex(pinned)).map_err(|err| err.to_string())?;
	let column = CanonicalEncoding::FIXED.load(persisted, ty).map_err(|err| err.to_string())?;
	column.to_canonical().map_err(|err| err.to_string())
}

fn column_mismatches(name: &str, column: (FieldRef, ArrayRef)) -> Vec<String> {
	let Some(pin) = PINS.iter().find(|pin| pin.name == name) else {
		return vec![format!("{name}: no pin")];
	};
	let expected = Canonical::from_column(&column).unwrap();
	let written = chunk_hex(&expected);
	let decoded = decode_chunk(pin.chunk_postcard, &expected.view().base_type());
	[
		(written != pin.chunk_postcard).then(|| format!("{name} chunk_postcard: \"{written}\"")),
		match &decoded {
			Ok(decoded)
				if decoded.field_type() == expected.field_type()
					&& decoded.buffer() == expected.buffer() =>
			{
				None
			}
			Ok(decoded) => Some(format!("{name} decode: got {decoded:?}, expected {expected:?}")),
			Err(err) => Some(format!("{name} decode failed: {err}")),
		},
		match &decoded {
			Ok(decoded) => {
				let again = chunk_hex(decoded);
				(again != pin.chunk_postcard).then(|| format!("{name} re-serialized to {again:?}"))
			}
			Err(err) => Some(format!("{name} decode failed: {err}")),
		},
	]
	.into_iter()
	.flatten()
	.collect()
}

fn assert_columns_pinned(fixtures: Vec<(&'static str, (FieldRef, ArrayRef))>) {
	let mismatches: Vec<String> =
		fixtures.into_iter().flat_map(|(name, column)| column_mismatches(name, column)).collect();
	assert!(mismatches.is_empty(), "persisted chunk bytes drifted from the pins:\n{}", mismatches.join("\n"));
}

fn bits(values: &[bool]) -> BooleanBuffer {
	BooleanBuffer::from(values.to_vec())
}

fn uuid7_bits(i: u128) -> Uuid {
	Uuid::from_u128(((i + 1) << 80) | (0x7 << 76) | (0x2 << 62))
}

fn digest_type() -> ValueType {
	ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	}
}

fn digest_of(values: &[f64]) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	digest
}

fn built(ty: ValueType, values: Vec<Option<Value>>) -> (FieldRef, ArrayRef) {
	let mut builder = ColumnBuilder::with_capacity(ty, values.len());
	for value in values {
		match value {
			Some(value) => builder.push_value(value),
			None => builder.push_none(),
		}
	}
	builder.finish("c")
}

fn option_of(ty: ValueType) -> ValueType {
	ValueType::Option(Box::new(ty))
}

fn sliced(column: (FieldRef, ArrayRef), start: usize, end: usize) -> (FieldRef, ArrayRef) {
	(column.0, column.1.slice(start, end - start))
}

fn only_column(batch: &RecordBatch) -> (FieldRef, ArrayRef) {
	(batch.schema_ref().fields()[0].clone(), batch.column(0).clone())
}

fn placeholder_int4() -> (FieldRef, ArrayRef) {
	factory::int4_with_bitvec("c", [1, 99, 3, 4], bits(&[true, false, true, true]))
}

fn placeholder_batch() -> RecordBatch {
	batch(vec![placeholder_int4()]).unwrap()
}

fn with_inner_dictionary_id(column: (FieldRef, ArrayRef), id: DictionaryId) -> (FieldRef, ArrayRef) {
	let mut field_type = from_field(&column.0).unwrap();
	assert_eq!(
		field_type.value_type,
		Some(option_of(ValueType::DictionaryId)),
		"fixture must be an Option(DictionaryId) column"
	);
	field_type.dictionary_id = Some(id);
	named(column.0.name(), field_type, column.1)
}

fn cast_to_option(column: (FieldRef, ArrayRef)) -> (FieldRef, ArrayRef) {
	let view = ColumnView::try_from(&column).unwrap();
	cast_column_data(
		TargetConvert {
			target: None,
		},
		&view,
		option_of(view.get_type()),
		|| Fragment::internal("fixture"),
	)
	.unwrap()
}

fn zero_none_fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	let filtered = only_column(
		&filter(
			&batch(vec![factory::int4_optional("c", [Some(1), None, Some(3)])]).unwrap(),
			&bits(&[true, false, true]),
		)
		.unwrap(),
	);
	vec![
		("zero_nones_int4_from_cast", cast_to_option(factory::int4("c", [4, 5, 6]))),
		("zero_nones_utf8_from_cast", cast_to_option(factory::utf8("c", ["a", "bc"]))),
		("zero_nones_bool_from_cast", cast_to_option(factory::bool("c", [true, false, true]))),
		("zero_nones_int4_from_filter", filtered),
		(
			"zero_nones_int4_from_slice",
			sliced(factory::int4_optional("c", [Some(1), None, Some(3), Some(4)]), 2, 4),
		),
		("zero_rows_int4_from_builder", built(option_of(ValueType::Int4), vec![])),
	]
}

fn all_none_fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	vec![
		("all_none_bool", factory::none_typed("c", ValueType::Boolean, 3)),
		("all_none_utf8", factory::none_typed("c", ValueType::Utf8, 3)),
		("all_none_uuid4", factory::none_typed("c", ValueType::Uuid4, 3)),
		("all_none_uuid7", factory::none_typed("c", ValueType::Uuid7, 3)),
		("all_none_date", factory::none_typed("c", ValueType::Date, 3)),
		("all_none_datetime", factory::none_typed("c", ValueType::DateTime, 3)),
		("all_none_time", factory::none_typed("c", ValueType::Time, 3)),
		("all_none_duration", factory::none_typed("c", ValueType::Duration, 3)),
		("all_none_float8", factory::none_typed("c", ValueType::Float8, 3)),
		("all_none_blob", factory::none_typed("c", ValueType::Blob, 3)),
	]
}

fn option_kind_fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	vec![
		("option_bool", factory::bool_with_bitvec("c", [true, false, true], vec![true, false, true])),
		("option_float4", factory::float4_with_bitvec("c", [1.5, 0.0, f32::MIN], vec![true, false, true])),
		("option_float8", factory::float8_with_bitvec("c", [-2.25, 0.0, f64::MAX], vec![true, false, true])),
		("option_int1", factory::int1_with_bitvec("c", [i8::MIN, 0, i8::MAX], vec![true, false, true])),
		("option_int2", factory::int2_with_bitvec("c", [i16::MIN, 0, i16::MAX], vec![true, false, true])),
		("option_int8", factory::int8_with_bitvec("c", [i64::MIN, 0, i64::MAX], vec![true, false, true])),
		("option_uint1", factory::uint1_with_bitvec("c", [1, 0, u8::MAX], vec![true, false, true])),
		("option_uint2", factory::uint2_with_bitvec("c", [1, 0, u16::MAX], vec![true, false, true])),
		("option_uint4", factory::uint4_with_bitvec("c", [1, 0, u32::MAX], vec![true, false, true])),
		("option_uint8", factory::uint8_with_bitvec("c", [1, 0, u64::MAX], vec![true, false, true])),
		(
			"option_date",
			factory::date_with_bitvec(
				"c",
				[
					Date::from_ymd(1900, 2, 28).unwrap(),
					Date::default(),
					Date::from_ymd(2026, 9, 22).unwrap(),
				],
				vec![true, false, true],
			),
		),
		(
			"option_datetime",
			factory::datetime_with_bitvec(
				"c",
				[
					DateTime::from_nanos(1),
					DateTime::default(),
					DateTime::from_nanos(1_758_500_000_123_456_789),
				],
				vec![true, false, true],
			),
		),
		(
			"option_time",
			factory::time_with_bitvec(
				"c",
				[
					Time::from_hms_nano(1, 2, 3, 4).unwrap(),
					Time::default(),
					Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
				],
				vec![true, false, true],
			),
		),
		(
			"option_duration",
			factory::duration_with_bitvec(
				"c",
				[
					Duration::new(1, 2, 3).unwrap(),
					Duration::default(),
					Duration::new(-14, -30, -86_400_000_000_000).unwrap(),
				],
				vec![true, false, true],
			),
		),
		(
			"option_uuid4",
			factory::uuid4_with_bitvec(
				"c",
				[Uuid4(Uuid::from_u128(0x0123_4567_89ab_4cde_8f01_2345_6789_abcd)), Uuid4::default()],
				vec![true, false],
			),
		),
		(
			"option_uuid7",
			factory::uuid7_with_bitvec("c", [Uuid7(uuid7_bits(5)), Uuid7::default()], vec![true, false]),
		),
		(
			"option_blob",
			factory::blob_with_bitvec(
				"c",
				[Blob::new(vec![0, 255, 7]), Blob::default(), Blob::new(vec![])],
				vec![true, false, true],
			),
		),
		(
			"option_tuple",
			built(
				ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8]),
				vec![Some(Value::Tuple(vec![Value::Int4(1), Value::Utf8("y".to_string())])), None],
			),
		),
		(
			"option_typed_list",
			built(
				ValueType::List(Box::new(ValueType::Int4)),
				vec![Some(Value::List(vec![Value::Int4(1), Value::Int4(2)])), None],
			),
		),
	]
}

fn dictionary_fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	vec![(
		"option_dictionary_id_with_dictionary",
		with_inner_dictionary_id(
			factory::dictionary_id_with_bitvec(
				"c",
				[DictionaryEntryId::U4(7), DictionaryEntryId::default(), DictionaryEntryId::U8(9)],
				vec![true, false, true],
			),
			DictionaryId(42),
		),
	)]
}

fn sliced_fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	let long = || factory::int4_optional("c", (0..16).map(|i| (i % 3 != 1).then_some(i * 10)));
	vec![
		("sliced_option_int4_at_offset_8", sliced(long(), 8, 13)),
		("sliced_option_int4_at_offset_3", sliced(long(), 3, 7)),
		(
			"sliced_option_utf8_at_offset_3",
			sliced(
				factory::utf8_with_bitvec(
					"c",
					["a", "", "ccc", "dd", "", "f"].map(String::from),
					vec![true, false, true, true, false, true],
				),
				3,
				6,
			),
		),
		(
			"sliced_option_bool_at_offset_3",
			sliced(
				factory::bool_with_bitvec(
					"c",
					[true, false, false, false, true, true],
					vec![true, false, true, false, true, true],
				),
				3,
				6,
			),
		),
		(
			"sliced_option_any_at_offset_3",
			sliced(
				factory::any_optional(
					"c",
					[
						Some(Value::Int4(1)),
						None,
						Some(Value::Utf8("c".to_string())),
						None,
						Some(Value::Boolean(true)),
						Some(Value::Int8(6)),
					],
				),
				3,
				6,
			),
		),
	]
}

fn placeholder_fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	vec![
		("placeholder_int4", placeholder_int4()),
		("placeholder_utf8", factory::utf8_with_bitvec("c", ["a", "zz", "c"], bits(&[true, false, true]))),
		("placeholder_int4_take", only_column(&head(&placeholder_batch(), 3))),
		(
			"placeholder_int4_filter",
			only_column(&filter(&placeholder_batch(), &bits(&[true, true, false, true])).unwrap()),
		),
		("placeholder_int4_reorder", only_column(&take_rows(&placeholder_batch(), &[3, 1, 0]).unwrap())),
		(
			"placeholder_int4_extend_by_bare",
			concat_columns(&[placeholder_int4(), factory::int4("c", [5, 6])]).unwrap(),
		),
		(
			"bare_int4_extend_by_placeholder",
			concat_columns(&[factory::int4("c", [5, 6]), placeholder_int4()]).unwrap(),
		),
		(
			"untyped_none_extend_by_int4",
			concat_columns(&[factory::none("c", 2), factory::int4("c", [5, 6])]).unwrap(),
		),
	]
}

fn digest_fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	let first = digest_of(&[1.0, 2.5, -4.0]);
	let digest_column = named("c", FieldType::from(digest_type()), Arc::new(digest_array([Some(&first), None])));
	vec![("option_digest_null_slot", with_nulls(digest_column, NullBuffer::new(bits(&[true, true]))).unwrap())]
}

#[test]
fn every_pin_has_exactly_one_fixture() {
	// A pin whose fixture is dropped stops guarding its bytes without any test going red.
	let column_names: Vec<&str> = [
		zero_none_fixtures(),
		all_none_fixtures(),
		option_kind_fixtures(),
		dictionary_fixtures(),
		sliced_fixtures(),
		placeholder_fixtures(),
		digest_fixtures(),
	]
	.into_iter()
	.flatten()
	.map(|(name, _)| name)
	.collect();
	assert_eq!(column_names, PINS.iter().map(|pin| pin.name).collect::<Vec<_>>());
}

#[test]
fn nullable_columns_with_zero_nones_keep_the_option_wrapper_on_the_wire() {
	// A nullable column with no none row must still write its Option field type.
	assert_columns_pinned(zero_none_fixtures());
}

#[test]
fn all_none_columns_are_pinned() {
	// Every none row must keep exactly today's placeholder and a cleared bit, per kind.
	assert_columns_pinned(all_none_fixtures());
}

#[test]
fn option_columns_of_every_remaining_kind_are_pinned() {
	// Each kind must keep its placeholder under the none row, otherwise stored optional columns drift.
	assert_columns_pinned(option_kind_fixtures());
}

#[test]
fn option_dictionary_id_keeps_its_dictionary_on_the_wire() {
	// The dictionary id must survive under the Option wrapper, never be dropped when the none bitmap moves.
	assert_columns_pinned(dictionary_fixtures());
}

#[test]
fn sliced_option_columns_repack_the_bitmap_from_the_slice_start() {
	// A slice at any offset must write the bitmap and values from the slice start, exactly like a fresh column.
	assert_columns_pinned(sliced_fixtures());
}

#[test]
fn values_under_none_rows_survive_serde_and_row_operations() {
	// A value stored under a none row must be written as stored, never replaced by a default.
	assert_columns_pinned(placeholder_fixtures());
	let error = take_rows(&placeholder_batch(), &[0, 9, 1]).unwrap_err();
	assert_eq!(error.diagnostic().message, "row index 9 out of range for a column of 4 rows");
}

#[test]
fn option_digest_bits_and_slots_are_pinned_independently() {
	// A Digest none must be the null bit, never an empty slot, otherwise the wire carries two kinds of none.
	assert_columns_pinned(digest_fixtures());
}
