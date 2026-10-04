// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::Path, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, LargeStringArray, RecordBatch};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_core::value::{
	batch::{batch, concat_columns, filter, head, take_rows},
	column::{
		builder::ColumnBuilder,
		cast::{cast_column_data, convert::TargetConvert},
		factory,
		nulls::with_nulls,
	},
};
use reifydb_runtime::io::fs::{Create, Open, Pwrite, memory::MemoryFs};
use reifydb_store_column::{
	compress::Compressor,
	convert::to_arrow,
	persist::{BlockHandle, serialize_block},
	reader::SnapshotReader,
	session::new_session,
	snapshot::ColumnBlock,
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{
		Value,
		blob::Blob,
		column_view::ColumnView,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{digest_array::digest_array, primitive, varlen_array},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::{DictionaryEntryId, DictionaryId},
		digest::Digest,
		duration::Duration,
		identity::IdentityId,
		system_columns::SystemColumn,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::{
			ValueType,
			field::{FieldType, from_field, named},
		},
	},
};
use uuid::Uuid;
use vortex_session::VortexSession;

type Column = (FieldRef, ArrayRef);

fn load(bytes: &[u8], session: &VortexSession) -> Result<ColumnBlock> {
	let fs = MemoryFs::new();
	let path = Path::new("/block.borg");
	let written = fs.create(path, bytes.len() as u64).unwrap().pwrite(0, bytes).unwrap();
	assert_eq!(written, bytes.len(), "the fixture file must hold every byte");
	BlockHandle::open(fs.open(path).unwrap(), session.clone())?.read(None)
}

fn system_columns(rows: usize) -> Vec<Column> {
	vec![
		factory::uint8(SystemColumn::RowNumbers.name(), (1..=rows as u64).collect::<Vec<_>>()),
		factory::datetime(SystemColumn::CreatedAt.name(), vec![DateTime::from_nanos(1); rows]),
		factory::datetime(SystemColumn::UpdatedAt.name(), vec![DateTime::from_nanos(2); rows]),
	]
}

fn round_trip_all(label: &str, columns: &[Column]) -> Vec<Column> {
	let rows = columns[0].1.len();
	let compressor = Compressor::new(new_session());
	let mut schema = Vec::new();
	let mut chunks = Vec::new();
	for column in system_columns(rows).iter().chain(columns) {
		let ty = ColumnView::try_from(column)
			.unwrap_or_else(|err| panic!("{label}: view failed: {err}"))
			.get_type();
		schema.push((column.0.name().to_string(), ty.clone(), column.0.is_nullable()));
		chunks.push(compressor
			.compress(ty, column)
			.unwrap_or_else(|err| panic!("{label}: compress failed: {err}")));
	}
	let block = ColumnBlock::new(Arc::new(schema), chunks);
	let bytes =
		serialize_block(&block, &new_session()).unwrap_or_else(|err| panic!("{label}: persist failed: {err}"));
	let session = new_session();
	let restored = load(&bytes, &session).unwrap_or_else(|err| panic!("{label}: load failed: {err}"));
	let restored = Arc::new(restored);
	let mut reader = SnapshotReader::new(Arc::clone(&restored), rows.max(1), session.clone());
	match reader.next() {
		Some(batch) => {
			let batch = batch.unwrap_or_else(|err| panic!("{label}: read failed: {err}"));
			assert!(reader.next().is_none(), "{label}: a batch as large as the block must hold every row");
			columns.iter().map(|column| by_name(&batch, column.0.name())).collect()
		}
		None => {
			assert_eq!(rows, 0, "{label}: the reader yielded no batch for a block with rows");
			columns.iter()
				.map(|column| {
					let (_, chunks) = restored.column_by_name(column.0.name()).unwrap();
					to_arrow(
						&session,
						column.0.name(),
						&chunks.field_type,
						chunks.chunks[0].clone(),
					)
					.unwrap_or_else(|err| panic!("{label}: export failed: {err}"))
				})
				.collect()
		}
	}
}

fn by_name(batch: &RecordBatch, name: &str) -> Column {
	let (index, _) = batch.schema_ref().column_with_name(name).unwrap_or_else(|| panic!("{name} missing"));
	(batch.schema_ref().fields()[index].clone(), batch.column(index).clone())
}

fn mismatches(label: &str, input: &Column, output: &Column) -> Vec<String> {
	let mut out = Vec::new();
	let (expected, actual) = (from_field(&input.0).unwrap(), from_field(&output.0).unwrap());
	if expected != actual {
		out.push(format!("{label}: field type {actual:?}, expected {expected:?}"));
	}
	if input.1.len() != output.1.len() {
		out.push(format!("{label}: {} rows, expected {}", output.1.len(), input.1.len()));
		return out;
	}
	let (expected, actual) = (ColumnView::try_from(input).unwrap(), ColumnView::try_from(output).unwrap());
	for row in 0..input.1.len() {
		let (want, got) = (expected.get_value(row), actual.get_value(row));
		if want != got {
			out.push(format!("{label}: row {row} read back {got:?}, expected {want:?}"));
		}
	}
	out
}

fn assert_all_round_trip(fixtures: Vec<(&str, Column)>) {
	let mut failures = Vec::new();
	for (label, column) in &fixtures {
		let output = round_trip_all(label, std::slice::from_ref(column)).remove(0);
		failures.extend(mismatches(label, column, &output));
	}
	assert!(failures.is_empty(), "columns changed across compress, persist and reload:\n{}", failures.join("\n"));
}

fn bits(values: &[bool]) -> BooleanBuffer {
	BooleanBuffer::from(values.to_vec())
}

fn uuid7_bits(i: u128) -> Uuid {
	Uuid::from_u128(((i + 1) << 80) | (0x7 << 76) | (0x2 << 62))
}

fn sliced(column: Column, start: usize, end: usize) -> Column {
	(column.0, column.1.slice(start, end - start))
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

fn digest_column() -> Column {
	let mut builder = ColumnBuilder::with_capacity(digest_type(), 2);
	builder.push_value(Value::Digest(Box::new(digest_of(&[1.0, 2.5, -4.0]))));
	builder.push_none();
	builder.finish("c")
}

fn built(ty: ValueType, values: Vec<Option<Value>>) -> Column {
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

fn only_column(batch: &RecordBatch) -> Column {
	(batch.schema_ref().fields()[0].clone(), batch.column(0).clone())
}

fn placeholder_int4() -> Column {
	factory::int4_with_bitvec("c", [1, 99, 3, 4], bits(&[true, false, true, true]))
}

fn placeholder_batch() -> RecordBatch {
	batch(vec![placeholder_int4()]).unwrap()
}

fn with_dictionary_id(column: Column, id: DictionaryId) -> Column {
	let mut field_type = from_field(&column.0).unwrap();
	field_type.dictionary_id = Some(id);
	named(column.0.name(), field_type, column.1)
}

fn cast_to_option(column: Column) -> Column {
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

fn decimals<const N: usize>(texts: [&str; N]) -> [Decimal; N] {
	texts.map(|text| text.parse::<Decimal>().unwrap())
}

fn decimal_value(text: &str) -> Value {
	Value::Decimal(text.parse::<Decimal>().unwrap())
}

fn serde_pin_fixtures() -> Vec<(&'static str, Column)> {
	vec![
		("bool", factory::bool("c", [true, false, true])),
		("float4", factory::float4("c", [f32::MIN, -0.0, 1.5, f32::MAX])),
		("float8", factory::float8("c", [f64::MIN, -0.0, 2.25, f64::MAX])),
		("int1", factory::int1("c", [i8::MIN, 0, i8::MAX])),
		("int2", factory::int2("c", [i16::MIN, 0, i16::MAX])),
		("int4", factory::int4("c", [i32::MIN, 0, i32::MAX])),
		("int8", factory::int8("c", [i64::MIN, 0, i64::MAX])),
		("int16", factory::int16("c", [i128::MIN, 0, i128::MAX])),
		("uint1", factory::uint1("c", [0, 1, u8::MAX])),
		("uint2", factory::uint2("c", [0, 1, u16::MAX])),
		("uint4", factory::uint4("c", [0, 1, u32::MAX])),
		("uint8", factory::uint8("c", [0, 1, u64::MAX])),
		("uint16", factory::uint16("c", [0, 1, u128::MAX])),
		("utf8", factory::utf8("c", ["a", "", "h\u{e9}llo"])),
		(
			"date",
			factory::date(
				"c",
				[
					Date::from_ymd(1970, 1, 1).unwrap(),
					Date::from_ymd(1900, 2, 28).unwrap(),
					Date::from_ymd(2026, 9, 22).unwrap(),
				],
			),
		),
		(
			"datetime",
			factory::datetime(
				"c",
				[DateTime::from_nanos(0), DateTime::from_nanos(1_758_500_000_123_456_789)],
			),
		),
		(
			"time",
			factory::time(
				"c",
				[
					Time::from_hms_nano(0, 0, 0, 0).unwrap(),
					Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
				],
			),
		),
		(
			"duration",
			factory::duration(
				"c",
				[
					Duration::new(0, 0, 0).unwrap(),
					Duration::new(1, 2, 3).unwrap(),
					Duration::new(-14, -30, -86_400_000_000_000).unwrap(),
				],
			),
		),
		(
			"identity_id",
			factory::identity_id("c", [IdentityId(Uuid7(uuid7_bits(0))), IdentityId(Uuid7(uuid7_bits(1)))]),
		),
		("uuid4", factory::uuid4("c", [Uuid4(Uuid::from_u128(0x0123_4567_89ab_4cde_8f01_2345_6789_abcd))])),
		("uuid7", factory::uuid7("c", [Uuid7(uuid7_bits(2)), Uuid7(uuid7_bits(3))])),
		("blob", factory::blob("c", [Blob::new(vec![]), Blob::new(vec![0, 255, 7])])),
		(
			"decimal",
			factory::decimal(
				"c",
				Precision::MAX,
				Scale::new(6),
				["0", "-1.5", "123456789012345678901234567890.000001"]
					.map(|s| s.parse::<Decimal>().unwrap()),
			),
		),
		("any", factory::any_optional("c", [Some(Value::Int4(7)), Some(Value::Utf8("x".to_string())), None])),
		(
			"any_typed_list",
			factory::any_typed(
				"c",
				[Value::List(vec![Value::Int4(1), Value::Int4(2)]), Value::List(vec![])],
				ValueType::List(Box::new(ValueType::Int4)),
			),
		),
		(
			"dictionary_id",
			factory::dictionary_id("c", [DictionaryEntryId::U4(1), DictionaryEntryId::U4(u32::MAX)]),
		),
		("dictionary_id_u16", factory::dictionary_id("c", [DictionaryEntryId::U16(u128::MAX)])),
		("option_int4", factory::int4_optional("c", [Some(1), None, Some(-3)])),
		("option_utf8", factory::utf8_with_bitvec("c", ["a".to_string(), String::new()], vec![true, false])),
		("none_typed_int8", factory::none_typed("c", ValueType::Int8, 2)),
		("digest", digest_column()),
		("sliced_int4", sliced(factory::int4("c", [1, 2, 3, 4, 5]), 1, 4)),
		("sliced_utf8", sliced(factory::utf8("c", ["a", "bb", "ccc", "dddd"]), 1, 3)),
		(
			"sliced_bool",
			sliced(
				factory::bool("c", [true, false, true, true, false, false, true, false, true, true]),
				3,
				9,
			),
		),
		(
			"sliced_option_int4",
			sliced(factory::int4_optional("c", [Some(1), None, Some(3), None, Some(5)]), 1, 4),
		),
	]
}

fn zero_none_fixtures() -> Vec<(&'static str, Column)> {
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

fn all_none_fixtures() -> Vec<(&'static str, Column)> {
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

fn option_kind_fixtures() -> Vec<(&'static str, Column)> {
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
			"option_identity_id",
			factory::identity_id_with_bitvec(
				"c",
				[
					IdentityId(Uuid7(Uuid::from_u128(0x0000_0000_0001_7000_8000_0000_0000_0000))),
					IdentityId::default(),
				],
				vec![true, false],
			),
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

fn dictionary_fixtures() -> Vec<(&'static str, Column)> {
	vec![(
		"option_dictionary_id_with_dictionary",
		with_dictionary_id(
			factory::dictionary_id_with_bitvec(
				"c",
				[DictionaryEntryId::U4(7), DictionaryEntryId::default(), DictionaryEntryId::U8(9)],
				vec![true, false, true],
			),
			DictionaryId(42),
		),
	)]
}

fn sliced_fixtures() -> Vec<(&'static str, Column)> {
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

fn placeholder_fixtures() -> Vec<(&'static str, Column)> {
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

fn digest_fixtures() -> Vec<(&'static str, Column)> {
	let first = digest_of(&[1.0, 2.5, -4.0]);
	let digest_column = named("c", FieldType::from(digest_type()), Arc::new(digest_array([Some(&first), None])));
	vec![("option_digest_null_slot", with_nulls(digest_column, NullBuffer::new(bits(&[true, true]))).unwrap())]
}

fn five_dictionary_entries() -> Column {
	factory::dictionary_id(
		"c",
		[
			DictionaryEntryId::U4(1),
			DictionaryEntryId::U4(2),
			DictionaryEntryId::U4(3),
			DictionaryEntryId::U4(4),
			DictionaryEntryId::U4(5),
		],
	)
}

fn wide_int_and_dictionary_fixtures() -> Vec<(&'static str, Column)> {
	vec![
		(
			"dictionary_id_with_some_dictionary_id",
			with_dictionary_id(
				factory::dictionary_id("c", [DictionaryEntryId::U4(1), DictionaryEntryId::U4(2)]),
				DictionaryId(42),
			),
		),
		(
			"dictionary_id_u1_rows",
			factory::dictionary_id("c", [DictionaryEntryId::U1(1), DictionaryEntryId::U1(u8::MAX)]),
		),
		(
			"dictionary_id_u2_rows",
			factory::dictionary_id("c", [DictionaryEntryId::U2(1), DictionaryEntryId::U2(u16::MAX)]),
		),
		(
			"dictionary_id_u8_rows",
			factory::dictionary_id("c", [DictionaryEntryId::U8(1), DictionaryEntryId::U8(u64::MAX)]),
		),
		(
			"dictionary_id_mixed_width_rows",
			factory::dictionary_id(
				"c",
				[
					DictionaryEntryId::U1(3),
					DictionaryEntryId::U4(7),
					DictionaryEntryId::U8(9),
					DictionaryEntryId::U16(1),
				],
			),
		),
		(
			"dictionary_id_u1_zero_placeholder_rows",
			factory::dictionary_id(
				"c",
				[DictionaryEntryId::U1(0), DictionaryEntryId::U1(0), DictionaryEntryId::U1(0)],
			),
		),
		("sliced_int16", sliced(factory::int16("c", [i128::MIN, -2, i128::MAX, 2, 0]), 1, 4)),
		("sliced_uint16", sliced(factory::uint16("c", [0, 1, u128::MAX, 3, 4]), 1, 4)),
		("sliced_dictionary_id", sliced(five_dictionary_entries(), 1, 4)),
		(
			"sliced_dictionary_id_keeps_some_dictionary_id",
			sliced(with_dictionary_id(five_dictionary_entries(), DictionaryId(42)), 1, 4),
		),
		(
			"option_int16_with_none_row",
			factory::int16_with_bitvec("c", [i128::MIN, 0, i128::MAX], vec![true, false, true]),
		),
		(
			"option_uint16_with_none_row",
			factory::uint16_with_bitvec("c", [1, 0, u128::MAX], vec![true, false, true]),
		),
		(
			"option_dictionary_id_with_none_row",
			factory::dictionary_id_with_bitvec(
				"c",
				[DictionaryEntryId::U4(7), DictionaryEntryId::default(), DictionaryEntryId::U8(9)],
				vec![true, false, true],
			),
		),
	]
}

fn mixed_scale_decimal() -> Column {
	factory::decimal(
		"c",
		Precision::new(12),
		Scale::new(7),
		decimals(["1.5", "1.50", "1.500", "0", "0.00", "-0.0", "0.0000001", "1E+3", "-123.4500", "100"]),
	)
}

fn bignum_any_and_digest_fixtures() -> Vec<(&'static str, Column)> {
	let mut sliced_digest = ColumnBuilder::with_capacity(digest_type(), 4);
	for values in [[1.0, 2.0], [3.5, -1.0], [10.0, 20.0], [0.25, 0.5]] {
		sliced_digest.push_value(Value::Digest(Box::new(digest_of(&values))));
	}
	let mut tuple = ColumnBuilder::with_capacity(ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8]), 1);
	tuple.push_value(Value::Tuple(vec![Value::Int4(1), Value::Utf8("y".to_string())]));
	let digest = digest_of(&[1.0, 2.5, -4.0]);
	vec![
		("decimal_mixed_input_scales", mixed_scale_decimal()),
		(
			"decimal_declared_precision_and_scale",
			factory::decimal("c", Precision::new(10), Scale::new(2), decimals(["1.25", "-0.5"])),
		),
		(
			"option_decimal_with_none_row",
			factory::decimal_with_bitvec(
				"c",
				Precision::MAX,
				Scale::new(2),
				["1.50".parse::<Decimal>().unwrap(), Decimal::default()],
				vec![true, false],
			),
		),
		("option_any_with_none_row", factory::any_optional("c", [Some(Value::Utf8("a".to_string())), None])),
		(
			"sliced_decimal",
			sliced(
				factory::decimal(
					"c",
					Precision::MAX,
					Scale::new(3),
					decimals(["1.10", "2.500", "-3.0", "4.00"]),
				),
				1,
				3,
			),
		),
		(
			"sliced_any",
			sliced(
				factory::any(
					"c",
					[
						Value::Int4(1),
						Value::Utf8("b".to_string()),
						decimal_value("1.50"),
						Value::Int4(4),
					],
				),
				1,
				3,
			),
		),
		("sliced_digest", sliced(sliced_digest.finish("c"), 1, 3)),
		(
			"digest_with_none_slot",
			named(
				"c",
				FieldType::from(ValueType::Option(Box::new(digest_type()))),
				Arc::new(digest_array([Some(&digest), None])),
			),
		),
		(
			"record_typed_any_with_placeholder",
			factory::any_typed(
				"c",
				[
					Value::Record(vec![
						("a".to_string(), Value::Int4(1)),
						("b".to_string(), Value::Utf8("x".to_string())),
					]),
					Value::Record(vec![]),
				],
				ValueType::Record(vec![
					("a".to_string(), ValueType::Int4),
					("b".to_string(), ValueType::Utf8),
				]),
			),
		),
		("tuple_column", tuple.finish("c")),
		(
			"any_with_nested_and_typed_none_values",
			factory::any(
				"c",
				[
					Value::List(vec![Value::Int4(1), Value::none_of(ValueType::Int4)]),
					decimal_value("1.50"),
					Value::Type(ValueType::Int4),
					Value::Digest(Box::new(digest_of(&[1.0, 2.5, -4.0]))),
				],
			),
		),
		("none_typed_list", factory::none_typed("c", ValueType::List(Box::new(ValueType::Int4)), 2)),
		(
			"none_typed_record",
			factory::none_typed("c", ValueType::Record(vec![("a".to_string(), ValueType::Int4)]), 1),
		),
		("none_typed_decimal", factory::none_typed("c", ValueType::DECIMAL, 2)),
		("none_typed_digest", factory::none_typed("c", digest_type(), 2)),
		(
			"any_with_every_value_variant",
			factory::any(
				"c",
				[
					Value::List(vec![Value::none_of(ValueType::Utf8)]),
					Value::Boolean(true),
					Value::float4(1.5f32),
					Value::float8(-0.0),
					Value::Int1(i8::MIN),
					Value::Int2(i16::MIN),
					Value::Int4(i32::MIN),
					Value::Int8(i64::MIN),
					Value::Int16(i128::MIN),
					Value::Utf8("h\u{e9}llo".to_string()),
					Value::Uint1(u8::MAX),
					Value::Uint2(u16::MAX),
					Value::Uint4(u32::MAX),
					Value::Uint8(u64::MAX),
					Value::Uint16(u128::MAX),
					Value::Date(Date::from_ymd(2026, 9, 22).unwrap()),
					Value::DateTime(DateTime::from_nanos(1_758_500_000_123_456_789)),
					Value::Time(Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap()),
					Value::Duration(Duration::new(1, 2, 3).unwrap()),
					Value::IdentityId(IdentityId(Uuid7(Uuid::from_u128(
						0x0000_0000_0001_7000_8000_0000_0000_0000,
					)))),
					Value::Uuid4(Uuid4(Uuid::from_u128(0x0123_4567_89ab_4cde_8f01_2345_6789_abcd))),
					Value::Uuid7(Uuid7(Uuid::from_u128(0x0000_0000_0002_7000_8000_0000_0000_0000))),
					Value::Blob(Blob::new(vec![0, 255, 7])),
					decimal_value("-123.4500"),
					Value::Any(Box::new(Value::Int4(5))),
					Value::DictionaryId(DictionaryEntryId::U4(3)),
					Value::Type(ValueType::DECIMAL),
					Value::List(vec![Value::Int4(1), Value::Utf8("z".to_string())]),
					Value::Record(vec![("f".to_string(), Value::Boolean(false))]),
					Value::Tuple(vec![Value::Int4(1), Value::Boolean(true)]),
					Value::Digest(Box::new(digest_of(&[1.0, 2.5, -4.0]))),
				],
			),
		),
	]
}

fn nullable_column(ty: ValueType, max_bytes: Option<MaxBytes>, array: ArrayRef) -> Column {
	let field_type = FieldType {
		value_type: Some(option_of(ty)),
		max_bytes,
		..FieldType::default()
	};
	named("a", field_type, array)
}

fn all_valid_int4(values: Vec<i32>) -> ArrayRef {
	let len = values.len();
	Arc::new(primitive::attach_nulls(Int32Array::from(values), Some(NullBuffer::new_valid(len))))
}

fn all_valid_utf8(values: Vec<&str>) -> ArrayRef {
	let len = values.len();
	Arc::new(varlen_array::attach_nulls(LargeStringArray::from(values), Some(NullBuffer::new_valid(len))))
}

#[test]
fn every_serde_pin_fixture_round_trips() {
	// Every kind the old wire format pinned must read back with its field type and every row intact.
	assert_all_round_trip(serde_pin_fixtures());
}

#[test]
fn option_column_fixtures_round_trip() {
	// Nones, slices, placeholders and dictionaries must survive, never turn a none into a default value.
	let fixtures = [
		zero_none_fixtures(),
		all_none_fixtures(),
		option_kind_fixtures(),
		dictionary_fixtures(),
		sliced_fixtures(),
		placeholder_fixtures(),
		digest_fixtures(),
	];
	assert_all_round_trip(fixtures.into_iter().flatten().collect());
}

#[test]
fn wide_int_and_dictionary_columns_round_trip() {
	// Byte-list columns must restore exact 128-bit values, entry widths and the dictionary id.
	assert_all_round_trip(wide_int_and_dictionary_fixtures());
}

#[test]
fn bignum_any_and_digest_columns_round_trip() {
	// A column holds one decimal scale, so mixed input scales must read back at exactly that scale.
	assert_all_round_trip(bignum_any_and_digest_fixtures());
	let column = mixed_scale_decimal();
	let output = round_trip_all("decimal_mixed_input_scales", std::slice::from_ref(&column)).remove(0);
	let view = ColumnView::try_from(&output).unwrap();
	let rows: Vec<String> = (0..output.1.len()).map(|i| view.as_string(i)).collect();
	let expected = [
		"1.5000000",
		"1.5000000",
		"1.5000000",
		"0.0000000",
		"0.0000000",
		"0.0000000",
		"0.0000001",
		"1000.0000000",
		"-123.4500000",
		"100.0000000",
	];
	assert_eq!(rows, expected, "decimal rows must read back at the column scale");
}

#[test]
fn nullable_blocks_round_trip() {
	// A nullable column must stay nullable with or without a none bitmap, and keep its max_bytes.
	assert_all_round_trip(vec![
		("nullable_int4_all_set_bitmap", nullable_column(ValueType::Int4, None, all_valid_int4(vec![1, 2, 3]))),
		(
			"nullable_utf8_all_set_bitmap",
			nullable_column(ValueType::Utf8, Some(MaxBytes::MAX), all_valid_utf8(vec!["a", "bc"])),
		),
		(
			"nullable_int4_without_bitmap",
			nullable_column(ValueType::Int4, None, Arc::new(Int32Array::from(vec![1, 2, 3]))),
		),
	]);
}

#[test]
fn a_multi_column_block_round_trips() {
	// A column that mis-sizes its bytes would shift every column stored after it.
	let cases = [
		("id_and_name", vec![factory::uint8("id", vec![1u64, 2, 3]), factory::utf8("name", ["x", "y", "z"])]),
		(
			"untyped_any_then_int4",
			vec![
				factory::any_optional("value", [Some(Value::Int4(1)), None]),
				factory::int4("after", [9, 10]),
			],
		),
	];
	let mut failures = Vec::new();
	for (label, columns) in &cases {
		let outputs = round_trip_all(label, columns);
		for (input, output) in columns.iter().zip(&outputs) {
			failures.extend(mismatches(&format!("{label}.{}", input.0.name()), input, output));
		}
	}
	assert!(failures.is_empty(), "multi-column blocks changed across the round trip:\n{}", failures.join("\n"));
}
