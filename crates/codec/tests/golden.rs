// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Golden byte-stability pins. Every case encodes a fixed input and compares against the checked-in
//! fixture under crates/codec/golden/. A mismatch means the wire format changed: that is a
//! coordinated workspace break (TypeScript port, persisted data), never a test to regenerate
//! casually. Regenerate intentionally with REIFYDB_GOLDEN_WRITE=1 after user sign-off.

use std::{fs, path::PathBuf, str::FromStr};

use num_bigint::BigInt;
use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	key::serializer::KeySerializer,
	tag::ValueKind,
	value::encode_value,
};
use reifydb_value::value::{
	Value,
	blob::Blob,
	container::{any::AnyContainer, bool::BoolContainer, number::NumberContainer, utf8::Utf8Container},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	identity::IdentityId,
	int::Int,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

fn golden_dir() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("golden")
}

fn write_mode() -> bool {
	std::env::var("REIFYDB_GOLDEN_WRITE").is_ok_and(|v| v == "1")
}

fn check_case(rel_path: &str, bytes: &[u8]) {
	let path = golden_dir().join(rel_path);
	if write_mode() {
		fs::create_dir_all(path.parent().unwrap()).unwrap();
		fs::write(&path, bytes).unwrap();
		return;
	}
	let expected = fs::read(&path).unwrap_or_else(|e| {
		panic!("missing golden fixture {rel_path} ({e}); run with REIFYDB_GOLDEN_WRITE=1 to create")
	});
	assert_eq!(
		expected,
		bytes,
		"golden mismatch for {rel_path}: the byte format changed (expected {}, got {})",
		hex_string(&expected),
		hex_string(bytes)
	);
}

fn hex_string(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn value_cases() -> Vec<(&'static str, Value)> {
	vec![
		("value/none_any.bin", Value::none()),
		("value/none_duration.bin", Value::none_of(ValueType::Duration)),
		("value/none_option_duration.bin", Value::none_of(ValueType::Option(Box::new(ValueType::Duration)))),
		(
			"value/none_option3_duration.bin",
			Value::none_of(ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Option(
				Box::new(ValueType::Duration),
			)))))),
		),
		("value/none_record.bin", Value::none_of(ValueType::Record(vec![("a".to_string(), ValueType::Int4)]))),
		("value/boolean_true.bin", Value::Boolean(true)),
		("value/float4.bin", Value::Float4(OrderedF32::try_from(3.5f32).unwrap())),
		("value/float8.bin", Value::Float8(OrderedF64::try_from(-2.25f64).unwrap())),
		("value/int1_min.bin", Value::Int1(i8::MIN)),
		("value/int2.bin", Value::Int2(-2)),
		("value/int4.bin", Value::Int4(42)),
		("value/int8_min.bin", Value::Int8(i64::MIN)),
		("value/int16_max.bin", Value::Int16(i128::MAX)),
		("value/utf8.bin", Value::Utf8("reify".to_string())),
		("value/uint1.bin", Value::Uint1(1)),
		("value/uint2.bin", Value::Uint2(2)),
		("value/uint4.bin", Value::Uint4(4)),
		("value/uint8.bin", Value::Uint8(8)),
		("value/uint16_max.bin", Value::Uint16(u128::MAX)),
		("value/date_epoch.bin", Value::Date(Date::from_days_since_epoch(0).unwrap())),
		("value/datetime.bin", Value::DateTime(DateTime::from_nanos(1_700_000_000_000_000_000))),
		("value/time_noon.bin", Value::Time(Time::from_nanos_since_midnight(43_200_000_000_000).unwrap())),
		("value/duration.bin", Value::Duration(Duration::new(1, 2, 3).unwrap())),
		("value/identity_id.bin", Value::IdentityId(IdentityId::new(Uuid7(uuid::Uuid::from_u128(7))))),
		("value/uuid4_nil.bin", Value::Uuid4(Uuid4(uuid::Uuid::nil()))),
		("value/uuid7.bin", Value::Uuid7(Uuid7(uuid::Uuid::from_u128(0x0123_4567_89ab_cdef)))),
		("value/blob.bin", Value::Blob(Blob::new(vec![0x00, 0xff, 0x7f]))),
		(
			"value/int_big_negative.bin",
			Value::Int(Int(BigInt::parse_bytes(b"-12345678901234567890", 10).unwrap())),
		),
		("value/uint_big.bin", Value::Uint(Uint(BigInt::parse_bytes(b"98765432109876543210", 10).unwrap()))),
		("value/decimal_pi.bin", Value::Decimal(Decimal::from_str("3.14159").unwrap())),
		("value/any_int4.bin", Value::Any(Box::new(Value::Int4(5)))),
		("value/any_none_duration.bin", Value::Any(Box::new(Value::none_of(ValueType::Duration)))),
		("value/dictionary_id_u2.bin", Value::DictionaryId(DictionaryEntryId::U2(300))),
		("value/type_option_int4.bin", Value::Type(ValueType::Option(Box::new(ValueType::Int4)))),
		(
			"value/list_mixed.bin",
			Value::List(vec![
				Value::Int4(1),
				Value::Utf8("two".to_string()),
				Value::none_of(ValueType::Int4),
			]),
		),
		("value/record.bin", Value::Record(vec![("k".to_string(), Value::Boolean(false))])),
		("value/tuple.bin", Value::Tuple(vec![Value::Int1(1), Value::Uint1(2)])),
	]
}

#[test]
fn golden_value_codec() {
	for (path, value) in value_cases() {
		let bytes = encode_value(&value).unwrap();
		check_case(path, &bytes);
	}
}

#[test]
fn golden_key_codec() {
	let cases: Vec<(&str, Value)> = vec![
		("key/none_any.bin", Value::none()),
		("key/none_option_duration.bin", Value::none_of(ValueType::Option(Box::new(ValueType::Duration)))),
		("key/boolean_true.bin", Value::Boolean(true)),
		("key/int4_positive.bin", Value::Int4(42)),
		("key/int4_negative.bin", Value::Int4(-42)),
		("key/int8_min.bin", Value::Int8(i64::MIN)),
		("key/uint8_max.bin", Value::Uint8(u64::MAX)),
		("key/float8_negative.bin", Value::Float8(OrderedF64::try_from(-1.5).unwrap())),
		("key/utf8_escaped.bin", Value::Utf8("a\u{00}b".to_string())),
		("key/blob_ff.bin", Value::Blob(Blob::new(vec![0xff, 0x00, 0xff]))),
		("key/date.bin", Value::Date(Date::from_days_since_epoch(19_000).unwrap())),
		("key/dictionary_id_u4.bin", Value::DictionaryId(DictionaryEntryId::U4(70_000))),
	];
	for (path, value) in cases {
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		check_case(path, ser.finish().as_slice());
	}
}

#[test]
fn golden_rbcf_frames() {
	let concrete_columns = vec![
		FrameColumn {
			name: "bools".to_string(),
			data: FrameColumnData::Bool(BoolContainer::new(vec![true, false, true])),
		},
		FrameColumn {
			name: "ints".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![1, 2, 3])),
		},
		FrameColumn {
			name: "texts".to_string(),
			data: FrameColumnData::Utf8(Utf8Container::new(vec![
				"a".to_string(),
				"bb".to_string(),
				"ccc".to_string(),
			])),
		},
		FrameColumn {
			name: "anys".to_string(),
			data: FrameColumnData::Any(AnyContainer::new(vec![
				Box::new(Value::Int4(9)),
				Box::new(Value::none_of(ValueType::Duration)),
				Box::new(Value::Utf8("x".to_string())),
			])),
		},
	];
	let frame = Frame::new(concrete_columns);
	let bytes = encode_frames(&[frame], &EncodeOptions::none()).unwrap();
	check_case("frames/plain_mixed.bin", &bytes);
}

#[test]
fn golden_tag_kinds_json() {
	let mut entries: Vec<String> =
		ValueKind::ALL.iter().map(|kind| format!("\t\"{:?}\": {}", kind, kind.byte())).collect();
	entries.sort_by_key(|line| line.split(": ").nth(1).unwrap().parse::<u8>().unwrap());
	let json = format!("{{\n{}\n}}\n", entries.join(",\n"));
	let path = golden_dir().join("tag/kinds.json");
	if write_mode() {
		fs::create_dir_all(path.parent().unwrap()).unwrap();
		fs::write(&path, json.as_bytes()).unwrap();
		return;
	}
	let expected = fs::read_to_string(&path).unwrap_or_else(|e| {
		panic!("missing golden fixture tag/kinds.json ({e}); run with REIFYDB_GOLDEN_WRITE=1 to create")
	});
	assert_eq!(expected, json, "the ValueKind numbering changed");
}
