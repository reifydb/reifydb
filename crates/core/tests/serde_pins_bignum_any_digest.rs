// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{Debug, Write as _};

use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{precision::Precision, scale::Scale},
	container::digest_array::digest_array,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	digest::Digest,
	duration::Duration,
	frame::data::FrameColumnData,
	identity::IdentityId,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

struct Pin {
	column_postcard: &'static str,
	column_json: &'static str,
	frame_postcard: &'static str,
	frame_json: &'static str,
}

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

fn postcard_hex<T: Serialize>(value: &T) -> String {
	hex(&to_stdvec(value).unwrap())
}

fn json<T: Serialize>(value: &T) -> String {
	serde_json::to_string(value).unwrap()
}

fn decode_postcard<T: DeserializeOwned>(pinned: &str) -> Result<T, String> {
	from_bytes(&unhex(pinned)).map_err(|err| err.to_string())
}

fn decode_json<T: DeserializeOwned>(pinned: &str) -> Result<T, String> {
	serde_json::from_str(pinned).map_err(|err| err.to_string())
}

fn check_decoded<T: PartialEq + Debug>(label: &str, decoded: Result<T, String>, expected: &T) -> Option<String> {
	match decoded {
		Ok(value) if value == *expected => None,
		Ok(value) => Some(format!("{label}: decoded {value:?}, expected {expected:?}")),
		Err(err) => Some(format!("{label}: decode failed: {err}")),
	}
}

fn check_reserialized<T>(
	label: &str,
	decoded: Result<T, String>,
	pinned: &str,
	write: fn(&T) -> String,
) -> Option<String> {
	match decoded {
		Ok(value) => {
			let again = write(&value);
			(again != pinned).then(|| format!("{label}: re-serialized to {again:?}"))
		}
		Err(err) => Some(format!("{label}: decode failed: {err}")),
	}
}

fn assert_pinned(buffer: ColumnBuffer, pin: &Pin) {
	let frame = FrameColumnData::from(buffer.clone());
	let column_postcard = postcard_hex(&buffer);
	let column_json = json(&buffer);
	let frame_postcard = postcard_hex(&frame);
	let frame_json = json(&frame);
	let mismatches: Vec<String> = [
		(column_postcard != pin.column_postcard).then(|| format!("column_postcard: \"{column_postcard}\"")),
		(column_json != pin.column_json).then(|| format!("column_json: {column_json:?}")),
		(frame_postcard != pin.frame_postcard).then(|| format!("frame_postcard: \"{frame_postcard}\"")),
		(frame_json != pin.frame_json).then(|| format!("frame_json: {frame_json:?}")),
		check_decoded("column postcard", decode_postcard(pin.column_postcard), &buffer),
		check_decoded("column json", decode_json(pin.column_json), &buffer),
		check_decoded("frame postcard", decode_postcard(pin.frame_postcard), &frame),
		check_decoded("frame json", decode_json(pin.frame_json), &frame),
		check_reserialized::<ColumnBuffer>(
			"column postcard",
			decode_postcard(pin.column_postcard),
			pin.column_postcard,
			postcard_hex,
		),
		check_reserialized::<ColumnBuffer>("column json", decode_json(pin.column_json), pin.column_json, json),
		check_reserialized::<FrameColumnData>(
			"frame postcard",
			decode_postcard(pin.frame_postcard),
			pin.frame_postcard,
			postcard_hex,
		),
		check_reserialized::<FrameColumnData>("frame json", decode_json(pin.frame_json), pin.frame_json, json),
	]
	.into_iter()
	.flatten()
	.collect();
	assert!(mismatches.is_empty(), "serde output drifted from the pins:\n{}", mismatches.join("\n"));
}

fn decoded_rows(pin: &Pin) -> Vec<(&'static str, Vec<String>)> {
	let column_rows = |buffer: ColumnBuffer| (0..buffer.len()).map(|i| buffer.as_string(i)).collect();
	let frame_rows = |frame: FrameColumnData| (0..frame.len()).map(|i| frame.as_string(i)).collect();
	vec![
		("column postcard", column_rows(decode_postcard(pin.column_postcard).unwrap())),
		("column json", column_rows(decode_json(pin.column_json).unwrap())),
		("frame postcard", frame_rows(decode_postcard(pin.frame_postcard).unwrap())),
		("frame json", frame_rows(decode_json(pin.frame_json).unwrap())),
	]
}

fn decimals<const N: usize>(texts: [&str; N]) -> [Decimal; N] {
	texts.map(|text| text.parse::<Decimal>().unwrap())
}

fn decimal_value(text: &str) -> Value {
	Value::Decimal(text.parse::<Decimal>().unwrap())
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

#[test]
fn decimal_mixed_input_scales_read_back_at_the_column_scale() {
	// A column holds one scale, so 1.5, 1.50 and 1.500 must store the same bytes and read back as the same text.
	let buffer = ColumnBuffer::decimal(
		Precision::new(12),
		Scale::new(7),
		decimals(["1.5", "1.50", "1.500", "0", "0.00", "-0.0", "0.0000001", "1E+3", "-123.4500", "100"]),
	);
	let pin = Pin {
		column_postcard: "16000c070a8087a70e8087a70e8087a70e000000028090dfc04abfe6a7990980a8d6b907",
		column_json: "{\"Decimal\":{\"Decimal128\":{\"precision\":12,\"scale\":7,\"data\":[15000000,15000000,15000000,0,0,0,1,10000000000,-1234500000,1000000000]}}}",
		frame_postcard: "16000c070a8087a70e8087a70e8087a70e000000028090dfc04abfe6a7990980a8d6b907",
		frame_json: "{\"Decimal\":{\"Decimal128\":{\"precision\":12,\"scale\":7,\"data\":[15000000,15000000,15000000,0,0,0,1,10000000000,-1234500000,1000000000]}}}",
	};
	assert_pinned(buffer, &pin);
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
	]
	.map(String::from)
	.to_vec();
	for (label, rows) in decoded_rows(&pin) {
		assert_eq!(rows, expected, "{label} rows after decode");
	}
}

#[test]
fn decimal_declared_precision_and_scale_are_pinned() {
	// A declared precision and scale must survive storage, otherwise a reloaded column loses its constraint.
	let buffer = ColumnBuffer::decimal(Precision::new(10), Scale::new(2), decimals(["1.25", "-0.5"]));
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "16000a0202fa0163",
			column_json: "{\"Decimal\":{\"Decimal128\":{\"precision\":10,\"scale\":2,\"data\":[125,-50]}}}",
			frame_postcard: "16000a0202fa0163",
			frame_json: "{\"Decimal\":{\"Decimal128\":{\"precision\":10,\"scale\":2,\"data\":[125,-50]}}}",
		},
	);
}

#[test]
fn option_decimal_with_none_row_keeps_scale() {
	// The defined row must keep the column scale and the none row must stay a zero placeholder.
	let buffer = ColumnBuffer::decimal_with_bitvec(
		Precision::MAX,
		Scale::new(2),
		["1.50".parse::<Decimal>().unwrap(), Decimal::default()],
		vec![true, false],
	);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1916014c02029601000000010102",
			column_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"Decimal256\":{\"precision\":76,\"scale\":2,\"data\":[[150,0],[0,0]]}}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
			frame_postcard: "1916014c02029601000000010102",
			frame_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"Decimal256\":{\"precision\":76,\"scale\":2,\"data\":[[150,0],[0,0]]}}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		},
	);
}

#[test]
fn option_any_with_none_row_is_pinned() {
	// The none row must stay an untyped none placeholder with a cleared bit, otherwise optional Any columns drift.
	let buffer = ColumnBuffer::any_with_bitvec([Value::Utf8("a".to_string()), Value::none()], vec![true, false]);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "191702090161001800010102",
			column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Utf8\":\"a\"},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
			frame_postcard: "191702090161001800010102",
			frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Utf8\":\"a\"},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		},
	);
}

#[test]
fn sliced_decimal_keeps_the_column_scale() {
	// A slice must keep the column scale, otherwise 2.500 comes back as 2.5 and still compares equal.
	let buffer = ColumnBuffer::decimal(Precision::MAX, Scale::new(3), decimals(["1.10", "2.500", "-3.0", "4.00"]))
		.slice(1, 3);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "16014c0302c41300c8e8ffffffffffffffffffffffffffffffff0301",
			column_json: "{\"Decimal\":{\"Decimal256\":{\"precision\":76,\"scale\":3,\"data\":[[2500,0],[340282366920938463463374607431768208456,-1]]}}}",
			frame_postcard: "16014c0302c41300c8e8ffffffffffffffffffffffffffffffff0301",
			frame_json: "{\"Decimal\":{\"Decimal256\":{\"precision\":76,\"scale\":3,\"data\":[[2500,0],[340282366920938463463374607431768208456,-1]]}}}",
		},
	);
}

#[test]
fn sliced_any_is_pinned() {
	// A sliced Any must serialize exactly like a fresh column, otherwise the wire format depends on slicing.
	let buffer =
		ColumnBuffer::any([Value::Int4(1), Value::Utf8("b".to_string()), decimal_value("1.50"), Value::none()])
			.slice(1, 3);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "17020901621704312e353000",
			column_json: "{\"Any\":{\"data\":[{\"Utf8\":\"b\"},{\"Decimal\":\"1.50\"}],\"declared_type\":null}}",
			frame_postcard: "17020901621704312e353000",
			frame_json: "{\"Any\":{\"data\":[{\"Utf8\":\"b\"},{\"Decimal\":\"1.50\"}],\"declared_type\":null}}",
		},
	);
}

#[test]
fn sliced_digest_is_pinned() {
	// A sliced Digest must serialize exactly like a fresh column, otherwise the wire format depends on slicing.
	let mut builder = ColumnBuilder::with_capacity(digest_type(), 4);
	for values in [[1.0, 2.0], [3.5, -1.0], [10.0, 20.0], [0.25, 0.5]] {
		builder.push_value(Value::Digest(Box::new(digest_of(&values))));
	}
	let buffer = builder.finish().slice(1, 3);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1a02010d0103904e000000010001017e01010e0103904e0000000002e80101220102904e",
			column_json: "{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,0,1,1,126,1],[1,3,144,78,0,0,0,0,2,232,1,1,34,1]]},\"inner\":\"Float8\",\"accuracy\":10000}}",
			frame_postcard: "1a02010d0103904e000000010001017e01010e0103904e0000000002e80101220102904e",
			frame_json: "{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,0,1,1,126,1],[1,3,144,78,0,0,0,0,2,232,1,1,34,1]]},\"inner\":\"Float8\",\"accuracy\":10000}}",
		},
	);
}

#[test]
fn digest_with_none_slot_is_pinned() {
	// A plain Digest none slot must stay a none entry, never an empty digest, otherwise aggregates count it.
	let digest = digest_of(&[1.0, 2.5, -4.0]);
	let buffer = ColumnBuffer::Digest {
		container: digest_array([Some(&digest), None]),
		inner: ValueType::Float8,
		accuracy: 10_000,
	};
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1a0201100103904e000000018c01010200012e010002904e",
			column_json: "{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],null]},\"inner\":\"Float8\",\"accuracy\":10000}}",
			frame_postcard: "1a0201100103904e000000018c01010200012e010002904e",
			frame_json: "{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],null]},\"inner\":\"Float8\",\"accuracy\":10000}}",
		},
	);
}

#[test]
fn record_typed_any_with_placeholder_is_pinned() {
	// The declared Record type and the empty placeholder row must both survive, otherwise the column retypes.
	let buffer = ColumnBuffer::any_typed(
		[
			Value::Record(vec![
				("a".to_string(), Value::Int4(1)),
				("b".to_string(), Value::Utf8("x".to_string())),
			]),
			Value::Record(vec![]),
		],
		ValueType::Record(vec![("a".to_string(), ValueType::Int4), ("b".to_string(), ValueType::Utf8)]),
	);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "17021c020161060201620901781c00011b02016105016208",
			column_json: "{\"Any\":{\"data\":[{\"Record\":[[\"a\",{\"Int4\":1}],[\"b\",{\"Utf8\":\"x\"}]]},{\"Record\":[]}],\"declared_type\":{\"Record\":[[\"a\",\"Int4\"],[\"b\",\"Utf8\"]]}}}",
			frame_postcard: "17021c020161060201620901781c00011b02016105016208",
			frame_json: "{\"Any\":{\"data\":[{\"Record\":[[\"a\",{\"Int4\":1}],[\"b\",{\"Utf8\":\"x\"}]]},{\"Record\":[]}],\"declared_type\":{\"Record\":[[\"a\",\"Int4\"],[\"b\",\"Utf8\"]]}}}",
		},
	);
}

#[test]
fn tuple_column_is_pinned() {
	// A Tuple column must keep exactly today's untyped declared type, otherwise stored tuple columns misread.
	let mut builder = ColumnBuilder::with_capacity(ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8]), 1);
	builder.push_value(Value::Tuple(vec![Value::Int4(1), Value::Utf8("y".to_string())]));
	assert_pinned(
		builder.finish(),
		&Pin {
			column_postcard: "17011d02060209017900",
			column_json: "{\"Any\":{\"data\":[{\"Tuple\":[{\"Int4\":1},{\"Utf8\":\"y\"}]}],\"declared_type\":null}}",
			frame_postcard: "17011d02060209017900",
			frame_json: "{\"Any\":{\"data\":[{\"Tuple\":[{\"Int4\":1},{\"Utf8\":\"y\"}]}],\"declared_type\":null}}",
		},
	);
}

#[test]
fn any_with_nested_and_typed_none_values_is_pinned() {
	// Typed none values and nested rows must keep their inner types and scales, never collapse to untyped none.
	let buffer = ColumnBuffer::any([
		Value::none_of(ValueType::Int4),
		Value::List(vec![Value::Int4(1), Value::none_of(ValueType::Int4)]),
		decimal_value("1.50"),
		Value::Type(ValueType::Int4),
		Value::Digest(Box::new(digest_of(&[1.0, 2.5, -4.0]))),
	]);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "170500051b02060200051704312e35301a051e100103904e000000018c01010200012e0100",
			column_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Int4\"}},{\"List\":[{\"Int4\":1},{\"None\":{\"inner\":\"Int4\"}}]},{\"Decimal\":\"1.50\"},{\"Type\":\"Int4\"},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]}],\"declared_type\":null}}",
			frame_postcard: "170500051b02060200051704312e35301a051e100103904e000000018c01010200012e0100",
			frame_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Int4\"}},{\"List\":[{\"Int4\":1},{\"None\":{\"inner\":\"Int4\"}}]},{\"Decimal\":\"1.50\"},{\"Type\":\"Int4\"},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]}],\"declared_type\":null}}",
		},
	);
}

#[test]
fn none_typed_list_is_pinned() {
	// A none-typed List column must keep its declared type and empty List placeholders exactly.
	assert_pinned(
		ColumnBuffer::none_typed(ValueType::List(Box::new(ValueType::Int4)), 2),
		&Pin {
			column_postcard: "1917021b001b00011a05010002",
			column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"List\":[]},{\"List\":[]}],\"declared_type\":{\"List\":\"Int4\"}}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
			frame_postcard: "1917021b001b00011a05010002",
			frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"List\":[]},{\"List\":[]}],\"declared_type\":{\"List\":\"Int4\"}}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
		},
	);
}

#[test]
fn none_typed_record_is_pinned() {
	// A none-typed Record column must keep its declared fields and exactly today's placeholder row.
	assert_pinned(
		ColumnBuffer::none_typed(ValueType::Record(vec![("a".to_string(), ValueType::Int4)]), 1),
		&Pin {
			column_postcard: "1917011c00011b01016105010001",
			column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Record\":[]}],\"declared_type\":{\"Record\":[[\"a\",\"Int4\"]]}}},\"bitvec\":{\"bits\":[0],\"len\":1}}}",
			frame_postcard: "1917011c00011b01016105010001",
			frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Record\":[]}],\"declared_type\":{\"Record\":[[\"a\",\"Int4\"]]}}},\"bitvec\":{\"bits\":[0],\"len\":1}}}",
		},
	);
}

#[test]
fn none_typed_decimal_is_pinned() {
	// A none-typed Decimal column must keep zero placeholders and cleared bits, otherwise none rows read as values.
	assert_pinned(
		ColumnBuffer::none_typed(ValueType::DECIMAL, 2),
		&Pin {
			column_postcard: "1916014c0a0200000000010002",
			column_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"Decimal256\":{\"precision\":76,\"scale\":10,\"data\":[[0,0],[0,0]]}}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
			frame_postcard: "1916014c0a0200000000010002",
			frame_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"Decimal256\":{\"precision\":76,\"scale\":10,\"data\":[[0,0],[0,0]]}}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
		},
	);
}

#[test]
fn none_typed_digest_is_pinned() {
	// A none-typed Digest column must keep none slots and its inner type and accuracy exactly.
	assert_pinned(
		ColumnBuffer::none_typed(digest_type(), 2),
		&Pin {
			column_postcard: "191a02000002904e010002",
			column_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[null,null]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
			frame_postcard: "191a02000002904e010002",
			frame_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[null,null]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
		},
	);
}

#[test]
fn any_with_every_value_variant_is_pinned() {
	// Every Value variant must round trip through an Any column without changing a single byte.
	let buffer = ColumnBuffer::any([
		Value::none_of(ValueType::Utf8),
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
		Value::IdentityId(IdentityId(Uuid7(Uuid::from_u128(0x0000_0000_0001_7000_8000_0000_0000_0000)))),
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
	]);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "171f00080101020000c03f030000000000000000048005ffff0306ffffffff0f07ffffffffffffffffff0108ffffffffffffffffffffffffffffffffffff03090668c3a96c6c6f0aff0bffff030cffffffff0f0dffffffffffffffffff010effffffffffffffffffffffffffffffffffff030fdcc30210aab490cedc9bb9e73011ffffbb8ac9d2131202040613100000000000017000800000000000000014100123456789ab4cde8f0123456789abcd151000000000000270008000000000000000160300ff0717092d3132332e3435303018060a1902031a164c0a1b02060209017a1c01016601001d02060201011e100103904e000000018c01010200012e0100",
			column_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Utf8\"}},{\"Boolean\":true},{\"Float4\":1.5},{\"Float8\":0.0},{\"Int1\":-128},{\"Int2\":-32768},{\"Int4\":-2147483648},{\"Int8\":-9223372036854775808},{\"Int16\":-170141183460469231731687303715884105728},{\"Utf8\":\"h\u{e9}llo\"},{\"Uint1\":255},{\"Uint2\":65535},{\"Uint4\":4294967295},{\"Uint8\":18446744073709551615},{\"Uint16\":340282366920938463463374607431768211455},{\"Date\":20718},{\"DateTime\":1758500000123456789},{\"Time\":86399999999999},{\"Duration\":{\"months\":1,\"days\":2,\"nanos\":3}},{\"IdentityId\":\"00000000-0001-7000-8000-000000000000\"},{\"Uuid4\":\"01234567-89ab-4cde-8f01-23456789abcd\"},{\"Uuid7\":\"00000000-0002-7000-8000-000000000000\"},{\"Blob\":[0,255,7]},{\"Decimal\":\"-123.4500\"},{\"Any\":{\"Int4\":5}},{\"DictionaryId\":{\"U4\":3}},{\"Type\":{\"Decimal\":{\"precision\":76,\"scale\":10}}},{\"List\":[{\"Int4\":1},{\"Utf8\":\"z\"}]},{\"Record\":[[\"f\",{\"Boolean\":false}]]},{\"Tuple\":[{\"Int4\":1},{\"Boolean\":true}]},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]}],\"declared_type\":null}}",
			frame_postcard: "171f00080101020000c03f030000000000000000048005ffff0306ffffffff0f07ffffffffffffffffff0108ffffffffffffffffffffffffffffffffffff03090668c3a96c6c6f0aff0bffff030cffffffff0f0dffffffffffffffffff010effffffffffffffffffffffffffffffffffff030fdcc30210aab490cedc9bb9e73011ffffbb8ac9d2131202040613100000000000017000800000000000000014100123456789ab4cde8f0123456789abcd151000000000000270008000000000000000160300ff0717092d3132332e3435303018060a1902031a164c0a1b02060209017a1c01016601001d02060201011e100103904e000000018c01010200012e0100",
			frame_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Utf8\"}},{\"Boolean\":true},{\"Float4\":1.5},{\"Float8\":0.0},{\"Int1\":-128},{\"Int2\":-32768},{\"Int4\":-2147483648},{\"Int8\":-9223372036854775808},{\"Int16\":-170141183460469231731687303715884105728},{\"Utf8\":\"h\u{e9}llo\"},{\"Uint1\":255},{\"Uint2\":65535},{\"Uint4\":4294967295},{\"Uint8\":18446744073709551615},{\"Uint16\":340282366920938463463374607431768211455},{\"Date\":20718},{\"DateTime\":1758500000123456789},{\"Time\":86399999999999},{\"Duration\":{\"months\":1,\"days\":2,\"nanos\":3}},{\"IdentityId\":\"00000000-0001-7000-8000-000000000000\"},{\"Uuid4\":\"01234567-89ab-4cde-8f01-23456789abcd\"},{\"Uuid7\":\"00000000-0002-7000-8000-000000000000\"},{\"Blob\":[0,255,7]},{\"Decimal\":\"-123.4500\"},{\"Any\":{\"Int4\":5}},{\"DictionaryId\":{\"U4\":3}},{\"Type\":{\"Decimal\":{\"precision\":76,\"scale\":10}}},{\"List\":[{\"Int4\":1},{\"Utf8\":\"z\"}]},{\"Record\":[[\"f\",{\"Boolean\":false}]]},{\"Tuple\":[{\"Int4\":1},{\"Boolean\":true}]},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]}],\"declared_type\":null}}",
		},
	);
}
