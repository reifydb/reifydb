// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{Debug, Write as _};

use num_bigint::BigInt;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
	container::digest_array::digest_array,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	digest::Digest,
	duration::Duration,
	frame::data::FrameColumnData,
	identity::IdentityId,
	int::Int,
	time::Time,
	uint::Uint,
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

fn big(text: &str) -> BigInt {
	text.parse::<BigInt>().unwrap()
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

fn with_max_bytes(mut buffer: ColumnBuffer, declared: MaxBytes) -> ColumnBuffer {
	match &mut buffer {
		ColumnBuffer::Int {
			max_bytes,
			..
		}
		| ColumnBuffer::Uint {
			max_bytes,
			..
		} => *max_bytes = declared,
		_ => panic!("max_bytes is only declared on Int and Uint buffers"),
	}
	buffer
}

fn with_precision_and_scale(mut buffer: ColumnBuffer, declared_precision: u8, declared_scale: u8) -> ColumnBuffer {
	let ColumnBuffer::Decimal {
		precision,
		scale,
		..
	} = &mut buffer
	else {
		panic!("decimal factory must build a Decimal buffer");
	};
	*precision = Precision::new(declared_precision);
	*scale = Scale::new(declared_scale);
	buffer
}

#[test]
fn decimal_mixed_scales_keep_every_byte() {
	// Decimal equality ignores scale, so bytes and row text must match exactly or 1.50 can come back as 1.5.
	let buffer = ColumnBuffer::decimal(decimals([
		"1.5",
		"1.50",
		"1.500",
		"0",
		"0.00",
		"-0.0",
		"0.0000001",
		"1E+3",
		"-123.4500",
		"100",
	]));
	let pin = Pin {
		column_postcard: "180a053135452d3106313530452d320731353030452d33033045300430452d320430452d310431452d37033145330b2d31323334353030452d34053130304530ff00",
		column_json: "{\"Decimal\":{\"container\":{\"data\":[\"15E-1\",\"150E-2\",\"1500E-3\",\"0E0\",\"0E-2\",\"0E-1\",\"1E-7\",\"1E3\",\"-1234500E-4\",\"100E0\"]},\"precision\":255,\"scale\":0}}",
		frame_postcard: "180a053135452d3106313530452d320731353030452d33033045300430452d320430452d310431452d37033145330b2d31323334353030452d34053130304530",
		frame_json: "{\"Decimal\":{\"data\":[\"15E-1\",\"150E-2\",\"1500E-3\",\"0E0\",\"0E-2\",\"0E-1\",\"1E-7\",\"1E3\",\"-1234500E-4\",\"100E0\"]}}",
	};
	assert_pinned(buffer, &pin);
	let expected = ["1.5", "1.50", "1.500", "0", "0.00", "0.0", "1E-7", "1000", "-123.4500", "100"]
		.map(String::from)
		.to_vec();
	for (label, rows) in decoded_rows(&pin) {
		assert_eq!(rows, expected, "{label} rows after decode");
	}
}

#[test]
fn decimal_declared_precision_and_scale_are_pinned() {
	// A declared precision and scale must survive storage, otherwise a reloaded column loses its constraint.
	let buffer = with_precision_and_scale(ColumnBuffer::decimal(decimals(["1.25", "-0.5"])), 10, 2);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "180206313235452d32052d35452d310a02",
			column_json: "{\"Decimal\":{\"container\":{\"data\":[\"125E-2\",\"-5E-1\"]},\"precision\":10,\"scale\":2}}",
			frame_postcard: "180206313235452d32052d35452d31",
			frame_json: "{\"Decimal\":{\"data\":[\"125E-2\",\"-5E-1\"]}}",
		},
	);
}

#[test]
fn int_declared_max_bytes_is_pinned() {
	// A declared max_bytes must survive storage, otherwise a reloaded Int column accepts wider values.
	let buffer = with_max_bytes(ColumnBuffer::int([Int::from(-7i64), Int::from(i64::MAX)]), MaxBytes::new(16));
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1602ff01070102ffffffff0fffffffff0710",
			column_json: "{\"Int\":{\"container\":{\"data\":[[-1,[7]],[1,[4294967295,2147483647]]]},\"max_bytes\":16}}",
			frame_postcard: "1602ff01070102ffffffff0fffffffff07",
			frame_json: "{\"Int\":{\"data\":[[-1,[7]],[1,[4294967295,2147483647]]]}}",
		},
	);
}

#[test]
fn uint_declared_max_bytes_is_pinned() {
	// A declared max_bytes must survive storage, otherwise a reloaded Uint column accepts wider values.
	let buffer = with_max_bytes(ColumnBuffer::uint([Uint::from(7u64), Uint::from(u64::MAX)]), MaxBytes::new(8));
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "17020101070102ffffffff0fffffffff0f08",
			column_json: "{\"Uint\":{\"container\":{\"data\":[[1,[7]],[1,[4294967295,4294967295]]]},\"max_bytes\":8}}",
			frame_postcard: "17020101070102ffffffff0fffffffff0f",
			frame_json: "{\"Uint\":{\"data\":[[1,[7]],[1,[4294967295,4294967295]]]}}",
		},
	);
}

#[test]
fn int_sign_and_length_edges_are_pinned() {
	// Each row must keep its exact sign and digit count, otherwise a limb or sign boundary shifts on the wire.
	let two_pow_64 = 1i128 << 64;
	let buffer = ColumnBuffer::int([-two_pow_64, -256, -255, -1, 0, 1, 255, 256, two_pow_64].map(Int::from));
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1609ff03000001ff018002ff01ff01ff010100000101010101ff01010180020103000001ffffffff0f",
			column_json: "{\"Int\":{\"container\":{\"data\":[[-1,[0,0,1]],[-1,[256]],[-1,[255]],[-1,[1]],[0,[]],[1,[1]],[1,[255]],[1,[256]],[1,[0,0,1]]]},\"max_bytes\":4294967295}}",
			frame_postcard: "1609ff03000001ff018002ff01ff01ff010100000101010101ff01010180020103000001",
			frame_json: "{\"Int\":{\"data\":[[-1,[0,0,1]],[-1,[256]],[-1,[255]],[-1,[1]],[0,[]],[1,[1]],[1,[255]],[1,[256]],[1,[0,0,1]]]}}",
		},
	);
}

#[test]
fn uint_length_edges_are_pinned() {
	// Each row must keep its exact digit count, otherwise a limb boundary shifts on the wire.
	let buffer = ColumnBuffer::uint([0u128, 255, 256, 1u128 << 64].map(Uint::from));
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "170400000101ff01010180020103000001ffffffff0f",
			column_json: "{\"Uint\":{\"container\":{\"data\":[[0,[]],[1,[255]],[1,[256]],[1,[0,0,1]]]},\"max_bytes\":4294967295}}",
			frame_postcard: "170400000101ff01010180020103000001",
			frame_json: "{\"Uint\":{\"data\":[[0,[]],[1,[255]],[1,[256]],[1,[0,0,1]]]}}",
		},
	);
}

#[test]
fn option_int_with_none_row_is_pinned() {
	// The none row must stay a zero placeholder with a cleared bit, otherwise optional Int columns drift.
	let buffer = ColumnBuffer::int_with_bitvec(
		[Int(big("-12345678901234567890123")), Int::default()],
		vec![true, false],
	);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1b1602ff03cb89898a07f69cd995049d050000ffffffff0f010102",
			column_json: "{\"Option\":{\"inner\":{\"Int\":{\"container\":{\"data\":[[-1,[1900168395,1119243894,669]],[0,[]]]},\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
			frame_postcard: "1b1602ff03cb89898a07f69cd995049d050000010102",
			frame_json: "{\"Option\":{\"inner\":{\"Int\":{\"data\":[[-1,[1900168395,1119243894,669]],[0,[]]]}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		},
	);
}

#[test]
fn option_uint_with_none_row_is_pinned() {
	// The none row must stay a zero placeholder with a cleared bit, otherwise optional Uint columns drift.
	let buffer = ColumnBuffer::uint_with_bitvec(
		[Uint(big("98765432109876543210987")), Uint::default()],
		vec![true, false],
	);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1b17020103eb8bfff201bfccd6ad01ea290000ffffffff0f010102",
			column_json: "{\"Option\":{\"inner\":{\"Uint\":{\"container\":{\"data\":[[1,[509593067,364226111,5354]],[0,[]]]},\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
			frame_postcard: "1b17020103eb8bfff201bfccd6ad01ea290000010102",
			frame_json: "{\"Option\":{\"inner\":{\"Uint\":{\"data\":[[1,[509593067,364226111,5354]],[0,[]]]}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		},
	);
}

#[test]
fn option_decimal_with_none_row_keeps_scale() {
	// The defined row must keep its trailing zero and the none row must stay a zero placeholder.
	let buffer = ColumnBuffer::decimal_with_bitvec(
		["1.50".parse::<Decimal>().unwrap(), Decimal::default()],
		vec![true, false],
	);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1b180206313530452d3203304530ff00010102",
			column_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"container\":{\"data\":[\"150E-2\",\"0E0\"]},\"precision\":255,\"scale\":0}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
			frame_postcard: "1b180206313530452d3203304530010102",
			frame_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"data\":[\"150E-2\",\"0E0\"]}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
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
			column_postcard: "1b1902090161001a00010102",
			column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Utf8\":\"a\"},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
			frame_postcard: "1b1902090161001a00010102",
			frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Utf8\":\"a\"},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		},
	);
}

#[test]
fn sliced_int_is_pinned() {
	// A sliced Int must serialize exactly like a fresh column, otherwise the wire format depends on slicing.
	let buffer = ColumnBuffer::int([1i128, -(1i128 << 64), 3, 4].map(Int::from)).slice(1, 3);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "1602ff03000001010103ffffffff0f",
			column_json: "{\"Int\":{\"container\":{\"data\":[[-1,[0,0,1]],[1,[3]]]},\"max_bytes\":4294967295}}",
			frame_postcard: "1602ff03000001010103",
			frame_json: "{\"Int\":{\"data\":[[-1,[0,0,1]],[1,[3]]]}}",
		},
	);
}

#[test]
fn sliced_uint_is_pinned() {
	// A sliced Uint must serialize exactly like a fresh column, otherwise the wire format depends on slicing.
	let buffer = ColumnBuffer::uint([1u128, u128::MAX, 3, 4].map(Uint::from)).slice(1, 3);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "17020104ffffffff0fffffffff0fffffffff0fffffffff0f010103ffffffff0f",
			column_json: "{\"Uint\":{\"container\":{\"data\":[[1,[4294967295,4294967295,4294967295,4294967295]],[1,[3]]]},\"max_bytes\":4294967295}}",
			frame_postcard: "17020104ffffffff0fffffffff0fffffffff0fffffffff0f010103",
			frame_json: "{\"Uint\":{\"data\":[[1,[4294967295,4294967295,4294967295,4294967295]],[1,[3]]]}}",
		},
	);
}

#[test]
fn sliced_decimal_keeps_trailing_zeros() {
	// A slice must keep each row's own scale, otherwise 2.500 comes back as 2.5 and still compares equal.
	let buffer = ColumnBuffer::decimal(decimals(["1.10", "2.500", "-3.0", "4.00"])).slice(1, 3);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "18020732353030452d33062d3330452d31ff00",
			column_json: "{\"Decimal\":{\"container\":{\"data\":[\"2500E-3\",\"-30E-1\"]},\"precision\":255,\"scale\":0}}",
			frame_postcard: "18020732353030452d33062d3330452d31",
			frame_json: "{\"Decimal\":{\"data\":[\"2500E-3\",\"-30E-1\"]}}",
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
			column_postcard: "19020901621906313530452d3200",
			column_json: "{\"Any\":{\"data\":[{\"Utf8\":\"b\"},{\"Decimal\":\"150E-2\"}],\"declared_type\":null}}",
			frame_postcard: "19020901621906313530452d3200",
			frame_json: "{\"Any\":{\"data\":[{\"Utf8\":\"b\"},{\"Decimal\":\"150E-2\"}],\"declared_type\":null}}",
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
			column_postcard: "1c02010d0103904e000000010001017e01010e0103904e0000000002e80101220102904e",
			column_json: "{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,0,1,1,126,1],[1,3,144,78,0,0,0,0,2,232,1,1,34,1]]},\"inner\":\"Float8\",\"accuracy\":10000}}",
			frame_postcard: "1c02010d0103904e000000010001017e01010e0103904e0000000002e80101220102904e",
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
			column_postcard: "1c0201100103904e000000018c01010200012e010002904e",
			column_json: "{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],null]},\"inner\":\"Float8\",\"accuracy\":10000}}",
			frame_postcard: "1c0201100103904e000000018c01010200012e010002904e",
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
			column_postcard: "19021e020161060201620901781e00011d02016105016208",
			column_json: "{\"Any\":{\"data\":[{\"Record\":[[\"a\",{\"Int4\":1}],[\"b\",{\"Utf8\":\"x\"}]]},{\"Record\":[]}],\"declared_type\":{\"Record\":[[\"a\",\"Int4\"],[\"b\",\"Utf8\"]]}}}",
			frame_postcard: "19021e020161060201620901781e00011d02016105016208",
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
			column_postcard: "19011f02060209017900",
			column_json: "{\"Any\":{\"data\":[{\"Tuple\":[{\"Int4\":1},{\"Utf8\":\"y\"}]}],\"declared_type\":null}}",
			frame_postcard: "19011f02060209017900",
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
		Value::Record(vec![(
			"k".to_string(),
			Value::Uint(Uint(big("98765432109876543210987654321098765432109876"))),
		)]),
	]);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "190600051d02060200051906313530452d321c0520100103904e000000018c01010200012e011e01016b180105b4f6f6cd05ead59e8a04adb5bcc208d9a6b4a608c5db1100",
			column_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Int4\"}},{\"List\":[{\"Int4\":1},{\"None\":{\"inner\":\"Int4\"}}]},{\"Decimal\":\"150E-2\"},{\"Type\":\"Int4\"},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]},{\"Record\":[[\"k\",{\"Uint\":[1,[1505606452,1095215850,2286885549,2228032345,290245]]}]]}],\"declared_type\":null}}",
			frame_postcard: "190600051d02060200051906313530452d321c0520100103904e000000018c01010200012e011e01016b180105b4f6f6cd05ead59e8a04adb5bcc208d9a6b4a608c5db1100",
			frame_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Int4\"}},{\"List\":[{\"Int4\":1},{\"None\":{\"inner\":\"Int4\"}}]},{\"Decimal\":\"150E-2\"},{\"Type\":\"Int4\"},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]},{\"Record\":[[\"k\",{\"Uint\":[1,[1505606452,1095215850,2286885549,2228032345,290245]]}]]}],\"declared_type\":null}}",
		},
	);
}

#[test]
fn none_typed_list_is_pinned() {
	// A none-typed List column must keep its declared type and empty List placeholders exactly.
	assert_pinned(
		ColumnBuffer::none_typed(ValueType::List(Box::new(ValueType::Int4)), 2),
		&Pin {
			column_postcard: "1b19021d001d00011c05010002",
			column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"List\":[]},{\"List\":[]}],\"declared_type\":{\"List\":\"Int4\"}}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
			frame_postcard: "1b19021d001d00011c05010002",
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
			column_postcard: "1b19011e00011d01016105010001",
			column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Record\":[]}],\"declared_type\":{\"Record\":[[\"a\",\"Int4\"]]}}},\"bitvec\":{\"bits\":[0],\"len\":1}}}",
			frame_postcard: "1b19011e00011d01016105010001",
			frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Record\":[]}],\"declared_type\":{\"Record\":[[\"a\",\"Int4\"]]}}},\"bitvec\":{\"bits\":[0],\"len\":1}}}",
		},
	);
}

#[test]
fn none_typed_decimal_is_pinned() {
	// A none-typed Decimal column must keep zero placeholders and cleared bits, otherwise none rows read as values.
	assert_pinned(
		ColumnBuffer::none_typed(ValueType::Decimal, 2),
		&Pin {
			column_postcard: "1b18020330453003304530ff00010002",
			column_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"container\":{\"data\":[\"0E0\",\"0E0\"]},\"precision\":255,\"scale\":0}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
			frame_postcard: "1b18020330453003304530010002",
			frame_json: "{\"Option\":{\"inner\":{\"Decimal\":{\"data\":[\"0E0\",\"0E0\"]}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
		},
	);
}

#[test]
fn none_typed_digest_is_pinned() {
	// A none-typed Digest column must keep none slots and its inner type and accuracy exactly.
	assert_pinned(
		ColumnBuffer::none_typed(digest_type(), 2),
		&Pin {
			column_postcard: "1b1c02000002904e010002",
			column_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[null,null]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
			frame_postcard: "1b1c02000002904e010002",
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
		Value::Int(Int(big("-1234567890123456789012345678901234567890123"))),
		Value::Uint(Uint(big("98765432109876543210987654321098765432109876"))),
		decimal_value("-123.4500"),
		Value::Any(Box::new(Value::Int4(5))),
		Value::DictionaryId(DictionaryEntryId::U4(3)),
		Value::Type(ValueType::Decimal),
		Value::List(vec![Value::Int4(1), Value::Utf8("z".to_string())]),
		Value::Record(vec![("f".to_string(), Value::Boolean(false))]),
		Value::Tuple(vec![Value::Int4(1), Value::Boolean(true)]),
		Value::Digest(Box::new(digest_of(&[1.0, 2.5, -4.0]))),
	]);
	assert_pinned(
		buffer,
		&Pin {
			column_postcard: "192100080101020000c03f030000000000000000048005ffff0306ffffffff0f07ffffffffffffffffff0108ffffffffffffffffffffffffffffffffffff03090668c3a96c6c6f0aff0bffff030cffffffff0f0dffffffffffffffffff010effffffffffffffffffffffffffffffffffff030fdcc30210959a88a7eecddcb31811ffffbb8ac9d2131202040613100000000000017000800000000000000014100123456789ab4cde8f0123456789abcd151000000000000270008000000000000000160300ff0717ff05cb8989b20a95cad5fe0be292c0d905f9979b8d01ac1c180105b4f6f6cd05ead59e8a04adb5bcc208d9a6b4a608c5db11190b2d31323334353030452d341a060a1b02031c181d02060209017a1e01016601001f020602010120100103904e000000018c01010200012e0100",
			column_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Utf8\"}},{\"Boolean\":true},{\"Float4\":1.5},{\"Float8\":0.0},{\"Int1\":-128},{\"Int2\":-32768},{\"Int4\":-2147483648},{\"Int8\":-9223372036854775808},{\"Int16\":-170141183460469231731687303715884105728},{\"Utf8\":\"h\u{e9}llo\"},{\"Uint1\":255},{\"Uint2\":65535},{\"Uint4\":4294967295},{\"Uint8\":18446744073709551615},{\"Uint16\":340282366920938463463374607431768211455},{\"Date\":20718},{\"DateTime\":1758500000123456789},{\"Time\":86399999999999},{\"Duration\":{\"months\":1,\"days\":2,\"nanos\":3}},{\"IdentityId\":\"00000000-0001-7000-8000-000000000000\"},{\"Uuid4\":\"01234567-89ab-4cde-8f01-23456789abcd\"},{\"Uuid7\":\"00000000-0002-7000-8000-000000000000\"},{\"Blob\":[0,255,7]},{\"Int\":[-1,[2789360843,3218433301,1529874786,296143865,3628]]},{\"Uint\":[1,[1505606452,1095215850,2286885549,2228032345,290245]]},{\"Decimal\":\"-1234500E-4\"},{\"Any\":{\"Int4\":5}},{\"DictionaryId\":{\"U4\":3}},{\"Type\":\"Decimal\"},{\"List\":[{\"Int4\":1},{\"Utf8\":\"z\"}]},{\"Record\":[[\"f\",{\"Boolean\":false}]]},{\"Tuple\":[{\"Int4\":1},{\"Boolean\":true}]},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]}],\"declared_type\":null}}",
			frame_postcard: "192100080101020000c03f030000000000000000048005ffff0306ffffffff0f07ffffffffffffffffff0108ffffffffffffffffffffffffffffffffffff03090668c3a96c6c6f0aff0bffff030cffffffff0f0dffffffffffffffffff010effffffffffffffffffffffffffffffffffff030fdcc30210959a88a7eecddcb31811ffffbb8ac9d2131202040613100000000000017000800000000000000014100123456789ab4cde8f0123456789abcd151000000000000270008000000000000000160300ff0717ff05cb8989b20a95cad5fe0be292c0d905f9979b8d01ac1c180105b4f6f6cd05ead59e8a04adb5bcc208d9a6b4a608c5db11190b2d31323334353030452d341a060a1b02031c181d02060209017a1e01016601001f020602010120100103904e000000018c01010200012e0100",
			frame_json: "{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Utf8\"}},{\"Boolean\":true},{\"Float4\":1.5},{\"Float8\":0.0},{\"Int1\":-128},{\"Int2\":-32768},{\"Int4\":-2147483648},{\"Int8\":-9223372036854775808},{\"Int16\":-170141183460469231731687303715884105728},{\"Utf8\":\"h\u{e9}llo\"},{\"Uint1\":255},{\"Uint2\":65535},{\"Uint4\":4294967295},{\"Uint8\":18446744073709551615},{\"Uint16\":340282366920938463463374607431768211455},{\"Date\":20718},{\"DateTime\":1758500000123456789},{\"Time\":86399999999999},{\"Duration\":{\"months\":1,\"days\":2,\"nanos\":3}},{\"IdentityId\":\"00000000-0001-7000-8000-000000000000\"},{\"Uuid4\":\"01234567-89ab-4cde-8f01-23456789abcd\"},{\"Uuid7\":\"00000000-0002-7000-8000-000000000000\"},{\"Blob\":[0,255,7]},{\"Int\":[-1,[2789360843,3218433301,1529874786,296143865,3628]]},{\"Uint\":[1,[1505606452,1095215850,2286885549,2228032345,290245]]},{\"Decimal\":\"-1234500E-4\"},{\"Any\":{\"Int4\":5}},{\"DictionaryId\":{\"U4\":3}},{\"Type\":\"Decimal\"},{\"List\":[{\"Int4\":1},{\"Utf8\":\"z\"}]},{\"Record\":[[\"f\",{\"Boolean\":false}]]},{\"Tuple\":[{\"Int4\":1},{\"Boolean\":true}]},{\"Digest\":[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1]}],\"declared_type\":null}}",
		},
	);
}
