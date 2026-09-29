// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Write as _, sync::Arc};

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use postcard::{from_bytes, to_stdvec};
use reifydb_column::{
	encoding::{Encoding, canonical::CanonicalEncoding},
	persist::PersistedArray,
};
use reifydb_core::value::column::{
	builder::ColumnBuilder,
	data::{Column, canonical::Canonical},
	factory,
};
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
	identity::IdentityId,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};
use uuid::Uuid;

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

fn assert_pinned(column: (FieldRef, ArrayRef), pinned: &str) {
	let expected = Canonical::from_column(&column).unwrap();
	let written = chunk_hex(&expected);
	let decoded = decode_chunk(pinned, &expected.view().base_type());
	let mismatches: Vec<String> = [
		(written != pinned).then(|| format!("chunk_postcard: \"{written}\"")),
		match &decoded {
			Ok(decoded)
				if decoded.field_type() == expected.field_type()
					&& decoded.buffer() == expected.buffer() =>
			{
				None
			}
			Ok(decoded) => Some(format!("chunk postcard: decoded {decoded:?}, expected {expected:?}")),
			Err(err) => Some(format!("chunk postcard: decode failed: {err}")),
		},
		match &decoded {
			Ok(decoded) => {
				let again = chunk_hex(decoded);
				(again != pinned).then(|| format!("chunk postcard: re-serialized to {again:?}"))
			}
			Err(err) => Some(format!("chunk postcard: decode failed: {err}")),
		},
	]
	.into_iter()
	.flatten()
	.collect();
	assert!(mismatches.is_empty(), "persisted chunk bytes drifted from the pin:\n{}", mismatches.join("\n"));
}

fn decoded_rows(pinned: &str, ty: &ValueType) -> Vec<String> {
	let decoded = decode_chunk(pinned, ty).unwrap();
	let view = decoded.view();
	(0..decoded.len()).map(|i| view.as_string(i)).collect()
}

fn sliced(column: (FieldRef, ArrayRef), start: usize, end: usize) -> (FieldRef, ArrayRef) {
	(column.0, column.1.slice(start, end - start))
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
	let column = factory::decimal(
		"c",
		Precision::new(12),
		Scale::new(7),
		decimals(["1.5", "1.50", "1.500", "0", "0.00", "-0.0", "0.0000001", "1E+3", "-123.4500", "100"]),
	);
	let pinned = "0001160c0700000016000c070a8087a70e8087a70e8087a70e000000028090dfc04abfe6a7990980a8d6b90700";
	assert_pinned(column, pinned);
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
	let ty = ValueType::Decimal {
		precision: Precision::new(12),
		scale: Scale::new(7),
	};
	assert_eq!(decoded_rows(pinned, &ty), expected, "chunk postcard rows after decode");
}

#[test]
fn decimal_declared_precision_and_scale_are_pinned() {
	// A declared precision and scale must survive storage, otherwise a reloaded column loses its constraint.
	let column = factory::decimal("c", Precision::new(10), Scale::new(2), decimals(["1.25", "-0.5"]));
	assert_pinned(column, "0001160a0200000016000a0202fa016300");
}

#[test]
fn option_decimal_with_none_row_keeps_scale() {
	// The defined row must keep the column scale and the none row must stay a zero placeholder.
	let column = factory::decimal_with_bitvec(
		"c",
		Precision::MAX,
		Scale::new(2),
		["1.50".parse::<Decimal>().unwrap(), Decimal::default()],
		vec![true, false],
	);
	assert_pinned(column, "000117164c0200000016014c0202960100000001010102");
}

#[test]
fn option_any_with_none_row_is_pinned() {
	// The none row is a null row with no encoded value, otherwise optional Any columns drift.
	let column = factory::any_optional("c", [Some(Value::Utf8("a".to_string())), None]);
	assert_pinned(column, "000117180000001702010901610001010102");
}

#[test]
fn sliced_decimal_keeps_the_column_scale() {
	// A slice must keep the column scale, otherwise 2.500 comes back as 2.5 and still compares equal.
	let column = factory::decimal("c", Precision::MAX, Scale::new(3), decimals(["1.10", "2.500", "-3.0", "4.00"]));
	assert_pinned(
		sliced(column, 1, 3),
		"0001164c0300000016014c0302c41300c8e8ffffffffffffffffffffffffffffffff030100",
	);
}

#[test]
fn sliced_any_is_pinned() {
	// A sliced Any must serialize exactly like a fresh column, otherwise the wire format depends on slicing.
	let column = factory::any(
		"c",
		[Value::Int4(1), Value::Utf8("b".to_string()), decimal_value("1.50"), Value::Int4(4)],
	);
	assert_pinned(sliced(column, 1, 3), "000118000000170201090162011704312e353000");
}

#[test]
fn sliced_digest_is_pinned() {
	// A sliced Digest must serialize exactly like a fresh column, otherwise the wire format depends on slicing.
	let mut builder = ColumnBuilder::with_capacity(digest_type(), 4);
	for values in [[1.0, 2.0], [3.5, -1.0], [10.0, 20.0], [0.25, 0.5]] {
		builder.push_value(Value::Digest(Box::new(digest_of(&values))));
	}
	assert_pinned(
		sliced(builder.finish("c"), 1, 3),
		"00011d02904e0000001902010d0103904e000000010001017e01010e0103904e0000000002e80101220100",
	);
}

#[test]
fn digest_with_none_slot_is_pinned() {
	// A plain Digest none must be a null row, never an empty digest, otherwise aggregates count it.
	let digest = digest_of(&[1.0, 2.5, -4.0]);
	let column = named(
		"c",
		FieldType::from(ValueType::Option(Box::new(digest_type()))),
		Arc::new(digest_array([Some(&digest), None])),
	);
	assert_pinned(column, "0001171d02904e000000190201100103904e000000018c01010200012e010001010102");
}

#[test]
fn record_typed_any_with_placeholder_is_pinned() {
	// The declared Record type and the empty placeholder row must both survive, otherwise the column retypes.
	let column = factory::any_typed(
		"c",
		[
			Value::Record(vec![
				("a".to_string(), Value::Int4(1)),
				("b".to_string(), Value::Utf8("x".to_string())),
			]),
			Value::Record(vec![]),
		],
		ValueType::Record(vec![("a".to_string(), ValueType::Int4), ("b".to_string(), ValueType::Utf8)]),
	);
	assert_pinned(column, "00011b020161050162080000011b020161050162081702011c02016106020162090178011c0000");
}

#[test]
fn tuple_column_is_pinned() {
	// A Tuple column must keep exactly today's untyped declared type, otherwise stored tuple columns misread.
	let mut builder = ColumnBuilder::with_capacity(ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8]), 1);
	builder.push_value(Value::Tuple(vec![Value::Int4(1), Value::Utf8("y".to_string())]));
	assert_pinned(builder.finish("c"), "0001180000001701011d02060209017900");
}

#[test]
fn any_with_nested_and_typed_none_values_is_pinned() {
	// Typed none values and nested rows must keep their inner types and scales, never collapse to untyped none.
	let column = factory::any(
		"c",
		[
			Value::List(vec![Value::Int4(1), Value::none_of(ValueType::Int4)]),
			decimal_value("1.50"),
			Value::Type(ValueType::Int4),
			Value::Digest(Box::new(digest_of(&[1.0, 2.5, -4.0]))),
		],
	);
	assert_pinned(
		column,
		"0001180000001704011b0206020005011704312e3530011a05011e100103904e000000018c01010200012e0100",
	);
}

#[test]
fn none_typed_list_is_pinned() {
	// A none-typed List column must keep its declared type over null rows, never List placeholders.
	assert_pinned(
		factory::none_typed("c", ValueType::List(Box::new(ValueType::Int4)), 2),
		"0001171a050000011a051702000001010002",
	);
}

#[test]
fn none_typed_record_is_pinned() {
	// A none-typed Record column must keep its declared fields over a null row, never a placeholder row.
	assert_pinned(
		factory::none_typed("c", ValueType::Record(vec![("a".to_string(), ValueType::Int4)]), 1),
		"0001171b010161050000011b0101610517010001010001",
	);
}

#[test]
fn none_typed_decimal_is_pinned() {
	// A none-typed Decimal column must keep zero placeholders and cleared bits, otherwise none rows read as values.
	assert_pinned(factory::none_typed("c", ValueType::DECIMAL, 2), "000117164c0a00000016014c0a020000000001010002");
}

#[test]
fn none_typed_digest_is_pinned() {
	// A none-typed Digest column must keep none slots and its inner type and accuracy exactly.
	assert_pinned(factory::none_typed("c", digest_type(), 2), "0001171d02904e0000001902000001010002");
}

#[test]
fn any_with_every_value_variant_is_pinned() {
	// Every Value variant must round trip through an Any column without changing a single byte.
	let column = factory::any(
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
	);
	assert_pinned(
		column,
		"000118000000171f011b01000801010101020000c03f010300000000000000000104800105ffff030106ffffffff0f0107ffffffffffffffffff010108ffffffffffffffffffffffffffffffffffff0301090668c3a96c6c6f010aff010bffff03010cffffffff0f010dffffffffffffffffff01010effffffffffffffffffffffffffffffffffff03010fdcc3020110aab490cedc9bb9e7300111ffffbb8ac9d2130112020406011310000000000001700080000000000000000114100123456789ab4cde8f0123456789abcd0115100000000000027000800000000000000001160300ff070117092d3132332e343530300118060a01190203011a164c0a011b02060209017a011c0101660100011d0206020101011e100103904e000000018c01010200012e0100",
	);
}
