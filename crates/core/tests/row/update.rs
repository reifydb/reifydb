// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Integration tests for updating EncodedRow fields in-place.
//! Verifies that replace_dynamic_data properly splices bytes, adjusts references,
//! and leaves no orphan data.

use std::{f64::consts::E, str::FromStr};

use num_bigint::BigInt;
use reifydb_core::encoded::schema::Schema;
use reifydb_type::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};

#[test]
fn test_utf8_update_same_size() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();
	schema.set_utf8(&mut row, 0, "abcde");
	let size = row.len();

	schema.set_utf8(&mut row, 0, "12345");
	assert_eq!(schema.get_utf8(&row, 0), "12345");
	assert_eq!(row.len(), size);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "12345");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_utf8_update_larger() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();
	schema.set_utf8(&mut row, 0, "hi");
	schema.set_utf8(&mut row, 0, "hello world");
	assert_eq!(schema.get_utf8(&row, 0), "hello world");
	assert_eq!(row.len(), schema.total_static_size() + 11);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "hello world");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_utf8_update_smaller() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();
	schema.set_utf8(&mut row, 0, "hello world");
	schema.set_utf8(&mut row, 0, "hi");
	assert_eq!(schema.get_utf8(&row, 0), "hi");
	assert_eq!(row.len(), schema.total_static_size() + 2);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "hi");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_utf8_update_to_empty() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();
	schema.set_utf8(&mut row, 0, "hello");
	schema.set_utf8(&mut row, 0, "");
	assert_eq!(schema.get_utf8(&row, 0), "");
	assert_eq!(row.len(), schema.total_static_size());

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_utf8_update_from_empty() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();
	schema.set_utf8(&mut row, 0, "");
	assert_eq!(row.len(), schema.total_static_size());

	schema.set_utf8(&mut row, 0, "now has content");
	assert_eq!(schema.get_utf8(&row, 0), "now has content");
	assert_eq!(row.len(), schema.total_static_size() + 15);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "now has content");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_utf8_alternating_sizes() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();

	let values = ["a", "hello world this is long", "xy", "medium string", "z"];
	for &v in &values {
		schema.set_utf8(&mut row, 0, v);
		assert_eq!(schema.get_utf8(&row, 0), v);
		assert_eq!(row.len(), schema.total_static_size() + v.len());

		let mut fresh = schema.allocate();
		schema.set_utf8(&mut fresh, 0, v);
		assert_eq!(row.len(), fresh.len());
	}
}

#[test]
fn test_blob_update_same_size() {
	let schema = Schema::testing(&[Type::Blob]);
	let mut row = schema.allocate();
	schema.set_blob(&mut row, 0, &Blob::from_slice(&[1, 2, 3]));
	let size = row.len();

	schema.set_blob(&mut row, 0, &Blob::from_slice(&[4, 5, 6]));
	assert_eq!(schema.get_blob(&row, 0), Blob::from_slice(&[4, 5, 6]));
	assert_eq!(row.len(), size);

	let mut fresh = schema.allocate();
	schema.set_blob(&mut fresh, 0, &Blob::from_slice(&[4, 5, 6]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_blob_update_larger() {
	let schema = Schema::testing(&[Type::Blob]);
	let mut row = schema.allocate();
	schema.set_blob(&mut row, 0, &Blob::from_slice(&[1]));
	schema.set_blob(&mut row, 0, &Blob::from_slice(&[1, 2, 3, 4, 5]));
	assert_eq!(schema.get_blob(&row, 0), Blob::from_slice(&[1, 2, 3, 4, 5]));
	assert_eq!(row.len(), schema.total_static_size() + 5);

	let mut fresh = schema.allocate();
	schema.set_blob(&mut fresh, 0, &Blob::from_slice(&[1, 2, 3, 4, 5]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_blob_update_smaller() {
	let schema = Schema::testing(&[Type::Blob]);
	let mut row = schema.allocate();
	schema.set_blob(&mut row, 0, &Blob::from_slice(&[1, 2, 3, 4, 5]));
	schema.set_blob(&mut row, 0, &Blob::from_slice(&[9]));
	assert_eq!(schema.get_blob(&row, 0), Blob::from_slice(&[9]));
	assert_eq!(row.len(), schema.total_static_size() + 1);

	let mut fresh = schema.allocate();
	schema.set_blob(&mut fresh, 0, &Blob::from_slice(&[9]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_blob_update_to_empty() {
	let schema = Schema::testing(&[Type::Blob]);
	let mut row = schema.allocate();
	schema.set_blob(&mut row, 0, &Blob::from_slice(&[1, 2, 3]));
	schema.set_blob(&mut row, 0, &Blob::from_slice(&[]));
	assert_eq!(schema.get_blob(&row, 0), Blob::from_slice(&[]));
	assert_eq!(row.len(), schema.total_static_size());

	let mut fresh = schema.allocate();
	schema.set_blob(&mut fresh, 0, &Blob::from_slice(&[]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_blob_alternating_sizes() {
	let schema = Schema::testing(&[Type::Blob]);
	let mut row = schema.allocate();

	let values: Vec<Vec<u8>> = vec![vec![1], vec![0; 100], vec![2, 3], vec![0; 50], vec![4]];
	for v in &values {
		let blob = Blob::from_slice(v);
		schema.set_blob(&mut row, 0, &blob);
		assert_eq!(schema.get_blob(&row, 0), blob);
		assert_eq!(row.len(), schema.total_static_size() + v.len());

		let mut fresh = schema.allocate();
		schema.set_blob(&mut fresh, 0, &Blob::from_slice(v));
		assert_eq!(row.len(), fresh.len());
	}
}

#[test]
fn test_update_first_of_three_dynamic_fields() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob, Type::Utf8]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "aaa");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3, 4, 5]));
	schema.set_utf8(&mut row, 2, "ccc");

	// Update first with larger data — should shift blob and third utf8
	schema.set_utf8(&mut row, 0, "aaaaaaaaaa"); // 3 → 10

	assert_eq!(schema.get_utf8(&row, 0), "aaaaaaaaaa");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[1, 2, 3, 4, 5]));
	assert_eq!(schema.get_utf8(&row, 2), "ccc");
	assert_eq!(row.len(), schema.total_static_size() + 10 + 5 + 3);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "aaaaaaaaaa");
	schema.set_blob(&mut fresh, 1, &Blob::from_slice(&[1, 2, 3, 4, 5]));
	schema.set_utf8(&mut fresh, 2, "ccc");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_update_middle_of_four_dynamic_fields() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob, Type::Utf8, Type::Blob]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "first");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[10, 20, 30]));
	schema.set_utf8(&mut row, 2, "third");
	schema.set_blob(&mut row, 3, &Blob::from_slice(&[40, 50]));

	// Update middle field (index 1) with smaller data
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[99]));

	assert_eq!(schema.get_utf8(&row, 0), "first");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[99]));
	assert_eq!(schema.get_utf8(&row, 2), "third");
	assert_eq!(schema.get_blob(&row, 3), Blob::from_slice(&[40, 50]));
	assert_eq!(row.len(), schema.total_static_size() + 5 + 1 + 5 + 2);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "first");
	schema.set_blob(&mut fresh, 1, &Blob::from_slice(&[99]));
	schema.set_utf8(&mut fresh, 2, "third");
	schema.set_blob(&mut fresh, 3, &Blob::from_slice(&[40, 50]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_update_mixed_dynamic_types_each_in_turn() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob, Type::Decimal, Type::Any]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "text");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3]));
	schema.set_decimal(&mut row, 2, &Decimal::from_str("1.5").unwrap());
	schema.set_any(&mut row, 3, &Value::Int4(42));

	// Update utf8
	schema.set_utf8(&mut row, 0, "longer text value");
	assert_eq!(schema.get_utf8(&row, 0), "longer text value");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[1, 2, 3]));
	assert_eq!(schema.get_decimal(&row, 2).to_string(), "1.5");
	assert_eq!(schema.get_any(&row, 3), Value::Int4(42));

	// Update blob
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[10]));
	assert_eq!(schema.get_utf8(&row, 0), "longer text value");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[10]));
	assert_eq!(schema.get_decimal(&row, 2).to_string(), "1.5");
	assert_eq!(schema.get_any(&row, 3), Value::Int4(42));

	// Update decimal
	schema.set_decimal(&mut row, 2, &Decimal::from_str("99999.12345").unwrap());
	assert_eq!(schema.get_utf8(&row, 0), "longer text value");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[10]));
	assert_eq!(schema.get_decimal(&row, 2).to_string(), "99999.12345");
	assert_eq!(schema.get_any(&row, 3), Value::Int4(42));

	// Update any
	schema.set_any(&mut row, 3, &Value::Utf8("now a string".to_string()));
	assert_eq!(schema.get_utf8(&row, 0), "longer text value");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[10]));
	assert_eq!(schema.get_decimal(&row, 2).to_string(), "99999.12345");
	assert_eq!(schema.get_any(&row, 3), Value::Utf8("now a string".to_string()));

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "longer text value");
	schema.set_blob(&mut fresh, 1, &Blob::from_slice(&[10]));
	schema.set_decimal(&mut fresh, 2, &Decimal::from_str("99999.12345").unwrap());
	schema.set_any(&mut fresh, 3, &Value::Utf8("now a string".to_string()));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_update_fields_forward_order() {
	let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "aaa");
	schema.set_utf8(&mut row, 1, "bbb");
	schema.set_utf8(&mut row, 2, "ccc");

	schema.set_utf8(&mut row, 0, "AAAAAAA");
	schema.set_utf8(&mut row, 1, "BB");
	schema.set_utf8(&mut row, 2, "CCCCC");

	assert_eq!(schema.get_utf8(&row, 0), "AAAAAAA");
	assert_eq!(schema.get_utf8(&row, 1), "BB");
	assert_eq!(schema.get_utf8(&row, 2), "CCCCC");
	assert_eq!(row.len(), schema.total_static_size() + 7 + 2 + 5);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "AAAAAAA");
	schema.set_utf8(&mut fresh, 1, "BB");
	schema.set_utf8(&mut fresh, 2, "CCCCC");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_update_fields_reverse_order() {
	let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "aaa");
	schema.set_utf8(&mut row, 1, "bbb");
	schema.set_utf8(&mut row, 2, "ccc");

	schema.set_utf8(&mut row, 2, "CCCCC");
	schema.set_utf8(&mut row, 1, "BB");
	schema.set_utf8(&mut row, 0, "AAAAAAA");

	assert_eq!(schema.get_utf8(&row, 0), "AAAAAAA");
	assert_eq!(schema.get_utf8(&row, 1), "BB");
	assert_eq!(schema.get_utf8(&row, 2), "CCCCC");
	assert_eq!(row.len(), schema.total_static_size() + 7 + 2 + 5);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "AAAAAAA");
	schema.set_utf8(&mut fresh, 1, "BB");
	schema.set_utf8(&mut fresh, 2, "CCCCC");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_update_fields_interleaved_order() {
	let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "aaa");
	schema.set_utf8(&mut row, 1, "bbb");
	schema.set_utf8(&mut row, 2, "ccc");

	// Update in order: 1, 0, 2
	schema.set_utf8(&mut row, 1, "BB");
	schema.set_utf8(&mut row, 0, "AAAAAAA");
	schema.set_utf8(&mut row, 2, "CCCCC");

	assert_eq!(schema.get_utf8(&row, 0), "AAAAAAA");
	assert_eq!(schema.get_utf8(&row, 1), "BB");
	assert_eq!(schema.get_utf8(&row, 2), "CCCCC");
	assert_eq!(row.len(), schema.total_static_size() + 7 + 2 + 5);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "AAAAAAA");
	schema.set_utf8(&mut fresh, 1, "BB");
	schema.set_utf8(&mut fresh, 2, "CCCCC");
	assert_eq!(row.len(), fresh.len());
}

fn huge_int() -> Int {
	Int::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap())
}

fn huge_int2() -> Int {
	Int::from(BigInt::parse_bytes(b"111111111111111111111111111111111111111111111111", 10).unwrap())
}

fn huge_uint() -> Uint {
	Uint::from(BigInt::parse_bytes(b"999999999999999999999999999999999999999999999999", 10).unwrap())
}

#[test]
fn test_int_multiple_transitions() {
	let schema = Schema::testing(&[Type::Int]);
	let mut row = schema.allocate();

	// inline
	schema.set_int(&mut row, 0, &Int::from(1));
	assert_eq!(schema.get_int(&row, 0), Int::from(1));
	assert_eq!(row.len(), schema.total_static_size());

	// inline → dynamic
	schema.set_int(&mut row, 0, &huge_int());
	assert_eq!(schema.get_int(&row, 0), huge_int());
	assert!(row.len() > schema.total_static_size());

	// dynamic → inline
	schema.set_int(&mut row, 0, &Int::from(42));
	assert_eq!(schema.get_int(&row, 0), Int::from(42));
	assert_eq!(row.len(), schema.total_static_size());

	// inline → dynamic again
	schema.set_int(&mut row, 0, &huge_int2());
	assert_eq!(schema.get_int(&row, 0), huge_int2());
	assert!(row.len() > schema.total_static_size());

	let mut fresh = schema.allocate();
	schema.set_int(&mut fresh, 0, &huge_int2());
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_uint_multiple_transitions() {
	let schema = Schema::testing(&[Type::Uint]);
	let mut row = schema.allocate();

	// inline
	schema.set_uint(&mut row, 0, &Uint::from(1u64));
	assert_eq!(schema.get_uint(&row, 0), Uint::from(1u64));
	assert_eq!(row.len(), schema.total_static_size());

	// inline → dynamic
	schema.set_uint(&mut row, 0, &huge_uint());
	assert_eq!(schema.get_uint(&row, 0), huge_uint());
	assert!(row.len() > schema.total_static_size());

	// dynamic → inline
	schema.set_uint(&mut row, 0, &Uint::from(99u64));
	assert_eq!(schema.get_uint(&row, 0), Uint::from(99u64));
	assert_eq!(row.len(), schema.total_static_size());

	let mut fresh = schema.allocate();
	schema.set_uint(&mut fresh, 0, &Uint::from(99u64));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_int_transition_with_other_dynamic_fields() {
	let schema = Schema::testing(&[Type::Utf8, Type::Int, Type::Blob]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "hello");
	schema.set_int(&mut row, 1, &huge_int());
	schema.set_blob(&mut row, 2, &Blob::from_slice(&[1, 2, 3]));

	// Verify initial state
	assert_eq!(schema.get_utf8(&row, 0), "hello");
	assert_eq!(schema.get_int(&row, 1), huge_int());
	assert_eq!(schema.get_blob(&row, 2), Blob::from_slice(&[1, 2, 3]));

	// dynamic → inline: removes dynamic int data, adjusts blob offset
	schema.set_int(&mut row, 1, &Int::from(7));
	assert_eq!(schema.get_utf8(&row, 0), "hello");
	assert_eq!(schema.get_int(&row, 1), Int::from(7));
	assert_eq!(schema.get_blob(&row, 2), Blob::from_slice(&[1, 2, 3]));

	// inline → dynamic again
	schema.set_int(&mut row, 1, &huge_int());
	assert_eq!(schema.get_utf8(&row, 0), "hello");
	assert_eq!(schema.get_int(&row, 1), huge_int());
	assert_eq!(schema.get_blob(&row, 2), Blob::from_slice(&[1, 2, 3]));

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "hello");
	schema.set_int(&mut fresh, 1, &huge_int());
	schema.set_blob(&mut fresh, 2, &Blob::from_slice(&[1, 2, 3]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_int_dynamic_to_dynamic() {
	let schema = Schema::testing(&[Type::Int]);
	let mut row = schema.allocate();

	schema.set_int(&mut row, 0, &huge_int());
	schema.set_int(&mut row, 0, &huge_int2());
	assert_eq!(schema.get_int(&row, 0), huge_int2());
	// Both huge values have similar serialized sizes
	let size2 = row.len();
	assert!(size2 > schema.total_static_size());
	let mut fresh = schema.allocate();
	schema.set_int(&mut fresh, 0, &huge_int2());
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_decimal_update_different_sizes() {
	let schema = Schema::testing(&[Type::Decimal]);
	let mut row = schema.allocate();

	// Small decimal
	schema.set_decimal(&mut row, 0, &Decimal::from_str("1.5").unwrap());
	assert_eq!(schema.get_decimal(&row, 0).to_string(), "1.5");

	// Much larger decimal (bigger mantissa)
	schema.set_decimal(&mut row, 0, &Decimal::from_str("99999999999999999999999999999.123456789").unwrap());
	assert_eq!(schema.get_decimal(&row, 0).to_string(), "99999999999999999999999999999.123456789");

	// Back to small
	schema.set_decimal(&mut row, 0, &Decimal::from_str("0.01").unwrap());
	assert_eq!(schema.get_decimal(&row, 0).to_string(), "0.01");

	let mut fresh = schema.allocate();
	schema.set_decimal(&mut fresh, 0, &Decimal::from_str("0.01").unwrap());
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_decimal_multiple_sequential_updates() {
	let schema = Schema::testing(&[Type::Decimal]);
	let mut row = schema.allocate();

	let values = ["1.0", "2.5", "100.001", "0.000001", "9999.99", "3.14159"];
	for v in &values {
		let d = Decimal::from_str(v).unwrap();
		schema.set_decimal(&mut row, 0, &d);
		assert_eq!(schema.get_decimal(&row, 0).to_string(), *v);

		let mut fresh = schema.allocate();
		schema.set_decimal(&mut fresh, 0, &Decimal::from_str(v).unwrap());
		assert_eq!(row.len(), fresh.len());
	}
}

#[test]
fn test_decimal_update_with_other_dynamic_fields() {
	let schema = Schema::testing(&[Type::Utf8, Type::Decimal, Type::Blob]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "price");
	schema.set_decimal(&mut row, 1, &Decimal::from_str("19.99").unwrap());
	schema.set_blob(&mut row, 2, &Blob::from_slice(&[0xFF; 10]));

	// Update decimal (changes size of dynamic data)
	schema.set_decimal(&mut row, 1, &Decimal::from_str("123456789.987654321").unwrap());

	assert_eq!(schema.get_utf8(&row, 0), "price");
	assert_eq!(schema.get_decimal(&row, 1).to_string(), "123456789.987654321");
	assert_eq!(schema.get_blob(&row, 2), Blob::from_slice(&[0xFF; 10]));

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "price");
	schema.set_decimal(&mut fresh, 1, &Decimal::from_str("123456789.987654321").unwrap());
	schema.set_blob(&mut fresh, 2, &Blob::from_slice(&[0xFF; 10]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_any_cycle_all_types() {
	let schema = Schema::testing(&[Type::Any]);
	let mut row = schema.allocate();

	let values: Vec<Value> = vec![
		Value::Boolean(true),
		Value::Int1(-42),
		Value::Int2(-1000),
		Value::Int4(-100000),
		Value::Int8(-9999999999i64),
		Value::Int16(i128::MAX),
		Value::Uint1(255),
		Value::Uint2(65535),
		Value::Uint4(u32::MAX),
		Value::Uint8(u64::MAX),
		Value::Uint16(u128::MAX),
		Value::Float4(OrderedF32::try_from(3.14f32).unwrap()),
		Value::Float8(OrderedF64::try_from(E).unwrap()),
		Value::Date(Date::new(2025, 12, 31).unwrap()),
		Value::DateTime(DateTime::new(2025, 7, 15, 14, 30, 45, 0).unwrap()),
		Value::Time(Time::new(23, 59, 59, 999999999).unwrap()),
		Value::Duration(Duration::new(12, 30, 1_000_000_000)),
		Value::Uuid4(Uuid4::generate()),
		Value::Uuid7(Uuid7::generate()),
		Value::IdentityId(IdentityId::generate()),
		Value::Utf8("hello world".to_string()),
		Value::Blob(Blob::from_slice(&[0xDE, 0xAD, 0xBE, 0xEF])),
	];

	// Set each value, overwriting the previous, and verify
	for val in &values {
		schema.set_any(&mut row, 0, val);
		assert_eq!(schema.get_any(&row, 0), *val);

		let mut fresh = schema.allocate();
		schema.set_any(&mut fresh, 0, val);
		assert_eq!(row.len(), fresh.len());
	}
}

#[test]
fn test_any_small_to_large_encoding() {
	let schema = Schema::testing(&[Type::Any]);
	let mut row = schema.allocate();

	// Boolean = 2 bytes encoded
	schema.set_any(&mut row, 0, &Value::Boolean(true));
	assert_eq!(schema.get_any(&row, 0), Value::Boolean(true));

	// Utf8 with long string = 5 + len bytes encoded
	let long_str = Value::Utf8("x".repeat(1000));
	schema.set_any(&mut row, 0, &long_str);
	assert_eq!(schema.get_any(&row, 0), long_str);

	let mut fresh = schema.allocate();
	schema.set_any(&mut fresh, 0, &long_str);
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_any_large_to_small_encoding() {
	let schema = Schema::testing(&[Type::Any]);
	let mut row = schema.allocate();

	let long_str = Value::Utf8("x".repeat(1000));
	schema.set_any(&mut row, 0, &long_str);
	assert_eq!(schema.get_any(&row, 0), long_str);

	schema.set_any(&mut row, 0, &Value::Boolean(false));
	assert_eq!(schema.get_any(&row, 0), Value::Boolean(false));
	// Dynamic section should have shrunk
	assert_eq!(row.len(), schema.total_static_size() + 2);

	let mut fresh = schema.allocate();
	schema.set_any(&mut fresh, 0, &Value::Boolean(false));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_any_same_size_encoding() {
	let schema = Schema::testing(&[Type::Any]);
	let mut row = schema.allocate();

	// Int4 = 5 bytes (1 type + 4 data)
	schema.set_any(&mut row, 0, &Value::Int4(42));
	let size = row.len();

	// Uint4 = 5 bytes (1 type + 4 data)
	schema.set_any(&mut row, 0, &Value::Uint4(42));
	assert_eq!(schema.get_any(&row, 0), Value::Uint4(42));
	assert_eq!(row.len(), size);

	// Float4 = 5 bytes (1 type + 4 data)
	schema.set_any(&mut row, 0, &Value::Float4(OrderedF32::try_from(1.5f32).unwrap()));
	assert_eq!(schema.get_any(&row, 0), Value::Float4(OrderedF32::try_from(1.5f32).unwrap()));
	assert_eq!(row.len(), size);

	let mut fresh = schema.allocate();
	schema.set_any(&mut fresh, 0, &Value::Float4(OrderedF32::try_from(1.5f32).unwrap()));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_any_update_with_other_dynamic_fields() {
	let schema = Schema::testing(&[Type::Utf8, Type::Any, Type::Blob]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "prefix");
	schema.set_any(&mut row, 1, &Value::Int4(1));
	schema.set_blob(&mut row, 2, &Blob::from_slice(&[1, 2, 3]));

	// Update any with much larger encoding
	schema.set_any(&mut row, 1, &Value::Utf8("a much longer value stored in any".to_string()));

	assert_eq!(schema.get_utf8(&row, 0), "prefix");
	assert_eq!(schema.get_any(&row, 1), Value::Utf8("a much longer value stored in any".to_string()));
	assert_eq!(schema.get_blob(&row, 2), Blob::from_slice(&[1, 2, 3]));

	// Update any with smaller encoding again
	schema.set_any(&mut row, 1, &Value::Boolean(true));
	assert_eq!(schema.get_utf8(&row, 0), "prefix");
	assert_eq!(schema.get_any(&row, 1), Value::Boolean(true));
	assert_eq!(schema.get_blob(&row, 2), Blob::from_slice(&[1, 2, 3]));

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "prefix");
	schema.set_any(&mut fresh, 1, &Value::Boolean(true));
	schema.set_blob(&mut fresh, 2, &Blob::from_slice(&[1, 2, 3]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_update_dynamic_preserves_static() {
	let schema = Schema::testing(&[Type::Boolean, Type::Int4, Type::Utf8, Type::Float8, Type::Blob]);
	let mut row = schema.allocate();

	schema.set_bool(&mut row, 0, true);
	schema.set_i32(&mut row, 1, 42);
	schema.set_utf8(&mut row, 2, "hello");
	schema.set_f64(&mut row, 3, 3.14);
	schema.set_blob(&mut row, 4, &Blob::from_slice(&[1, 2, 3]));

	// Update dynamic fields multiple times
	for i in 0..10 {
		schema.set_utf8(&mut row, 2, &format!("iteration_{}", i));
		schema.set_blob(&mut row, 4, &Blob::from_slice(&vec![i as u8; i + 1]));

		// Static fields must be unchanged
		assert_eq!(schema.get_bool(&row, 0), true);
		assert_eq!(schema.get_i32(&row, 1), 42);
		assert!((schema.get_f64(&row, 3) - 3.14).abs() < f64::EPSILON);
	}

	assert_eq!(schema.get_utf8(&row, 2), "iteration_9");
	assert_eq!(schema.get_blob(&row, 4), Blob::from_slice(&[9; 10]));

	let mut fresh = schema.allocate();
	schema.set_bool(&mut fresh, 0, true);
	schema.set_i32(&mut fresh, 1, 42);
	schema.set_utf8(&mut fresh, 2, "iteration_9");
	schema.set_f64(&mut fresh, 3, 3.14);
	schema.set_blob(&mut fresh, 4, &Blob::from_slice(&[9; 10]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_all_dynamic_types_in_one_row() {
	let schema = Schema::testing(&[
		Type::Utf8,
		Type::Blob,
		Type::Decimal,
		Type::Int,
		Type::Uint,
		Type::Any,
		Type::Boolean, // static
		Type::Int4,    // static
	]);
	let mut row = schema.allocate();

	// Initial set
	schema.set_utf8(&mut row, 0, "text");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3]));
	schema.set_decimal(&mut row, 2, &Decimal::from_str("1.5").unwrap());
	schema.set_int(&mut row, 3, &Int::from(42));
	schema.set_uint(&mut row, 4, &Uint::from(100u64));
	schema.set_any(&mut row, 5, &Value::Int4(7));
	schema.set_bool(&mut row, 6, true);
	schema.set_i32(&mut row, 7, 999);

	// Update all dynamic fields
	schema.set_utf8(&mut row, 0, "updated text that is longer");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[10, 20]));
	schema.set_decimal(&mut row, 2, &Decimal::from_str("99999.99").unwrap());
	schema.set_int(&mut row, 3, &huge_int());
	schema.set_uint(&mut row, 4, &huge_uint());
	schema.set_any(&mut row, 5, &Value::Utf8("now a string".to_string()));

	// Verify all
	assert_eq!(schema.get_utf8(&row, 0), "updated text that is longer");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[10, 20]));
	assert_eq!(schema.get_decimal(&row, 2).to_string(), "99999.99");
	assert_eq!(schema.get_int(&row, 3), huge_int());
	assert_eq!(schema.get_uint(&row, 4), huge_uint());
	assert_eq!(schema.get_any(&row, 5), Value::Utf8("now a string".to_string()));
	assert_eq!(schema.get_bool(&row, 6), true);
	assert_eq!(schema.get_i32(&row, 7), 999);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "updated text that is longer");
	schema.set_blob(&mut fresh, 1, &Blob::from_slice(&[10, 20]));
	schema.set_decimal(&mut fresh, 2, &Decimal::from_str("99999.99").unwrap());
	schema.set_int(&mut fresh, 3, &huge_int());
	schema.set_uint(&mut fresh, 4, &huge_uint());
	schema.set_any(&mut fresh, 5, &Value::Utf8("now a string".to_string()));
	schema.set_bool(&mut fresh, 6, true);
	schema.set_i32(&mut fresh, 7, 999);
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_set_value_update_utf8() {
	let schema = Schema::testing(&[Type::Utf8, Type::Int4]);
	let mut row = schema.allocate();

	schema.set_value(&mut row, 0, &Value::Utf8("first".to_string()));
	schema.set_value(&mut row, 1, &Value::Int4(10));

	// Update via set_value
	schema.set_value(&mut row, 0, &Value::Utf8("updated".to_string()));

	assert_eq!(schema.get_value(&row, 0), Value::Utf8("updated".to_string()));
	assert_eq!(schema.get_value(&row, 1), Value::Int4(10));

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "updated");
	schema.set_i32(&mut fresh, 1, 10);
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_set_values_overwrite_entire_row() {
	let schema = Schema::testing(&[Type::Utf8, Type::Int4, Type::Blob]);
	let mut row = schema.allocate();

	let values1 =
		vec![Value::Utf8("first".to_string()), Value::Int4(10), Value::Blob(Blob::from_slice(&[1, 2, 3]))];
	schema.set_values(&mut row, &values1);

	assert_eq!(schema.get_value(&row, 0), Value::Utf8("first".to_string()));
	assert_eq!(schema.get_value(&row, 1), Value::Int4(10));
	assert_eq!(schema.get_value(&row, 2), Value::Blob(Blob::from_slice(&[1, 2, 3])));

	// Overwrite all values
	let values2 = vec![
		Value::Utf8("second, much longer".to_string()),
		Value::Int4(20),
		Value::Blob(Blob::from_slice(&[4])),
	];
	schema.set_values(&mut row, &values2);

	assert_eq!(schema.get_value(&row, 0), Value::Utf8("second, much longer".to_string()));
	assert_eq!(schema.get_value(&row, 1), Value::Int4(20));
	assert_eq!(schema.get_value(&row, 2), Value::Blob(Blob::from_slice(&[4])));

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "second, much longer");
	schema.set_i32(&mut fresh, 1, 20);
	schema.set_blob(&mut fresh, 2, &Blob::from_slice(&[4]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_defined_undefined_defined_cycle() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "hello");
	assert!(row.is_defined(0));
	assert_eq!(schema.get_utf8(&row, 0), "hello");

	schema.set_none(&mut row, 0);
	assert!(!row.is_defined(0));

	schema.set_utf8(&mut row, 0, "world");
	assert!(row.is_defined(0));
	assert_eq!(schema.get_utf8(&row, 0), "world");

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "world");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_set_none_then_set_different_dynamic_field() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "hello");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3]));

	// Clear first, set second to new value
	schema.set_none(&mut row, 0);
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[4, 5, 6, 7, 8]));

	assert!(!row.is_defined(0));
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[4, 5, 6, 7, 8]));

	let mut fresh = schema.allocate();
	schema.set_blob(&mut fresh, 1, &Blob::from_slice(&[4, 5, 6, 7, 8]));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_interleaved_none_and_set() {
	let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "aaa");
	schema.set_utf8(&mut row, 1, "bbb");
	schema.set_utf8(&mut row, 2, "ccc");

	// None on 1, update 0
	schema.set_none(&mut row, 1);
	schema.set_utf8(&mut row, 0, "AAAA");

	assert_eq!(schema.get_utf8(&row, 0), "AAAA");
	assert!(!row.is_defined(1));
	assert_eq!(schema.get_utf8(&row, 2), "ccc");

	// Re-set 1
	schema.set_utf8(&mut row, 1, "BBBB");
	assert_eq!(schema.get_utf8(&row, 0), "AAAA");
	assert_eq!(schema.get_utf8(&row, 1), "BBBB");
	assert_eq!(schema.get_utf8(&row, 2), "ccc");

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "AAAA");
	schema.set_utf8(&mut fresh, 1, "BBBB");
	schema.set_utf8(&mut fresh, 2, "ccc");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_clone_update_clone_original_unchanged() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob]);
	let mut row = schema.allocate();
	schema.set_utf8(&mut row, 0, "original");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3]));

	let mut cloned = row.clone();

	// Update clone
	schema.set_utf8(&mut cloned, 0, "modified in clone");
	schema.set_blob(&mut cloned, 1, &Blob::from_slice(&[4, 5, 6, 7, 8]));

	// Original unchanged
	assert_eq!(schema.get_utf8(&row, 0), "original");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[1, 2, 3]));

	// Clone has new values
	assert_eq!(schema.get_utf8(&cloned, 0), "modified in clone");
	assert_eq!(schema.get_blob(&cloned, 1), Blob::from_slice(&[4, 5, 6, 7, 8]));

	let mut fresh_orig = schema.allocate();
	schema.set_utf8(&mut fresh_orig, 0, "original");
	schema.set_blob(&mut fresh_orig, 1, &Blob::from_slice(&[1, 2, 3]));
	assert_eq!(row.len(), fresh_orig.len());

	let mut fresh_clone = schema.allocate();
	schema.set_utf8(&mut fresh_clone, 0, "modified in clone");
	schema.set_blob(&mut fresh_clone, 1, &Blob::from_slice(&[4, 5, 6, 7, 8]));
	assert_eq!(cloned.len(), fresh_clone.len());
}

#[test]
fn test_clone_update_original_clone_unchanged() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();
	schema.set_utf8(&mut row, 0, "original");

	let cloned = row.clone();

	// Update original
	schema.set_utf8(&mut row, 0, "modified in original");

	// Clone unchanged
	assert_eq!(schema.get_utf8(&cloned, 0), "original");
	assert_eq!(schema.get_utf8(&row, 0), "modified in original");

	let mut fresh_orig = schema.allocate();
	schema.set_utf8(&mut fresh_orig, 0, "modified in original");
	assert_eq!(row.len(), fresh_orig.len());

	let mut fresh_clone = schema.allocate();
	schema.set_utf8(&mut fresh_clone, 0, "original");
	assert_eq!(cloned.len(), fresh_clone.len());
}

#[test]
fn test_no_orphan_data_after_many_updates() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob]);
	let mut row = schema.allocate();

	for i in 0..100 {
		let s = format!("iter_{}", i);
		let b = Blob::from_slice(&vec![i as u8; (i % 20) + 1]);
		schema.set_utf8(&mut row, 0, &s);
		schema.set_blob(&mut row, 1, &b);

		// Verify no orphan: total = static + current utf8 len + current blob len
		let expected = schema.total_static_size() + s.len() + (i % 20) + 1;
		assert_eq!(
			row.len(),
			expected,
			"Orphan data at iteration {}: got {} expected {}",
			i,
			row.len(),
			expected
		);

		let mut fresh = schema.allocate();
		schema.set_utf8(&mut fresh, 0, &s);
		schema.set_blob(&mut fresh, 1, &b);
		assert_eq!(row.len(), fresh.len());
	}
}

#[test]
fn test_no_orphan_data_three_dynamic_fields() {
	let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "aaaa");
	schema.set_utf8(&mut row, 1, "bb");
	schema.set_utf8(&mut row, 2, "cccccc");
	assert_eq!(row.len(), schema.total_static_size() + 4 + 2 + 6);

	// Update each field with different sizes
	schema.set_utf8(&mut row, 0, "a");
	assert_eq!(row.len(), schema.total_static_size() + 1 + 2 + 6);

	schema.set_utf8(&mut row, 1, "bbbbb");
	assert_eq!(row.len(), schema.total_static_size() + 1 + 5 + 6);

	schema.set_utf8(&mut row, 2, "c");
	assert_eq!(row.len(), schema.total_static_size() + 1 + 5 + 1);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "a");
	schema.set_utf8(&mut fresh, 1, "bbbbb");
	schema.set_utf8(&mut fresh, 2, "c");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_no_orphan_data_mixed_types() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob, Type::Any]);
	let mut row = schema.allocate();

	schema.set_utf8(&mut row, 0, "hello"); // 5 bytes
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3])); // 3 bytes
	schema.set_any(&mut row, 2, &Value::Int4(42)); // 5 bytes (1 type + 4 data)

	let expected = schema.total_static_size() + 5 + 3 + 5;
	assert_eq!(row.len(), expected);

	// Update each with different sizes
	schema.set_utf8(&mut row, 0, "hi"); // 2 bytes
	let expected = schema.total_static_size() + 2 + 3 + 5;
	assert_eq!(row.len(), expected);

	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3, 4, 5, 6, 7])); // 7 bytes
	let expected = schema.total_static_size() + 2 + 7 + 5;
	assert_eq!(row.len(), expected);

	schema.set_any(&mut row, 2, &Value::Boolean(true)); // 2 bytes (1 type + 1 bool)
	let expected = schema.total_static_size() + 2 + 7 + 2;
	assert_eq!(row.len(), expected);

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "hi");
	schema.set_blob(&mut fresh, 1, &Blob::from_slice(&[1, 2, 3, 4, 5, 6, 7]));
	schema.set_any(&mut fresh, 2, &Value::Boolean(true));
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_repeated_set_unset_utf8() {
	let schema = Schema::testing(&[Type::Utf8]);
	let mut row = schema.allocate();

	for i in 0..10 {
		let val = format!("value_{}", i);
		schema.set_utf8(&mut row, 0, &val);
		assert_eq!(schema.get_utf8(&row, 0), val);

		let mut fresh = schema.allocate();
		schema.set_utf8(&mut fresh, 0, &val);
		assert_eq!(row.len(), fresh.len());

		schema.set_none(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(row.len(), schema.total_static_size());
	}

	schema.set_utf8(&mut row, 0, "final");
	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "final");
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_repeated_set_unset_blob() {
	let schema = Schema::testing(&[Type::Blob]);
	let mut row = schema.allocate();

	for i in 0..10 {
		let data = vec![i as u8; (i % 5) + 1];
		let blob = Blob::from_slice(&data);
		schema.set_blob(&mut row, 0, &blob);
		assert_eq!(schema.get_blob(&row, 0), blob);

		let mut fresh = schema.allocate();
		schema.set_blob(&mut fresh, 0, &blob);
		assert_eq!(row.len(), fresh.len());

		schema.set_none(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(row.len(), schema.total_static_size());
	}

	let final_blob = Blob::from_slice(&[0xFF; 8]);
	schema.set_blob(&mut row, 0, &final_blob);
	let mut fresh = schema.allocate();
	schema.set_blob(&mut fresh, 0, &final_blob);
	assert_eq!(row.len(), fresh.len());
}

#[test]
fn test_repeated_set_unset_mixed_dynamic() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob, Type::Decimal, Type::Any]);
	let mut row = schema.allocate();

	for i in 0..5 {
		let text = format!("round_{}", i);
		let blob = Blob::from_slice(&vec![i as u8; i + 1]);
		let decimal = Decimal::from_str(&format!("{}.{}", i * 10, i)).unwrap();
		let any_val = Value::Int4(i as i32);

		schema.set_utf8(&mut row, 0, &text);
		schema.set_blob(&mut row, 1, &blob);
		schema.set_decimal(&mut row, 2, &decimal);
		schema.set_any(&mut row, 3, &any_val);

		assert_eq!(schema.get_utf8(&row, 0), text);
		assert_eq!(schema.get_blob(&row, 1), blob);
		assert_eq!(schema.get_any(&row, 3), any_val);

		let mut fresh = schema.allocate();
		schema.set_utf8(&mut fresh, 0, &text);
		schema.set_blob(&mut fresh, 1, &blob);
		schema.set_decimal(&mut fresh, 2, &decimal);
		schema.set_any(&mut fresh, 3, &any_val);
		assert_eq!(row.len(), fresh.len());

		// Unset in different order each round
		match i % 4 {
			0 => {
				schema.set_none(&mut row, 0);
				schema.set_none(&mut row, 1);
				schema.set_none(&mut row, 2);
				schema.set_none(&mut row, 3);
			}
			1 => {
				schema.set_none(&mut row, 3);
				schema.set_none(&mut row, 2);
				schema.set_none(&mut row, 1);
				schema.set_none(&mut row, 0);
			}
			2 => {
				schema.set_none(&mut row, 1);
				schema.set_none(&mut row, 3);
				schema.set_none(&mut row, 0);
				schema.set_none(&mut row, 2);
			}
			_ => {
				schema.set_none(&mut row, 2);
				schema.set_none(&mut row, 0);
				schema.set_none(&mut row, 3);
				schema.set_none(&mut row, 1);
			}
		}

		assert_eq!(row.len(), schema.total_static_size());
		for idx in 0..4 {
			assert!(!row.is_defined(idx));
		}
	}
}

#[test]
fn test_set_unset_all_fields_then_reset() {
	let schema = Schema::testing(&[Type::Utf8, Type::Blob, Type::Any, Type::Int, Type::Decimal]);
	let mut row = schema.allocate();

	// Set all
	schema.set_utf8(&mut row, 0, "first");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[1, 2, 3]));
	schema.set_any(&mut row, 2, &Value::Utf8("any_first".to_string()));
	schema.set_int(&mut row, 3, &huge_int());
	schema.set_decimal(&mut row, 4, &Decimal::from_str("123.456").unwrap());
	assert!(row.len() > schema.total_static_size());

	// Unset all
	for i in 0..5 {
		schema.set_none(&mut row, i);
	}
	assert_eq!(row.len(), schema.total_static_size());
	for i in 0..5 {
		assert!(!row.is_defined(i));
	}

	// Re-set with different values
	schema.set_utf8(&mut row, 0, "second, much longer text");
	schema.set_blob(&mut row, 1, &Blob::from_slice(&[10, 20]));
	schema.set_any(&mut row, 2, &Value::Boolean(true));
	schema.set_int(&mut row, 3, &Int::from(42));
	schema.set_decimal(&mut row, 4, &Decimal::from_str("0.001").unwrap());

	assert_eq!(schema.get_utf8(&row, 0), "second, much longer text");
	assert_eq!(schema.get_blob(&row, 1), Blob::from_slice(&[10, 20]));
	assert_eq!(schema.get_any(&row, 2), Value::Boolean(true));
	assert_eq!(schema.get_int(&row, 3), Int::from(42));

	let mut fresh = schema.allocate();
	schema.set_utf8(&mut fresh, 0, "second, much longer text");
	schema.set_blob(&mut fresh, 1, &Blob::from_slice(&[10, 20]));
	schema.set_any(&mut fresh, 2, &Value::Boolean(true));
	schema.set_int(&mut fresh, 3, &Int::from(42));
	schema.set_decimal(&mut fresh, 4, &Decimal::from_str("0.001").unwrap());
	assert_eq!(row.len(), fresh.len());
}
