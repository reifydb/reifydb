// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	decimal::Decimal,
	value_type::ValueType,
};

fn decimal(precision: u8, scale: u8) -> ValueType {
	ValueType::decimal(Precision::new(precision), Scale::new(scale))
}

#[test]
fn test_compact_inline() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(38, 2)]);
	let mut row = shape.allocate_pod();

	let decimal = Decimal::from_str("123.45").unwrap();
	shape.set_decimal(&mut row, 0, &decimal);
	assert!(row.is_defined(0));

	let retrieved = shape.get_decimal(&row, 0);
	assert_eq!(retrieved.to_string(), "123.45");

	let mut row2 = shape.allocate_pod();
	let negative = Decimal::from_str("-999.99").unwrap();
	shape.set_decimal(&mut row2, 0, &negative);
	assert_eq!(shape.get_decimal(&row2, 0).to_string(), "-999.99");
}

#[test]
fn test_compact_boundaries() {
	// Scale comes from the column, not the slot, so a high scale and a scale-0 integer of the same digit count
	// share one slot width.
	let shape1 = RowShape::testing(RowFamily::Pod, &[decimal(38, 31)]);
	let mut row1 = shape1.allocate_pod();
	let high_precision = Decimal::from_str("1.0000000000000000000000000000001").unwrap();
	shape1.set_decimal(&mut row1, 0, &high_precision);
	let retrieved = shape1.get_decimal(&row1, 0);
	assert_eq!(retrieved.to_string(), "1.0000000000000000000000000000001");

	let shape2 = RowShape::testing(RowFamily::Pod, &[decimal(38, 0)]);
	let mut row2 = shape2.allocate_pod();
	let large_int = Decimal::from_str("100000000000000000000000000000000").unwrap();
	shape2.set_decimal(&mut row2, 0, &large_int);
	assert_eq!(shape2.get_decimal(&row2, 0).to_string(), "100000000000000000000000000000000");
	assert_eq!(shape1.fields()[0].size, shape2.fields()[0].size);
}

#[test]
fn test_extended_i128() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(76, 9)]);
	let mut row = shape.allocate_pod();

	// A 39 digit unscaled value is past i128, so it only survives in the 32 byte slot.
	let large = Decimal::from_str("999999999999999999999999999999.123456789").unwrap();
	shape.set_decimal(&mut row, 0, &large);
	assert!(row.is_defined(0));

	let retrieved = shape.get_decimal(&row, 0);
	assert_eq!(retrieved.to_string(), "999999999999999999999999999999.123456789");
}

#[test]
fn test_dynamic_storage() {
	// Every decimal lives in its fixed slot regardless of magnitude; a row that grew would mean it spilled.
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(76, 9)]);
	let mut row = shape.allocate_pod();

	let huge = Decimal::from_str("99999999999999999999999999999999999999999999999999999999999999999.123456789")
		.unwrap();

	shape.set_decimal(&mut row, 0, &huge);
	assert!(row.is_defined(0));
	assert_eq!(row.len(), shape.total_static_size());

	let retrieved = shape.get_decimal(&row, 0);
	assert_eq!(
		retrieved.to_string(),
		"99999999999999999999999999999999999999999999999999999999999999999.123456789"
	);
}

#[test]
fn test_slot_width_follows_precision() {
	// Precision 38 is the widest that fits i128 whatever the scale; one more digit must switch to 32 bytes.
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[decimal(38, 0), decimal(38, 38), decimal(39, 2), ValueType::DECIMAL, decimal(1, 1)],
	);
	let sizes: Vec<u32> = shape.fields().iter().map(|field| field.size).collect();
	assert_eq!(sizes, vec![16, 16, 32, 32, 16]);
}

#[test]
fn test_value_is_stored_at_the_column_scale() {
	// A value with fewer fraction digits is rescaled up on write, so it reads back with the column scale.
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(10, 4)]);
	let mut row = shape.allocate_pod();
	shape.set_decimal(&mut row, 0, &Decimal::from_str("1.5").unwrap());
	let retrieved = shape.get_decimal(&row, 0);
	assert_eq!(retrieved.scale(), 4);
	assert_eq!(retrieved.to_string(), "1.5000");
}

#[test]
#[should_panic(expected = "does not fit")]
fn test_set_with_lossy_scale_panics() {
	// Writing 1.25 into a scale 1 column would drop a digit, so it must be refused, not rounded.
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(10, 1)]);
	let mut row = shape.allocate_pod();
	shape.set_decimal(&mut row, 0, &Decimal::from_str("1.25").unwrap());
}

#[test]
#[should_panic(expected = "does not fit")]
fn test_set_past_precision_panics() {
	// 100.0 needs four digits at scale 1, one more than precision 3 allows.
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(3, 1)]);
	let mut row = shape.allocate_pod();
	shape.set_decimal(&mut row, 0, &Decimal::from_str("100.0").unwrap());
}

#[test]
fn test_zero() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(38, 1)]);
	let mut row = shape.allocate_pod();

	let zero = Decimal::from_str("0.0").unwrap();
	shape.set_decimal(&mut row, 0, &zero);
	assert!(row.is_defined(0));

	let retrieved = shape.get_decimal(&row, 0);
	assert!(retrieved.is_zero());
}

#[test]
fn test_currency_values() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(38, 2)]);

	let mut row1 = shape.allocate_pod();
	let price = Decimal::from_str("19.99").unwrap();
	shape.set_decimal(&mut row1, 0, &price);
	assert_eq!(shape.get_decimal(&row1, 0).to_string(), "19.99");

	let mut row2 = shape.allocate_pod();
	let large_price = Decimal::from_str("999999999.99").unwrap();
	shape.set_decimal(&mut row2, 0, &large_price);
	assert_eq!(shape.get_decimal(&row2, 0).to_string(), "999999999.99");

	let fine = RowShape::testing(RowFamily::Pod, &[decimal(38, 8)]);
	let mut row3 = fine.allocate_pod();
	let fraction = Decimal::from_str("0.00000001").unwrap();
	fine.set_decimal(&mut row3, 0, &fraction);
	assert_eq!(fine.get_decimal(&row3, 0), fraction);
}

#[test]
fn test_scientific_notation() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(38, 0)]);
	let mut row = shape.allocate_pod();

	let scientific = Decimal::from_str("1.23456e10").unwrap();
	shape.set_decimal(&mut row, 0, &scientific);

	let retrieved = shape.get_decimal(&row, 0);
	assert_eq!(retrieved.to_string(), "12345600000");
}

#[test]
fn test_try_get() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(38, 2)]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get_decimal(&row, 0), None);

	let value = Decimal::from_str("42.42").unwrap();
	shape.set_decimal(&mut row, 0, &value);

	let retrieved = shape.try_get_decimal(&row, 0);
	assert!(retrieved.is_some());
	assert_eq!(retrieved.unwrap().to_string(), "42.42");
}

#[test]
fn test_clone_on_write() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(38, 5)]);
	let row1 = shape.allocate_pod();
	let mut row2 = row1.clone();

	let value = Decimal::from_str("3.14159").unwrap();
	shape.set_decimal(&mut row2, 0, &value);

	assert!(!row1.is_defined(0));
	assert!(row2.is_defined(0));
	assert_ne!(row1.as_ptr(), row2.as_ptr());
	assert_eq!(shape.get_decimal(&row2, 0).to_string(), "3.14159");
}

#[test]
fn test_mixed_with_other_types() {
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[ValueType::Boolean, decimal(38, 2), ValueType::Utf8, decimal(76, 9), ValueType::Int4],
	);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	let small_decimal = Decimal::from_str("99.99").unwrap();
	shape.set_decimal(&mut row, 1, &small_decimal);

	shape.set_utf8(&mut row, 2, "test");

	let large_decimal = Decimal::from_str("123456789.987654321").unwrap();
	shape.set_decimal(&mut row, 3, &large_decimal);

	shape.set::<i32>(&mut row, 4, -42i32);

	assert_eq!(shape.get::<bool>(&row, 0), true);
	assert_eq!(shape.get_decimal(&row, 1).to_string(), "99.99");
	assert_eq!(shape.get_utf8(&row, 2), "test");
	assert_eq!(shape.get_decimal(&row, 3).to_string(), "123456789.987654321");
	assert_eq!(shape.get::<i32>(&row, 4), -42);
}

#[test]
fn test_negative_values() {
	// The unscaled value is stored two's complement, so the sign has to survive in both slot widths.
	let shape1 = RowShape::testing(RowFamily::Pod, &[decimal(38, 2)]);

	let mut row1 = shape1.allocate_pod();
	let small_neg = Decimal::from_str("-0.01").unwrap();
	shape1.set_decimal(&mut row1, 0, &small_neg);
	assert_eq!(shape1.get_decimal(&row1, 0).to_string(), "-0.01");

	let shape2 = RowShape::testing(RowFamily::Pod, &[decimal(38, 3)]);
	let mut row2 = shape2.allocate_pod();
	let large_neg = Decimal::from_str("-999999999999999999.999").unwrap();
	shape2.set_decimal(&mut row2, 0, &large_neg);
	assert_eq!(shape2.get_decimal(&row2, 0).to_string(), "-999999999999999999.999");

	let shape3 = RowShape::testing(RowFamily::Pod, &[decimal(76, 9)]);
	let mut row3 = shape3.allocate_pod();
	let huge_neg = Decimal::from_str("-99999999999999999999999999999999999999999999999999.999999999").unwrap();
	shape3.set_decimal(&mut row3, 0, &huge_neg);
	assert_eq!(
		shape3.get_decimal(&row3, 0).to_string(),
		"-99999999999999999999999999999999999999999999999999.999999999"
	);
}

#[test]
fn test_try_get_decimal_wrong_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get_decimal(&row, 0), None);
}

#[test]
fn test_update_decimal() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(76, 9)]);
	let mut row = shape.allocate_pod();

	let d1 = Decimal::from_str("123.45").unwrap();
	shape.set_decimal(&mut row, 0, &d1);
	assert_eq!(shape.get_decimal(&row, 0).to_string(), "123.450000000");

	let d2 = Decimal::from_str("-999.99").unwrap();
	shape.set_decimal(&mut row, 0, &d2);
	assert_eq!(shape.get_decimal(&row, 0).to_string(), "-999.990000000");

	// The overwrite must replace all 32 bytes; stale upper bytes of the negative value would corrupt it.
	let d3 = Decimal::from_str("99999999999999999999999999999.123456789").unwrap();
	shape.set_decimal(&mut row, 0, &d3);
	assert_eq!(shape.get_decimal(&row, 0).to_string(), "99999999999999999999999999999.123456789");
	assert_eq!(row.len(), shape.total_static_size());
}

#[test]
fn test_update_decimal_with_other_dynamic_fields() {
	let shape = RowShape::testing(RowFamily::Pod, &[decimal(38, 5), ValueType::Utf8, decimal(38, 1)]);
	let mut row = shape.allocate_pod();

	shape.set_decimal(&mut row, 0, &Decimal::from_str("1.0").unwrap());
	shape.set_utf8(&mut row, 1, "test");
	shape.set_decimal(&mut row, 2, &Decimal::from_str("2.0").unwrap());

	// Rewriting the first slot must not disturb the utf8 bytes or the slot stored after it.
	shape.set_decimal(&mut row, 0, &Decimal::from_str("99999.12345").unwrap());

	assert_eq!(shape.get_decimal(&row, 0).to_string(), "99999.12345");
	assert_eq!(shape.get_utf8(&row, 1), "test");
	assert_eq!(shape.get_decimal(&row, 2).to_string(), "2.0");
}
