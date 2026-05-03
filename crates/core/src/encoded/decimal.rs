// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use bigdecimal::BigDecimal as StdBigDecimal;
use num_bigint::BigInt as StdBigInt;
use reifydb_type::value::{decimal::Decimal, r#type::Type};

use crate::encoded::{row::EncodedRow, shape::RowShape};

const MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF;
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000;

impl RowShape {
	pub fn set_decimal(&self, row: &mut EncodedRow, index: usize, value: &Decimal) {
		debug_assert!(matches!(self.fields()[index].constraint.get_type().inner_type(), Type::Decimal));

		let (mantissa, original_scale) = value.inner().as_bigint_and_exponent();
		let scale_bytes = original_scale.to_le_bytes();
		let digits_bytes = mantissa.to_signed_bytes_le();

		let mut serialized = Vec::with_capacity(8 + digits_bytes.len());
		serialized.extend_from_slice(&scale_bytes);
		serialized.extend_from_slice(&digits_bytes);

		self.replace_dynamic_data(row, index, &serialized);
	}

	pub fn get_decimal(&self, row: &EncodedRow, index: usize) -> Decimal {
		let field = &self.fields()[index];
		debug_assert!(matches!(field.constraint.get_type().inner_type(), Type::Decimal));

		let packed = unsafe { (row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned() };
		let packed = u128::from_le(packed);

		debug_assert!(packed & MODE_MASK == MODE_DYNAMIC, "Expected dynamic storage");

		let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
		let length = ((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

		let dynamic_start = self.dynamic_section_start();
		let data_bytes = &row.as_slice()[dynamic_start + offset..dynamic_start + offset + length];

		let original_scale = i64::from_le_bytes(data_bytes[0..8].try_into().unwrap());
		let mantissa = StdBigInt::from_signed_bytes_le(&data_bytes[8..]);

		let big_decimal = StdBigDecimal::new(mantissa, original_scale);

		Decimal::from(big_decimal)
	}

	pub fn try_get_decimal(&self, row: &EncodedRow, index: usize) -> Option<Decimal> {
		if row.is_defined(index)
			&& matches!(self.fields()[index].constraint.get_type().inner_type(), Type::Decimal)
		{
			Some(self.get_decimal(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use std::str::FromStr;

	use num_traits::Zero;
	use reifydb_type::value::{decimal::Decimal, r#type::Type};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_compact_inline() {
		let shape = RowShape::testing(&[Type::Decimal]);
		let mut row = shape.allocate();

		// Test simple decimal
		let decimal = Decimal::from_str("123.45").unwrap();
		shape.set_decimal(&mut row, 0, &decimal);
		assert!(row.is_defined(0));

		let retrieved = shape.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "123.45");

		// Test negative decimal
		let mut row2 = shape.allocate();
		let negative = Decimal::from_str("-999.99").unwrap();
		shape.set_decimal(&mut row2, 0, &negative);
		assert_eq!(shape.get_decimal(&row2, 0).to_string(), "-999.99");
	}

	#[test]
	fn test_compact_boundaries() {
		// Test high precision decimal
		let shape1 = RowShape::testing(&[Type::Decimal]);
		let mut row1 = shape1.allocate();
		let high_precision = Decimal::from_str("1.0000000000000000000000000000001").unwrap();
		shape1.set_decimal(&mut row1, 0, &high_precision);
		let retrieved = shape1.get_decimal(&row1, 0);
		assert_eq!(retrieved.to_string(), "1.0000000000000000000000000000001");

		// Test large integer (scale 0)
		let shape2 = RowShape::testing(&[Type::Decimal]);
		let mut row2 = shape2.allocate();
		let large_int = Decimal::from_str("100000000000000000000000000000000").unwrap();
		shape2.set_decimal(&mut row2, 0, &large_int);
		assert_eq!(shape2.get_decimal(&row2, 0).to_string(), "100000000000000000000000000000000");
	}

	#[test]
	fn test_extended_i128() {
		let shape = RowShape::testing(&[Type::Decimal]);
		let mut row = shape.allocate();

		// Value that needs i128 mantissa
		let large = Decimal::from_str("999999999999999999999.123456789").unwrap();
		shape.set_decimal(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = shape.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "999999999999999999999.123456789");
	}

	#[test]
	fn test_dynamic_storage() {
		// Use a smaller test that will still trigger dynamic storage
		// due to large mantissa
		let shape = RowShape::testing(&[Type::Decimal]);
		let mut row = shape.allocate();

		// Create a value with large precision that will exceed i128
		// when scaled
		let huge = Decimal::from_str("99999999999999999999999999999.123456789").unwrap();

		shape.set_decimal(&mut row, 0, &huge);
		assert!(row.is_defined(0));

		let retrieved = shape.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "99999999999999999999999999999.123456789");
	}

	#[test]
	fn test_zero() {
		let shape = RowShape::testing(&[Type::Decimal]);
		let mut row = shape.allocate();

		let zero = Decimal::from_str("0.0").unwrap();
		shape.set_decimal(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = shape.get_decimal(&row, 0);
		assert!(retrieved.inner().is_zero());
	}

	#[test]
	fn test_currency_values() {
		let shape = RowShape::testing(&[Type::Decimal]);

		// Test typical currency value (2 decimal places)
		let mut row1 = shape.allocate();
		let price = Decimal::from_str("19.99").unwrap();
		shape.set_decimal(&mut row1, 0, &price);
		assert_eq!(shape.get_decimal(&row1, 0).to_string(), "19.99");

		// Test large currency value
		let mut row2 = shape.allocate();
		let large_price = Decimal::from_str("999999999.99").unwrap();
		shape.set_decimal(&mut row2, 0, &large_price);
		assert_eq!(shape.get_decimal(&row2, 0).to_string(), "999999999.99");

		// Test small fraction
		let mut row3 = shape.allocate();
		let fraction = Decimal::from_str("0.00000001").unwrap();
		shape.set_decimal(&mut row3, 0, &fraction);
		assert_eq!(shape.get_decimal(&row3, 0), fraction);
	}

	#[test]
	fn test_scientific_notation() {
		let shape = RowShape::testing(&[Type::Decimal]);
		let mut row = shape.allocate();

		let scientific = Decimal::from_str("1.23456e10").unwrap();
		shape.set_decimal(&mut row, 0, &scientific);

		let retrieved = shape.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "12345600000");
	}

	#[test]
	fn test_try_get() {
		let shape = RowShape::testing(&[Type::Decimal]);
		let mut row = shape.allocate();

		// Undefined initially
		assert_eq!(shape.try_get_decimal(&row, 0), None);

		// Set value
		let value = Decimal::from_str("42.42").unwrap();
		shape.set_decimal(&mut row, 0, &value);

		let retrieved = shape.try_get_decimal(&row, 0);
		assert!(retrieved.is_some());
		assert_eq!(retrieved.unwrap().to_string(), "42.42");
	}

	#[test]
	fn test_clone_on_write() {
		let shape = RowShape::testing(&[Type::Decimal]);
		let row1 = shape.allocate();
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
		let shape = RowShape::testing(&[Type::Boolean, Type::Decimal, Type::Utf8, Type::Decimal, Type::Int4]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		let small_decimal = Decimal::from_str("99.99").unwrap();
		shape.set_decimal(&mut row, 1, &small_decimal);

		shape.set_utf8(&mut row, 2, "test");

		let large_decimal = Decimal::from_str("123456789.987654321").unwrap();
		shape.set_decimal(&mut row, 3, &large_decimal);

		shape.set_i32(&mut row, 4, -42);

		assert_eq!(shape.get_bool(&row, 0), true);
		assert_eq!(shape.get_decimal(&row, 1).to_string(), "99.99");
		assert_eq!(shape.get_utf8(&row, 2), "test");
		assert_eq!(shape.get_decimal(&row, 3).to_string(), "123456789.987654321");
		assert_eq!(shape.get_i32(&row, 4), -42);
	}

	#[test]
	fn test_negative_values() {
		// Small negative (compact inline) - needs scale 2
		let shape1 = RowShape::testing(&[Type::Decimal]);

		let mut row1 = shape1.allocate();
		let small_neg = Decimal::from_str("-0.01").unwrap();
		shape1.set_decimal(&mut row1, 0, &small_neg);
		assert_eq!(shape1.get_decimal(&row1, 0).to_string(), "-0.01");

		// Large negative (extended i128) - needs scale 3
		let shape2 = RowShape::testing(&[Type::Decimal]);
		let mut row2 = shape2.allocate();
		let large_neg = Decimal::from_str("-999999999999999999.999").unwrap();
		shape2.set_decimal(&mut row2, 0, &large_neg);
		assert_eq!(shape2.get_decimal(&row2, 0).to_string(), "-999999999999999999.999");

		// Huge negative (dynamic) - needs scale 9
		let shape3 = RowShape::testing(&[Type::Decimal]);
		let mut row3 = shape3.allocate();
		let huge_neg = Decimal::from_str("-99999999999999999999999999999.999999999").unwrap();
		shape3.set_decimal(&mut row3, 0, &huge_neg);
		assert_eq!(shape3.get_decimal(&row3, 0).to_string(), "-99999999999999999999999999999.999999999");
	}

	#[test]
	fn test_try_get_decimal_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_decimal(&row, 0), None);
	}

	#[test]
	fn test_update_decimal() {
		let shape = RowShape::testing(&[Type::Decimal]);
		let mut row = shape.allocate();

		let d1 = Decimal::from_str("123.45").unwrap();
		shape.set_decimal(&mut row, 0, &d1);
		assert_eq!(shape.get_decimal(&row, 0).to_string(), "123.45");

		// Overwrite with a different value
		let d2 = Decimal::from_str("999.99").unwrap();
		shape.set_decimal(&mut row, 0, &d2);
		assert_eq!(shape.get_decimal(&row, 0).to_string(), "999.99");

		// Overwrite with a larger precision value
		let d3 = Decimal::from_str("99999999999999999999999999999.123456789").unwrap();
		shape.set_decimal(&mut row, 0, &d3);
		assert_eq!(shape.get_decimal(&row, 0).to_string(), "99999999999999999999999999999.123456789");
	}

	#[test]
	fn test_update_decimal_with_other_dynamic_fields() {
		let shape = RowShape::testing(&[Type::Decimal, Type::Utf8, Type::Decimal]);
		let mut row = shape.allocate();

		shape.set_decimal(&mut row, 0, &Decimal::from_str("1.0").unwrap());
		shape.set_utf8(&mut row, 1, "test");
		shape.set_decimal(&mut row, 2, &Decimal::from_str("2.0").unwrap());

		// Update first decimal
		shape.set_decimal(&mut row, 0, &Decimal::from_str("99999.12345").unwrap());

		assert_eq!(shape.get_decimal(&row, 0).to_string(), "99999.12345");
		assert_eq!(shape.get_utf8(&row, 1), "test");
		assert_eq!(shape.get_decimal(&row, 2).to_string(), "2.0");
	}
}
