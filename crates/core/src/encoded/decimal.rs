// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use bigdecimal::BigDecimal as StdBigDecimal;
use num_bigint::BigInt as StdBigInt;
use reifydb_type::value::{decimal::Decimal, r#type::Type};

use crate::encoded::{encoded::EncodedValues, schema::Schema};

/// Decimal storage using dynamic section
/// All decimals are stored in dynamic section with MSB=1 to store both mantissa
/// and scale
const MODE_DYNAMIC: u128 = 0x80000000000000000000000000000000;
const MODE_MASK: u128 = 0x80000000000000000000000000000000;

/// Bit masks for dynamic mode (lower 127 bits contain offset+length)
const DYNAMIC_OFFSET_MASK: u128 = 0x0000000000000000FFFFFFFFFFFFFFFF; // 64 bits for offset
const DYNAMIC_LENGTH_MASK: u128 = 0x7FFFFFFFFFFFFFFF0000000000000000; // 63 bits for length

impl Schema {
	/// Set a Decimal value with 2-tier storage optimization
	/// - Values that fit in i128: stored inline with MSB=0
	/// - Large values: stored in dynamic section with MSB=1
	pub fn set_decimal(&self, row: &mut EncodedValues, index: usize, value: &Decimal) {
		let field = &self.fields()[index];
		debug_assert!(matches!(field.constraint.get_type(), Type::Decimal { .. }));

		// Get the mantissa and original scale from the BigDecimal
		let (mantissa, original_scale) = value.inner().as_bigint_and_exponent();

		// Always use dynamic storage to store both mantissa and scale
		debug_assert!(!row.is_defined(index), "Decimal field {} already set", index);

		// Serialize as scale (i64) + mantissa (variable bytes)
		let scale_bytes = original_scale.to_le_bytes();
		let digits_bytes = mantissa.to_signed_bytes_le();

		let dynamic_offset = self.dynamic_section_size(row);
		let total_size = 8 + digits_bytes.len(); // 8 bytes for scale + variable for mantissa

		// Append to dynamic section: scale first, then mantissa
		row.0.extend_from_slice(&scale_bytes);
		row.0.extend_from_slice(&digits_bytes);

		// Pack offset and length in lower 127 bits, set MSB=1
		let offset_part = (dynamic_offset as u128) & DYNAMIC_OFFSET_MASK;
		let length_part = ((total_size as u128) << 64) & DYNAMIC_LENGTH_MASK;
		let packed = MODE_DYNAMIC | offset_part | length_part;

		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut u128,
				packed.to_le(),
			);
		}
		row.set_valid(index, true);
	}

	/// Get a Decimal value, detecting storage mode from MSB
	pub fn get_decimal(&self, row: &EncodedValues, index: usize) -> Decimal {
		let field = &self.fields()[index];
		debug_assert!(matches!(field.constraint.get_type(), Type::Decimal { .. }));

		let packed = unsafe { (row.as_ptr().add(field.offset as usize) as *const u128).read_unaligned() };
		let packed = u128::from_le(packed);

		// Always expect dynamic storage (MSB=1)
		debug_assert!(packed & MODE_MASK == MODE_DYNAMIC, "Expected dynamic storage");

		// Extract offset and length
		let offset = (packed & DYNAMIC_OFFSET_MASK) as usize;
		let length = ((packed & DYNAMIC_LENGTH_MASK) >> 64) as usize;

		let dynamic_start = self.dynamic_section_start();
		let data_bytes = &row.as_slice()[dynamic_start + offset..dynamic_start + offset + length];

		// Parse scale (first 8 bytes) and mantissa (remaining bytes)
		let original_scale = i64::from_le_bytes(data_bytes[0..8].try_into().unwrap());
		let mantissa = StdBigInt::from_signed_bytes_le(&data_bytes[8..]);

		// Reconstruct the BigDecimal with original scale
		let big_decimal = StdBigDecimal::new(mantissa, original_scale);

		// Create our Decimal from the reconstructed BigDecimal
		Decimal::from(big_decimal)
	}

	/// Try to get a Decimal value, returning None if undefined
	pub fn try_get_decimal(&self, row: &EncodedValues, index: usize) -> Option<Decimal> {
		if row.is_defined(index) && matches!(self.fields()[index].constraint.get_type(), Type::Decimal { .. }) {
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

	use crate::encoded::schema::Schema;

	#[test]
	fn test_compact_inline() {
		let schema = Schema::testing(&[Type::Decimal]);
		let mut row = schema.allocate();

		// Test simple decimal
		let decimal = Decimal::from_str("123.45").unwrap();
		schema.set_decimal(&mut row, 0, &decimal);
		assert!(row.is_defined(0));

		let retrieved = schema.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "123.45");

		// Test negative decimal
		let mut row2 = schema.allocate();
		let negative = Decimal::from_str("-999.99").unwrap();
		schema.set_decimal(&mut row2, 0, &negative);
		assert_eq!(schema.get_decimal(&row2, 0).to_string(), "-999.99");
	}

	#[test]
	fn test_compact_boundaries() {
		// Test high precision decimal
		let schema1 = Schema::testing(&[Type::Decimal]);
		let mut row1 = schema1.allocate();
		let high_precision = Decimal::from_str("1.0000000000000000000000000000001").unwrap();
		schema1.set_decimal(&mut row1, 0, &high_precision);
		let retrieved = schema1.get_decimal(&row1, 0);
		assert_eq!(retrieved.to_string(), "1.0000000000000000000000000000001");

		// Test large integer (scale 0)
		let schema2 = Schema::testing(&[Type::Decimal]);
		let mut row2 = schema2.allocate();
		let large_int = Decimal::from_str("100000000000000000000000000000000").unwrap();
		schema2.set_decimal(&mut row2, 0, &large_int);
		assert_eq!(schema2.get_decimal(&row2, 0).to_string(), "100000000000000000000000000000000");
	}

	#[test]
	fn test_extended_i128() {
		let schema = Schema::testing(&[Type::Decimal]);
		let mut row = schema.allocate();

		// Value that needs i128 mantissa
		let large = Decimal::from_str("999999999999999999999.123456789").unwrap();
		schema.set_decimal(&mut row, 0, &large);
		assert!(row.is_defined(0));

		let retrieved = schema.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "999999999999999999999.123456789");
	}

	#[test]
	fn test_dynamic_storage() {
		// Use a smaller test that will still trigger dynamic storage
		// due to large mantissa
		let schema = Schema::testing(&[Type::Decimal]);
		let mut row = schema.allocate();

		// Create a value with large precision that will exceed i128
		// when scaled
		let huge = Decimal::from_str("99999999999999999999999999999.123456789").unwrap();

		schema.set_decimal(&mut row, 0, &huge);
		assert!(row.is_defined(0));

		let retrieved = schema.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "99999999999999999999999999999.123456789");
	}

	#[test]
	fn test_zero() {
		let schema = Schema::testing(&[Type::Decimal]);
		let mut row = schema.allocate();

		let zero = Decimal::from_str("0.0").unwrap();
		schema.set_decimal(&mut row, 0, &zero);
		assert!(row.is_defined(0));

		let retrieved = schema.get_decimal(&row, 0);
		assert!(retrieved.inner().is_zero());
	}

	#[test]
	fn test_currency_values() {
		let schema = Schema::testing(&[Type::Decimal]);

		// Test typical currency value (2 decimal places)
		let mut row1 = schema.allocate();
		let price = Decimal::from_str("19.99").unwrap();
		schema.set_decimal(&mut row1, 0, &price);
		assert_eq!(schema.get_decimal(&row1, 0).to_string(), "19.99");

		// Test large currency value
		let mut row2 = schema.allocate();
		let large_price = Decimal::from_str("999999999.99").unwrap();
		schema.set_decimal(&mut row2, 0, &large_price);
		assert_eq!(schema.get_decimal(&row2, 0).to_string(), "999999999.99");

		// Test small fraction
		let mut row3 = schema.allocate();
		let fraction = Decimal::from_str("0.00000001").unwrap();
		schema.set_decimal(&mut row3, 0, &fraction);
		assert_eq!(schema.get_decimal(&row3, 0), fraction);
	}

	#[test]
	fn test_scientific_notation() {
		let schema = Schema::testing(&[Type::Decimal]);
		let mut row = schema.allocate();

		let scientific = Decimal::from_str("1.23456e10").unwrap();
		schema.set_decimal(&mut row, 0, &scientific);

		let retrieved = schema.get_decimal(&row, 0);
		assert_eq!(retrieved.to_string(), "12345600000");
	}

	#[test]
	fn test_try_get() {
		let schema = Schema::testing(&[Type::Decimal]);
		let mut row = schema.allocate();

		// Undefined initially
		assert_eq!(schema.try_get_decimal(&row, 0), None);

		// Set value
		let value = Decimal::from_str("42.42").unwrap();
		schema.set_decimal(&mut row, 0, &value);

		let retrieved = schema.try_get_decimal(&row, 0);
		assert!(retrieved.is_some());
		assert_eq!(retrieved.unwrap().to_string(), "42.42");
	}

	#[test]
	fn test_clone_on_write() {
		let schema = Schema::testing(&[Type::Decimal]);
		let row1 = schema.allocate();
		let mut row2 = row1.clone();

		let value = Decimal::from_str("3.14159").unwrap();
		schema.set_decimal(&mut row2, 0, &value);

		assert!(!row1.is_defined(0));
		assert!(row2.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
		assert_eq!(schema.get_decimal(&row2, 0).to_string(), "3.14159");
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Boolean, Type::Decimal, Type::Utf8, Type::Decimal, Type::Int4]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		let small_decimal = Decimal::from_str("99.99").unwrap();
		schema.set_decimal(&mut row, 1, &small_decimal);

		schema.set_utf8(&mut row, 2, "test");

		let large_decimal = Decimal::from_str("123456789.987654321").unwrap();
		schema.set_decimal(&mut row, 3, &large_decimal);

		schema.set_i32(&mut row, 4, -42);

		assert_eq!(schema.get_bool(&row, 0), true);
		assert_eq!(schema.get_decimal(&row, 1).to_string(), "99.99");
		assert_eq!(schema.get_utf8(&row, 2), "test");
		assert_eq!(schema.get_decimal(&row, 3).to_string(), "123456789.987654321");
		assert_eq!(schema.get_i32(&row, 4), -42);
	}

	#[test]
	fn test_negative_values() {
		// Small negative (compact inline) - needs scale 2
		let schema1 = Schema::testing(&[Type::Decimal]);

		let mut row1 = schema1.allocate();
		let small_neg = Decimal::from_str("-0.01").unwrap();
		schema1.set_decimal(&mut row1, 0, &small_neg);
		assert_eq!(schema1.get_decimal(&row1, 0).to_string(), "-0.01");

		// Large negative (extended i128) - needs scale 3
		let schema2 = Schema::testing(&[Type::Decimal]);
		let mut row2 = schema2.allocate();
		let large_neg = Decimal::from_str("-999999999999999999.999").unwrap();
		schema2.set_decimal(&mut row2, 0, &large_neg);
		assert_eq!(schema2.get_decimal(&row2, 0).to_string(), "-999999999999999999.999");

		// Huge negative (dynamic) - needs scale 9
		let schema3 = Schema::testing(&[Type::Decimal]);
		let mut row3 = schema3.allocate();
		let huge_neg = Decimal::from_str("-99999999999999999999999999999.999999999").unwrap();
		schema3.set_decimal(&mut row3, 0, &huge_neg);
		assert_eq!(schema3.get_decimal(&row3, 0).to_string(), "-99999999999999999999999999999.999999999");
	}

	#[test]
	fn test_try_get_decimal_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_decimal(&row, 0), None);
	}
}
