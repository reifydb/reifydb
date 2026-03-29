// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_i64(&self, row: &mut EncodedRow, index: usize, value: impl Into<i64>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Int8);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut i64,
				value.into(),
			)
		}
	}

	pub fn get_i64(&self, row: &EncodedRow, index: usize) -> i64 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Int8);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const i64).read_unaligned() }
	}

	pub fn try_get_i64(&self, row: &EncodedRow, index: usize) -> Option<i64> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Int8 {
			Some(self.get_i64(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_i64() {
		let shape = RowShape::testing(&[Type::Int8]);
		let mut row = shape.allocate();
		shape.set_i64(&mut row, 0, -987654321i64);
		assert_eq!(shape.get_i64(&row, 0), -987654321i64);
	}

	#[test]
	fn test_try_get_i64() {
		let shape = RowShape::testing(&[Type::Int8]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_i64(&row, 0), None);

		shape.set_i64(&mut row, 0, -987654321i64);
		assert_eq!(shape.try_get_i64(&row, 0), Some(-987654321i64));
	}

	#[test]
	fn test_extremes() {
		let shape = RowShape::testing(&[Type::Int8]);
		let mut row = shape.allocate();

		shape.set_i64(&mut row, 0, i64::MAX);
		assert_eq!(shape.get_i64(&row, 0), i64::MAX);

		let mut row2 = shape.allocate();
		shape.set_i64(&mut row2, 0, i64::MIN);
		assert_eq!(shape.get_i64(&row2, 0), i64::MIN);

		let mut row3 = shape.allocate();
		shape.set_i64(&mut row3, 0, 0i64);
		assert_eq!(shape.get_i64(&row3, 0), 0i64);
	}

	#[test]
	fn test_large_values() {
		let shape = RowShape::testing(&[Type::Int8]);

		let test_values = [
			-9_223_372_036_854_775_808i64,
			-1_000_000_000_000_000_000i64,
			-1i64,
			0i64,
			1i64,
			1_000_000_000_000_000_000i64,
			9_223_372_036_854_775_807i64,
		];

		for value in test_values {
			let mut row = shape.allocate();
			shape.set_i64(&mut row, 0, value);
			assert_eq!(shape.get_i64(&row, 0), value);
		}
	}

	#[test]
	fn test_timestamp_values() {
		let shape = RowShape::testing(&[Type::Int8]);

		// Test typical Unix timestamp values
		let timestamps = [
			0i64,           // Unix epoch
			1640995200i64,  // 2022-01-01 00:00:00 SVTC
			1735689600i64,  // 2025-01-01 00:00:00 SVTC
			-2147483648i64, // Before Unix epoch
		];

		for timestamp in timestamps {
			let mut row = shape.allocate();
			shape.set_i64(&mut row, 0, timestamp);
			assert_eq!(shape.get_i64(&row, 0), timestamp);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[Type::Int8, Type::Float8, Type::Int8]);
		let mut row = shape.allocate();

		shape.set_i64(&mut row, 0, -9_000_000_000_000_000i64);
		shape.set_f64(&mut row, 1, 3.14159265359);
		shape.set_i64(&mut row, 2, 8_000_000_000_000_000i64);

		assert_eq!(shape.get_i64(&row, 0), -9_000_000_000_000_000i64);
		assert_eq!(shape.get_f64(&row, 1), 3.14159265359);
		assert_eq!(shape.get_i64(&row, 2), 8_000_000_000_000_000i64);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[Type::Int8, Type::Int8]);
		let mut row = shape.allocate();

		shape.set_i64(&mut row, 0, 1234567890123456789i64);

		assert_eq!(shape.try_get_i64(&row, 0), Some(1234567890123456789));
		assert_eq!(shape.try_get_i64(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_i64(&row, 0), None);
	}

	#[test]
	fn test_try_get_i64_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_i64(&row, 0), None);
	}
}
