// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::encoded::{encoded::EncodedValues, schema::Schema};

impl Schema {
	pub fn set_u32(&self, row: &mut EncodedValues, index: usize, value: impl Into<u32>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uint4);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut u32,
				value.into(),
			)
		}
	}

	pub fn get_u32(&self, row: &EncodedValues, index: usize) -> u32 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uint4);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const u32).read_unaligned() }
	}

	pub fn try_get_u32(&self, row: &EncodedValues, index: usize) -> Option<u32> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Uint4 {
			Some(self.get_u32(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use crate::encoded::schema::Schema;

	#[test]
	fn test_set_get_u32() {
		let schema = Schema::testing(&[Type::Uint4]);
		let mut row = schema.allocate();
		schema.set_u32(&mut row, 0, 4294967295u32);
		assert_eq!(schema.get_u32(&row, 0), 4294967295u32);
	}

	#[test]
	fn test_try_get_u32() {
		let schema = Schema::testing(&[Type::Uint4]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_u32(&row, 0), None);

		schema.set_u32(&mut row, 0, 4294967295u32);
		assert_eq!(schema.try_get_u32(&row, 0), Some(4294967295u32));
	}

	#[test]
	fn test_extremes() {
		let schema = Schema::testing(&[Type::Uint4]);
		let mut row = schema.allocate();

		schema.set_u32(&mut row, 0, u32::MAX);
		assert_eq!(schema.get_u32(&row, 0), u32::MAX);

		let mut row2 = schema.allocate();
		schema.set_u32(&mut row2, 0, u32::MIN);
		assert_eq!(schema.get_u32(&row2, 0), u32::MIN);

		let mut row3 = schema.allocate();
		schema.set_u32(&mut row3, 0, 0u32);
		assert_eq!(schema.get_u32(&row3, 0), 0u32);
	}

	#[test]
	fn test_large_values() {
		let schema = Schema::testing(&[Type::Uint4]);

		let test_values = [
			0u32,
			1u32,
			1_000_000u32,
			1_000_000_000u32,
			2_147_483_647u32, // i32::MAX
			2_147_483_648u32, // i32::MAX + 1
			4_000_000_000u32,
			4_294_967_294u32,
			4_294_967_295u32, // u32::MAX
		];

		for value in test_values {
			let mut row = schema.allocate();
			schema.set_u32(&mut row, 0, value);
			assert_eq!(schema.get_u32(&row, 0), value);
		}
	}

	#[test]
	fn test_timestamp_values() {
		let schema = Schema::testing(&[Type::Uint4]);

		// Test Unix timestamp values that fit in u32
		let timestamps = [
			0u32,          // Unix epoch
			946684800u32,  // 2000-01-01 00:00:00 SVTC
			1640995200u32, // 2022-01-01 00:00:00 SVTC
			2147483647u32, // 2038-01-19 (Y2038 boundary)
		];

		for timestamp in timestamps {
			let mut row = schema.allocate();
			schema.set_u32(&mut row, 0, timestamp);
			assert_eq!(schema.get_u32(&row, 0), timestamp);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Uint4, Type::Float4, Type::Uint4]);
		let mut row = schema.allocate();

		schema.set_u32(&mut row, 0, 3_000_000_000u32);
		schema.set_f32(&mut row, 1, 3.14f32);
		schema.set_u32(&mut row, 2, 1_500_000_000u32);

		assert_eq!(schema.get_u32(&row, 0), 3_000_000_000u32);
		assert_eq!(schema.get_f32(&row, 1), 3.14f32);
		assert_eq!(schema.get_u32(&row, 2), 1_500_000_000u32);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Uint4, Type::Uint4]);
		let mut row = schema.allocate();

		schema.set_u32(&mut row, 0, 123456789u32);

		assert_eq!(schema.try_get_u32(&row, 0), Some(123456789));
		assert_eq!(schema.try_get_u32(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_u32(&row, 0), None);
	}

	#[test]
	fn test_try_get_u32_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_u32(&row, 0), None);
	}
}
