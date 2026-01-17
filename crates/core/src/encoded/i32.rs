// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::encoded::{encoded::EncodedValues, schema::Schema};

impl Schema {
	pub fn set_i32(&self, row: &mut EncodedValues, index: usize, value: impl Into<i32>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::Int4);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut i32,
				value.into(),
			)
		}
	}

	pub fn get_i32(&self, row: &EncodedValues, index: usize) -> i32 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::Int4);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const i32).read_unaligned() }
	}

	pub fn try_get_i32(&self, row: &EncodedValues, index: usize) -> Option<i32> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Int4 {
			Some(self.get_i32(row, index))
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
	fn test_set_get_i32() {
		let schema = Schema::testing(&[Type::Int4]);
		let mut row = schema.allocate();
		schema.set_i32(&mut row, 0, 56789i32);
		assert_eq!(schema.get_i32(&row, 0), 56789i32);
	}

	#[test]
	fn test_try_get_i32() {
		let schema = Schema::testing(&[Type::Int4]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_i32(&row, 0), None);

		schema.set_i32(&mut row, 0, 56789i32);
		assert_eq!(schema.try_get_i32(&row, 0), Some(56789i32));
	}

	#[test]
	fn test_extremes() {
		let schema = Schema::testing(&[Type::Int4]);
		let mut row = schema.allocate();

		schema.set_i32(&mut row, 0, i32::MAX);
		assert_eq!(schema.get_i32(&row, 0), i32::MAX);

		let mut row2 = schema.allocate();
		schema.set_i32(&mut row2, 0, i32::MIN);
		assert_eq!(schema.get_i32(&row2, 0), i32::MIN);

		let mut row3 = schema.allocate();
		schema.set_i32(&mut row3, 0, 0i32);
		assert_eq!(schema.get_i32(&row3, 0), 0i32);
	}

	#[test]
	fn test_large_values() {
		let schema = Schema::testing(&[Type::Int4]);

		let test_values =
			[-2_147_483_648i32, -1_000_000_000i32, -1i32, 0i32, 1i32, 1_000_000_000i32, 2_147_483_647i32];

		for value in test_values {
			let mut row = schema.allocate();
			schema.set_i32(&mut row, 0, value);
			assert_eq!(schema.get_i32(&row, 0), value);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Int4, Type::Boolean, Type::Int4, Type::Float4]);
		let mut row = schema.allocate();

		schema.set_i32(&mut row, 0, -1_000_000i32);
		schema.set_bool(&mut row, 1, true);
		schema.set_i32(&mut row, 2, 2_000_000i32);
		schema.set_f32(&mut row, 3, 3.14f32);

		assert_eq!(schema.get_i32(&row, 0), -1_000_000i32);
		assert_eq!(schema.get_bool(&row, 1), true);
		assert_eq!(schema.get_i32(&row, 2), 2_000_000i32);
		assert_eq!(schema.get_f32(&row, 3), 3.14f32);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Int4, Type::Int4]);
		let mut row = schema.allocate();

		schema.set_i32(&mut row, 0, 12345);

		assert_eq!(schema.try_get_i32(&row, 0), Some(12345));
		assert_eq!(schema.try_get_i32(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_i32(&row, 0), None);
	}

	#[test]
	fn test_try_get_i32_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_i32(&row, 0), None);
	}
}
