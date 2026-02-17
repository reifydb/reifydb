// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;

use crate::encoded::{encoded::EncodedValues, schema::Schema};

impl Schema {
	pub fn set_u8(&self, row: &mut EncodedValues, index: usize, value: impl Into<u8>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uint1);
		row.set_valid(index, true);
		unsafe {
			row.make_mut().as_mut_ptr().add(field.offset as usize).write_unaligned(value.into());
		}
	}

	pub fn get_u8(&self, row: &EncodedValues, index: usize) -> u8 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uint1);
		unsafe { row.as_ptr().add(field.offset as usize).read_unaligned() }
	}

	pub fn try_get_u8(&self, row: &EncodedValues, index: usize) -> Option<u8> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Uint1 {
			Some(self.get_u8(row, index))
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
	fn test_set_get_u8() {
		let schema = Schema::testing(&[Type::Uint1]);
		let mut row = schema.allocate();
		schema.set_u8(&mut row, 0, 255u8);
		assert_eq!(schema.get_u8(&row, 0), 255u8);
	}

	#[test]
	fn test_try_get_u8() {
		let schema = Schema::testing(&[Type::Uint1]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_u8(&row, 0), None);

		schema.set_u8(&mut row, 0, 255u8);
		assert_eq!(schema.try_get_u8(&row, 0), Some(255u8));
	}

	#[test]
	fn test_extremes() {
		let schema = Schema::testing(&[Type::Uint1]);
		let mut row = schema.allocate();

		schema.set_u8(&mut row, 0, u8::MAX);
		assert_eq!(schema.get_u8(&row, 0), u8::MAX);

		let mut row2 = schema.allocate();
		schema.set_u8(&mut row2, 0, u8::MIN);
		assert_eq!(schema.get_u8(&row2, 0), u8::MIN);

		let mut row3 = schema.allocate();
		schema.set_u8(&mut row3, 0, 0u8);
		assert_eq!(schema.get_u8(&row3, 0), 0u8);
	}

	#[test]
	fn test_various_values() {
		let schema = Schema::testing(&[Type::Uint1]);

		let test_values = [0u8, 1u8, 127u8, 128u8, 254u8, 255u8];

		for value in test_values {
			let mut row = schema.allocate();
			schema.set_u8(&mut row, 0, value);
			assert_eq!(schema.get_u8(&row, 0), value);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Uint1, Type::Boolean, Type::Uint1]);
		let mut row = schema.allocate();

		schema.set_u8(&mut row, 0, 200u8);
		schema.set_bool(&mut row, 1, true);
		schema.set_u8(&mut row, 2, 100u8);

		assert_eq!(schema.get_u8(&row, 0), 200u8);
		assert_eq!(schema.get_bool(&row, 1), true);
		assert_eq!(schema.get_u8(&row, 2), 100u8);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Uint1, Type::Uint1]);
		let mut row = schema.allocate();

		schema.set_u8(&mut row, 0, 42);

		assert_eq!(schema.try_get_u8(&row, 0), Some(42));
		assert_eq!(schema.try_get_u8(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_u8(&row, 0), None);
	}

	#[test]
	fn test_try_get_u8_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_u8(&row, 0), None);
	}
}
