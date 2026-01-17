// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::{
	encoded::{encoded::EncodedValues, layout::EncodedValuesLayout},
	schema::Schema,
};

impl EncodedValuesLayout {
	pub fn set_i16(&self, row: &mut EncodedValues, index: usize, value: impl Into<i16>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Int2);
		row.set_valid(index, true);
		unsafe { ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut i16, value.into()) }
	}

	pub fn get_i16(&self, row: &EncodedValues, index: usize) -> i16 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Int2);
		unsafe { (row.as_ptr().add(field.offset) as *const i16).read_unaligned() }
	}

	pub fn try_get_i16(&self, row: &EncodedValues, index: usize) -> Option<i16> {
		if row.is_defined(index) && self.fields[index].r#type == Type::Int2 {
			Some(self.get_i16(row, index))
		} else {
			None
		}
	}
}

impl Schema {
	pub fn set_i16(&self, row: &mut EncodedValues, index: usize, value: impl Into<i16>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::Int2);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut i16,
				value.into(),
			)
		}
	}

	pub fn get_i16(&self, row: &EncodedValues, index: usize) -> i16 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::Int2);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const i16).read_unaligned() }
	}

	pub fn try_get_i16(&self, row: &EncodedValues, index: usize) -> Option<i16> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Int2 {
			Some(self.get_i16(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use crate::schema::Schema;

	#[test]
	fn test_set_get_i16() {
		let schema = Schema::testing(&[Type::Int2]);
		let mut row = schema.allocate();
		schema.set_i16(&mut row, 0, -1234i16);
		assert_eq!(schema.get_i16(&row, 0), -1234i16);
	}

	#[test]
	fn test_try_get_i16() {
		let schema = Schema::testing(&[Type::Int2]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_i16(&row, 0), None);

		schema.set_i16(&mut row, 0, -1234i16);
		assert_eq!(schema.try_get_i16(&row, 0), Some(-1234i16));
	}

	#[test]
	fn test_extremes() {
		let schema = Schema::testing(&[Type::Int2]);
		let mut row = schema.allocate();

		schema.set_i16(&mut row, 0, i16::MAX);
		assert_eq!(schema.get_i16(&row, 0), i16::MAX);

		let mut row2 = schema.allocate();
		schema.set_i16(&mut row2, 0, i16::MIN);
		assert_eq!(schema.get_i16(&row2, 0), i16::MIN);

		let mut row3 = schema.allocate();
		schema.set_i16(&mut row3, 0, 0i16);
		assert_eq!(schema.get_i16(&row3, 0), 0i16);
	}

	#[test]
	fn test_various_values() {
		let schema = Schema::testing(&[Type::Int2]);

		let test_values = [-32768i16, -10000i16, -1i16, 0i16, 1i16, 10000i16, 32767i16];

		for value in test_values {
			let mut row = schema.allocate();
			schema.set_i16(&mut row, 0, value);
			assert_eq!(schema.get_i16(&row, 0), value);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Int2, Type::Int1, Type::Int2]);
		let mut row = schema.allocate();

		schema.set_i16(&mut row, 0, -30000i16);
		schema.set_i8(&mut row, 1, 100i8);
		schema.set_i16(&mut row, 2, 25000i16);

		assert_eq!(schema.get_i16(&row, 0), -30000i16);
		assert_eq!(schema.get_i8(&row, 1), 100i8);
		assert_eq!(schema.get_i16(&row, 2), 25000i16);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Int2, Type::Int2]);
		let mut row = schema.allocate();

		schema.set_i16(&mut row, 0, 1234i16);

		assert_eq!(schema.try_get_i16(&row, 0), Some(1234));
		assert_eq!(schema.try_get_i16(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_i16(&row, 0), None);
	}

	#[test]
	fn test_try_get_i16_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_i16(&row, 0), None);
	}
}
