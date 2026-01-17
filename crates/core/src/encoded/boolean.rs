// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::{
	encoded::{encoded::EncodedValues, layout::EncodedValuesLayout},
	schema::Schema,
};

impl EncodedValuesLayout {
	pub fn set_bool(&self, row: &mut EncodedValues, index: usize, value: impl Into<bool>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Boolean);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut bool, value.into())
		}
	}

	pub fn get_bool(&self, row: &EncodedValues, index: usize) -> bool {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Boolean);
		unsafe { (row.as_ptr().add(field.offset) as *const bool).read_unaligned() }
	}

	pub fn try_get_bool(&self, row: &EncodedValues, index: usize) -> Option<bool> {
		if row.is_defined(index) && self.fields[index].r#type == Type::Boolean {
			Some(self.get_bool(row, index))
		} else {
			None
		}
	}
}

impl Schema {
	pub fn set_bool(&self, row: &mut EncodedValues, index: usize, value: impl Into<bool>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::Boolean);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut bool,
				value.into(),
			)
		}
	}

	pub fn get_bool(&self, row: &EncodedValues, index: usize) -> bool {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::Boolean);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const bool).read_unaligned() }
	}

	pub fn try_get_bool(&self, row: &EncodedValues, index: usize) -> Option<bool> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Boolean {
			Some(self.get_bool(row, index))
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
	fn test_set_get_bool() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();
		schema.set_bool(&mut row, 0, true);
		assert!(schema.get_bool(&row, 0));
	}

	#[test]
	fn test_try_get_bool() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_bool(&row, 0), None);

		schema.set_bool(&mut row, 0, true);
		assert_eq!(schema.try_get_bool(&row, 0), Some(true));
	}

	#[test]
	fn test_false() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();
		schema.set_bool(&mut row, 0, false);
		assert!(!schema.get_bool(&row, 0));
		assert_eq!(schema.try_get_bool(&row, 0), Some(false));
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Boolean, Type::Int4, Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);
		schema.set_i32(&mut row, 1, 42);
		schema.set_bool(&mut row, 2, false);

		assert_eq!(schema.get_bool(&row, 0), true);
		assert_eq!(schema.get_i32(&row, 1), 42);
		assert_eq!(schema.get_bool(&row, 2), false);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Boolean, Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_bool(&row, 0), Some(true));
		assert_eq!(schema.try_get_bool(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_bool(&row, 0), None);
	}

	#[test]
	fn test_try_get_bool_wrong_type() {
		let schema = Schema::testing(&[Type::Int1]);
		let mut row = schema.allocate();

		schema.set_i8(&mut row, 0, 42);

		assert_eq!(schema.try_get_bool(&row, 0), None);
	}
}
