// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{f64, ptr};

use reifydb_type::value::r#type::Type;

use crate::encoded::{encoded::EncodedValues, schema::Schema};

impl Schema {
	pub fn set_f64(&self, row: &mut EncodedValues, index: usize, value: impl Into<f64>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Float8);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut f64,
				value.into(),
			)
		}
	}

	pub fn get_f64(&self, row: &EncodedValues, index: usize) -> f64 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Float8);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const f64).read_unaligned() }
	}

	pub fn try_get_f64(&self, row: &EncodedValues, index: usize) -> Option<f64> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Float8 {
			Some(self.get_f64(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use std::f64::consts::{E, PI};

	use reifydb_type::value::r#type::Type;

	use crate::encoded::schema::Schema;

	#[test]
	fn test_set_get_f64() {
		let schema = Schema::testing(&[Type::Float8]);
		let mut row = schema.allocate();
		schema.set_f64(&mut row, 0, 2.5f64);
		assert_eq!(schema.get_f64(&row, 0), 2.5f64);
	}

	#[test]
	fn test_try_get_f64() {
		let schema = Schema::testing(&[Type::Float8]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_f64(&row, 0), None);

		schema.set_f64(&mut row, 0, 2.5f64);
		assert_eq!(schema.try_get_f64(&row, 0), Some(2.5f64));
	}

	#[test]
	fn test_special_values() {
		let schema = Schema::testing(&[Type::Float8]);
		let mut row = schema.allocate();

		// Test zero
		schema.set_f64(&mut row, 0, 0.0f64);
		assert_eq!(schema.get_f64(&row, 0), 0.0f64);

		// Test negative zero
		let mut row2 = schema.allocate();
		schema.set_f64(&mut row2, 0, -0.0f64);
		assert_eq!(schema.get_f64(&row2, 0), -0.0f64);

		// Test infinity
		let mut row3 = schema.allocate();
		schema.set_f64(&mut row3, 0, f64::INFINITY);
		assert_eq!(schema.get_f64(&row3, 0), f64::INFINITY);

		// Test negative infinity
		let mut row4 = schema.allocate();
		schema.set_f64(&mut row4, 0, f64::NEG_INFINITY);
		assert_eq!(schema.get_f64(&row4, 0), f64::NEG_INFINITY);

		// Test NaN
		let mut row5 = schema.allocate();
		schema.set_f64(&mut row5, 0, f64::NAN);
		assert!(schema.get_f64(&row5, 0).is_nan());
	}

	#[test]
	fn test_extreme_values() {
		let schema = Schema::testing(&[Type::Float8]);
		let mut row = schema.allocate();

		schema.set_f64(&mut row, 0, f64::MAX);
		assert_eq!(schema.get_f64(&row, 0), f64::MAX);

		let mut row2 = schema.allocate();
		schema.set_f64(&mut row2, 0, f64::MIN);
		assert_eq!(schema.get_f64(&row2, 0), f64::MIN);

		let mut row3 = schema.allocate();
		schema.set_f64(&mut row3, 0, f64::MIN_POSITIVE);
		assert_eq!(schema.get_f64(&row3, 0), f64::MIN_POSITIVE);
	}

	#[test]
	fn test_high_precision() {
		let schema = Schema::testing(&[Type::Float8]);
		let mut row = schema.allocate();

		let pi = PI;
		schema.set_f64(&mut row, 0, pi);
		assert_eq!(schema.get_f64(&row, 0), pi);

		let mut row2 = schema.allocate();
		let e = E;
		schema.set_f64(&mut row2, 0, e);
		assert_eq!(schema.get_f64(&row2, 0), e);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Float8, Type::Int8, Type::Float8]);
		let mut row = schema.allocate();

		schema.set_f64(&mut row, 0, 3.14159265359);
		schema.set_i64(&mut row, 1, 9223372036854775807i64);
		schema.set_f64(&mut row, 2, -2.718281828459045);

		assert_eq!(schema.get_f64(&row, 0), 3.14159265359);
		assert_eq!(schema.get_i64(&row, 1), 9223372036854775807);
		assert_eq!(schema.get_f64(&row, 2), -2.718281828459045);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Float8, Type::Float8]);
		let mut row = schema.allocate();

		schema.set_f64(&mut row, 0, 2.718281828459045);

		assert_eq!(schema.try_get_f64(&row, 0), Some(2.718281828459045));
		assert_eq!(schema.try_get_f64(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_f64(&row, 0), None);
	}

	#[test]
	fn test_try_get_f64_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_f64(&row, 0), None);
	}
}
