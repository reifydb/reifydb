// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::Type;

use crate::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_f64(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<f64>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Float8);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut f64,
				value.into(),
			)
		}
	}

	pub fn get_f64(&self, row: &EncodedRow, index: usize) -> f64 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Float8);
		unsafe {
			(row.as_ptr().add(field.offset) as *const f64)
				.read_unaligned()
		}
	}

	pub fn try_get_f64(
		&self,
		row: &EncodedRow,
		index: usize,
	) -> Option<f64> {
		if row.is_defined(index) {
			Some(self.get_f64(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
	use reifydb_type::Type;

	use crate::row::EncodedRowLayout;

	#[test]
	fn test_set_get_f64() {
		let layout = EncodedRowLayout::new(&[Type::Float8]);
		let mut row = layout.allocate_row();
		layout.set_f64(&mut row, 0, 2.5f64);
		assert_eq!(layout.get_f64(&row, 0), 2.5f64);
	}

	#[test]
	fn test_try_get_f64() {
		let layout = EncodedRowLayout::new(&[Type::Float8]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_f64(&row, 0), None);

		layout.set_f64(&mut row, 0, 2.5f64);
		assert_eq!(layout.try_get_f64(&row, 0), Some(2.5f64));
	}

	#[test]
	fn test_special_values() {
		let layout = EncodedRowLayout::new(&[Type::Float8]);
		let mut row = layout.allocate_row();

		// Test zero
		layout.set_f64(&mut row, 0, 0.0f64);
		assert_eq!(layout.get_f64(&row, 0), 0.0f64);

		// Test negative zero
		let mut row2 = layout.allocate_row();
		layout.set_f64(&mut row2, 0, -0.0f64);
		assert_eq!(layout.get_f64(&row2, 0), -0.0f64);

		// Test infinity
		let mut row3 = layout.allocate_row();
		layout.set_f64(&mut row3, 0, f64::INFINITY);
		assert_eq!(layout.get_f64(&row3, 0), f64::INFINITY);

		// Test negative infinity
		let mut row4 = layout.allocate_row();
		layout.set_f64(&mut row4, 0, f64::NEG_INFINITY);
		assert_eq!(layout.get_f64(&row4, 0), f64::NEG_INFINITY);

		// Test NaN
		let mut row5 = layout.allocate_row();
		layout.set_f64(&mut row5, 0, f64::NAN);
		assert!(layout.get_f64(&row5, 0).is_nan());
	}

	#[test]
	fn test_extreme_values() {
		let layout = EncodedRowLayout::new(&[Type::Float8]);
		let mut row = layout.allocate_row();

		layout.set_f64(&mut row, 0, f64::MAX);
		assert_eq!(layout.get_f64(&row, 0), f64::MAX);

		let mut row2 = layout.allocate_row();
		layout.set_f64(&mut row2, 0, f64::MIN);
		assert_eq!(layout.get_f64(&row2, 0), f64::MIN);

		let mut row3 = layout.allocate_row();
		layout.set_f64(&mut row3, 0, f64::MIN_POSITIVE);
		assert_eq!(layout.get_f64(&row3, 0), f64::MIN_POSITIVE);
	}

	#[test]
	fn test_high_precision() {
		let layout = EncodedRowLayout::new(&[Type::Float8]);
		let mut row = layout.allocate_row();

		let pi = std::f64::consts::PI;
		layout.set_f64(&mut row, 0, pi);
		assert_eq!(layout.get_f64(&row, 0), pi);

		let mut row2 = layout.allocate_row();
		let e = std::f64::consts::E;
		layout.set_f64(&mut row2, 0, e);
		assert_eq!(layout.get_f64(&row2, 0), e);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[
			Type::Float8,
			Type::Int8,
			Type::Float8,
		]);
		let mut row = layout.allocate_row();

		layout.set_f64(&mut row, 0, 3.14159265359);
		layout.set_i64(&mut row, 1, 9223372036854775807i64);
		layout.set_f64(&mut row, 2, -2.718281828459045);

		assert_eq!(layout.get_f64(&row, 0), 3.14159265359);
		assert_eq!(layout.get_i64(&row, 1), 9223372036854775807);
		assert_eq!(layout.get_f64(&row, 2), -2.718281828459045);
	}

	#[test]
	fn test_undefined_handling() {
		let layout =
			EncodedRowLayout::new(&[Type::Float8, Type::Float8]);
		let mut row = layout.allocate_row();

		layout.set_f64(&mut row, 0, 2.718281828459045);

		assert_eq!(
			layout.try_get_f64(&row, 0),
			Some(2.718281828459045)
		);
		assert_eq!(layout.try_get_f64(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_f64(&row, 0), None);
	}
}
