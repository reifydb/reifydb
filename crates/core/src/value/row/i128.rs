// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::Type;

use crate::value::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_i128(&self, row: &mut EncodedRow, index: usize, value: impl Into<i128>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int16);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut i128, value.into())
		}
	}

	pub fn get_i128(&self, row: &EncodedRow, index: usize) -> i128 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int16);
		unsafe { (row.as_ptr().add(field.offset) as *const i128).read_unaligned() }
	}

	pub fn try_get_i128(&self, row: &EncodedRow, index: usize) -> Option<i128> {
		if row.is_defined(index) {
			Some(self.get_i128(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use crate::value::row::EncodedRowLayout;

	#[test]
	fn test_set_get_i128() {
		let layout = EncodedRowLayout::new(&[Type::Int16]);
		let mut row = layout.allocate_row();
		layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
		assert_eq!(layout.get_i128(&row, 0), 123456789012345678901234567890i128);
	}

	#[test]
	fn test_try_get_i128() {
		let layout = EncodedRowLayout::new(&[Type::Int16]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_i128(&row, 0), None);

		layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
		assert_eq!(layout.try_get_i128(&row, 0), Some(123456789012345678901234567890i128));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedRowLayout::new(&[Type::Int16]);
		let mut row = layout.allocate_row();

		layout.set_i128(&mut row, 0, i128::MAX);
		assert_eq!(layout.get_i128(&row, 0), i128::MAX);

		let mut row2 = layout.allocate_row();
		layout.set_i128(&mut row2, 0, i128::MIN);
		assert_eq!(layout.get_i128(&row2, 0), i128::MIN);

		let mut row3 = layout.allocate_row();
		layout.set_i128(&mut row3, 0, 0i128);
		assert_eq!(layout.get_i128(&row3, 0), 0i128);
	}

	#[test]
	fn test_very_large_values() {
		let layout = EncodedRowLayout::new(&[Type::Int16]);

		let test_values = [
			-170141183460469231731687303715884105728i128, // i128::MIN
			-99999999999999999999999999999999999999i128,
			-1i128,
			0i128,
			1i128,
			99999999999999999999999999999999999999i128,
			170141183460469231731687303715884105727i128, // i128::MAX
		];

		for value in test_values {
			let mut row = layout.allocate_row();
			layout.set_i128(&mut row, 0, value);
			assert_eq!(layout.get_i128(&row, 0), value);
		}
	}

	#[test]
	fn test_powers_of_ten() {
		let layout = EncodedRowLayout::new(&[Type::Int16]);

		let powers = [
			1i128,
			10i128,
			100i128,
			1_000i128,
			10_000i128,
			100_000i128,
			1_000_000i128,
			10_000_000i128,
			100_000_000i128,
			1_000_000_000i128,
			10_000_000_000i128,
			100_000_000_000i128,
		];

		for power in powers {
			let mut row = layout.allocate_row();
			layout.set_i128(&mut row, 0, power);
			assert_eq!(layout.get_i128(&row, 0), power);

			let mut row2 = layout.allocate_row();
			layout.set_i128(&mut row2, 0, -power);
			assert_eq!(layout.get_i128(&row2, 0), -power);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[Type::Int16, Type::Boolean, Type::Int16]);
		let mut row = layout.allocate_row();

		let large_negative = -12345678901234567890123456789012345i128;
		let large_positive = 98765432109876543210987654321098765i128;

		layout.set_i128(&mut row, 0, large_negative);
		layout.set_bool(&mut row, 1, true);
		layout.set_i128(&mut row, 2, large_positive);

		assert_eq!(layout.get_i128(&row, 0), large_negative);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_i128(&row, 2), large_positive);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedRowLayout::new(&[Type::Int16, Type::Int16]);
		let mut row = layout.allocate_row();

		let value = 170141183460469231731687303715884105727i128; // Max i128
		layout.set_i128(&mut row, 0, value);

		assert_eq!(layout.try_get_i128(&row, 0), Some(value));
		assert_eq!(layout.try_get_i128(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_i128(&row, 0), None);
	}
}
