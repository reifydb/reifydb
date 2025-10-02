// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::Type;

use crate::value::encoded::{EncodedValues, EncodedValuesLayout};

impl EncodedValuesLayout {
	pub fn set_u64(&self, row: &mut EncodedValues, index: usize, value: impl Into<u64>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Uint8);
		row.set_valid(index, true);
		unsafe { ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut u64, value.into()) }
	}

	pub fn get_u64(&self, row: &EncodedValues, index: usize) -> u64 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Uint8);
		unsafe { (row.as_ptr().add(field.offset) as *const u64).read_unaligned() }
	}

	pub fn try_get_u64(&self, row: &EncodedValues, index: usize) -> Option<u64> {
		if row.is_defined(index) {
			Some(self.get_u64(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use crate::value::encoded::EncodedValuesLayout;

	#[test]
	fn test_set_get_u64() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);
		let mut row = layout.allocate();
		layout.set_u64(&mut row, 0, 18446744073709551615u64);
		assert_eq!(layout.get_u64(&row, 0), 18446744073709551615u64);
	}

	#[test]
	fn test_try_get_u64() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);
		let mut row = layout.allocate();

		assert_eq!(layout.try_get_u64(&row, 0), None);

		layout.set_u64(&mut row, 0, 18446744073709551615u64);
		assert_eq!(layout.try_get_u64(&row, 0), Some(18446744073709551615u64));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);
		let mut row = layout.allocate();

		layout.set_u64(&mut row, 0, u64::MAX);
		assert_eq!(layout.get_u64(&row, 0), u64::MAX);

		let mut row2 = layout.allocate();
		layout.set_u64(&mut row2, 0, u64::MIN);
		assert_eq!(layout.get_u64(&row2, 0), u64::MIN);

		let mut row3 = layout.allocate();
		layout.set_u64(&mut row3, 0, 0u64);
		assert_eq!(layout.get_u64(&row3, 0), 0u64);
	}

	#[test]
	fn test_large_values() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);

		let test_values = [
			0u64,
			1u64,
			1_000_000_000u64,
			1_000_000_000_000_000_000u64,
			9_223_372_036_854_775_807u64, // i64::MAX
			9_223_372_036_854_775_808u64, // i64::MAX + 1
			18_000_000_000_000_000_000u64,
			18_446_744_073_709_551_614u64,
			18_446_744_073_709_551_615u64, // u64::MAX
		];

		for value in test_values {
			let mut row = layout.allocate();
			layout.set_u64(&mut row, 0, value);
			assert_eq!(layout.get_u64(&row, 0), value);
		}
	}

	#[test]
	fn test_memory_sizes() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);

		// Test values representing memory sizes in bytes
		let memory_sizes = [
			1024u64,                // 1 KB
			1048576u64,             // 1 MB
			1073741824u64,          // 1 GB
			1099511627776u64,       // 1 TB
			1125899906842624u64,    // 1 PB
			1152921504606846976u64, // 1 EB
		];

		for size in memory_sizes {
			let mut row = layout.allocate();
			layout.set_u64(&mut row, 0, size);
			assert_eq!(layout.get_u64(&row, 0), size);
		}
	}

	#[test]
	fn test_nanosecond_timestamps() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);

		// Test nanosecond precision timestamps
		let ns_timestamps = [
			0u64,                   // Unix epoch in ns
			946684800000000000u64,  // 2000-01-01 in ns
			1640995200000000000u64, // 2022-01-01 in ns
			1735689600000000000u64, // 2025-01-01 in ns
		];

		for timestamp in ns_timestamps {
			let mut row = layout.allocate();
			layout.set_u64(&mut row, 0, timestamp);
			assert_eq!(layout.get_u64(&row, 0), timestamp);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8, Type::Float8, Type::Uint8]);
		let mut row = layout.allocate();

		layout.set_u64(&mut row, 0, 15_000_000_000_000_000_000u64);
		layout.set_f64(&mut row, 1, 3.14159265359);
		layout.set_u64(&mut row, 2, 12_000_000_000_000_000_000u64);

		assert_eq!(layout.get_u64(&row, 0), 15_000_000_000_000_000_000u64);
		assert_eq!(layout.get_f64(&row, 1), 3.14159265359);
		assert_eq!(layout.get_u64(&row, 2), 12_000_000_000_000_000_000u64);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedValuesLayout::new(&[Type::Uint8, Type::Uint8]);
		let mut row = layout.allocate();

		layout.set_u64(&mut row, 0, 1234567890123456789u64);

		assert_eq!(layout.try_get_u64(&row, 0), Some(1234567890123456789));
		assert_eq!(layout.try_get_u64(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_u64(&row, 0), None);
	}
}
