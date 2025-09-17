// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::{Time, Type};

use crate::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_time(&self, row: &mut EncodedRow, index: usize, value: Time) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Time);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset) as *mut u64,
				value.to_nanos_since_midnight(),
			)
		}
	}

	pub fn get_time(&self, row: &EncodedRow, index: usize) -> Time {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Time);
		unsafe {
			Time::from_nanos_since_midnight((row.as_ptr().add(field.offset) as *const u64).read_unaligned())
				.unwrap()
		}
	}

	pub fn try_get_time(&self, row: &EncodedRow, index: usize) -> Option<Time> {
		if row.is_defined(index) {
			Some(self.get_time(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::{Time, Type};

	use crate::row::EncodedRowLayout;

	#[test]
	fn test_set_get_time() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		let value = Time::new(20, 50, 0, 0).unwrap();
		layout.set_time(&mut row, 0, value.clone());
		assert_eq!(layout.get_time(&row, 0), value);
	}

	#[test]
	fn test_try_get_time() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_time(&row, 0), None);

		let test_time = Time::from_hms(14, 30, 45).unwrap();
		layout.set_time(&mut row, 0, test_time.clone());
		assert_eq!(layout.try_get_time(&row, 0), Some(test_time));
	}

	#[test]
	fn test_time_midnight() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		let midnight = Time::default(); // 00:00:00
		layout.set_time(&mut row, 0, midnight.clone());
		assert_eq!(layout.get_time(&row, 0), midnight);
	}

	#[test]
	fn test_time_with_nanoseconds() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		// Test with high precision nanoseconds
		let precise_time = Time::new(15, 30, 45, 123456789).unwrap();
		layout.set_time(&mut row, 0, precise_time.clone());
		assert_eq!(layout.get_time(&row, 0), precise_time);
	}

	#[test]
	fn test_time_various_times() {
		let layout = EncodedRowLayout::new(&[Type::Time]);

		let test_times = [
			Time::new(0, 0, 0, 0).unwrap(),            // Midnight
			Time::new(12, 0, 0, 0).unwrap(),           // Noon
			Time::new(23, 59, 59, 999999999).unwrap(), // Just before midnight
			Time::new(6, 30, 15, 500000000).unwrap(),  // Morning time
			Time::new(18, 45, 30, 750000000).unwrap(), // Evening time
		];

		for time in test_times {
			let mut row = layout.allocate_row();
			layout.set_time(&mut row, 0, time.clone());
			assert_eq!(layout.get_time(&row, 0), time);
		}
	}

	#[test]
	fn test_time_boundary_cases() {
		let layout = EncodedRowLayout::new(&[Type::Time]);

		let boundary_times = [
			Time::new(0, 0, 0, 0).unwrap(), // Start of day
			Time::new(0, 0, 0, 1).unwrap(), /* One nanosecond
			                                 * after midnight */
			Time::new(23, 59, 59, 999999998).unwrap(), // One nanosecond before midnight
			Time::new(23, 59, 59, 999999999).unwrap(), // Last nanosecond of day
		];

		for time in boundary_times {
			let mut row = layout.allocate_row();
			layout.set_time(&mut row, 0, time.clone());
			assert_eq!(layout.get_time(&row, 0), time);
		}
	}

	#[test]
	fn test_time_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[Type::Time, Type::Boolean, Type::Time, Type::Int4]);
		let mut row = layout.allocate_row();

		let time1 = Time::new(9, 15, 30, 0).unwrap();
		let time2 = Time::new(21, 45, 0, 250000000).unwrap();

		layout.set_time(&mut row, 0, time1.clone());
		layout.set_bool(&mut row, 1, false);
		layout.set_time(&mut row, 2, time2.clone());
		layout.set_i32(&mut row, 3, -999);

		assert_eq!(layout.get_time(&row, 0), time1);
		assert_eq!(layout.get_bool(&row, 1), false);
		assert_eq!(layout.get_time(&row, 2), time2);
		assert_eq!(layout.get_i32(&row, 3), -999);
	}

	#[test]
	fn test_time_undefined_handling() {
		let layout = EncodedRowLayout::new(&[Type::Time, Type::Time]);
		let mut row = layout.allocate_row();

		let time = Time::new(16, 20, 45, 333000000).unwrap();
		layout.set_time(&mut row, 0, time.clone());

		assert_eq!(layout.try_get_time(&row, 0), Some(time));
		assert_eq!(layout.try_get_time(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_time(&row, 0), None);
	}

	#[test]
	fn test_time_precision_preservation() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		// Test that nanosecond precision is preserved
		let high_precision = Time::new(12, 34, 56, 987654321).unwrap();
		layout.set_time(&mut row, 0, high_precision.clone());

		let retrieved = layout.get_time(&row, 0);
		assert_eq!(retrieved, high_precision);
		assert_eq!(retrieved.to_nanos_since_midnight(), high_precision.to_nanos_since_midnight());
	}

	#[test]
	fn test_time_microsecond_precision() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		// Test microsecond precision (common in databases)
		let microsecond_precision = Time::new(14, 25, 30, 123456000).unwrap();
		layout.set_time(&mut row, 0, microsecond_precision.clone());
		assert_eq!(layout.get_time(&row, 0), microsecond_precision);
	}

	#[test]
	fn test_time_millisecond_precision() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		// Test millisecond precision
		let millisecond_precision = Time::new(8, 15, 42, 123000000).unwrap();
		layout.set_time(&mut row, 0, millisecond_precision.clone());
		assert_eq!(layout.get_time(&row, 0), millisecond_precision);
	}

	#[test]
	fn test_time_common_times() {
		let layout = EncodedRowLayout::new(&[Type::Time]);

		// Test common business/system times
		let common_times = [
			Time::new(9, 0, 0, 0).unwrap(),   // 9 AM start of work
			Time::new(12, 0, 0, 0).unwrap(),  // Noon
			Time::new(17, 0, 0, 0).unwrap(),  // 5 PM end of work
			Time::new(0, 0, 1, 0).unwrap(),   // 1 second after midnight
			Time::new(23, 59, 0, 0).unwrap(), // 1 minute before midnight
		];

		for time in common_times {
			let mut row = layout.allocate_row();
			layout.set_time(&mut row, 0, time.clone());
			assert_eq!(layout.get_time(&row, 0), time);
		}
	}
}
