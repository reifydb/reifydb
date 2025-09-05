// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::{Interval, Type};

use crate::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_interval(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: Interval,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Interval);
		row.set_valid(index, true);

		let months = value.get_months();
		let days = value.get_days();
		let nanos = value.get_nanos();
		unsafe {
			// Write months (i32) at offset
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i32,
				months,
			);
			// Write days (i32) at offset + 4
			ptr::write_unaligned(
				row.make_mut()
					.as_mut_ptr()
					.add(field.offset + 4) as *mut i32,
				days,
			);
			// Write nanos (i64) at offset + 8
			ptr::write_unaligned(
				row.make_mut()
					.as_mut_ptr()
					.add(field.offset + 8) as *mut i64,
				nanos,
			);
		}
	}

	pub fn get_interval(&self, row: &EncodedRow, index: usize) -> Interval {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Interval);
		unsafe {
			// Read months (i32) from offset
			let months = (row.as_ptr().add(field.offset)
				as *const i32)
				.read_unaligned();
			// Read days (i32) from offset + 4
			let days = (row.as_ptr().add(field.offset + 4)
				as *const i32)
				.read_unaligned();
			// Read nanos (i64) from offset + 8
			let nanos = (row.as_ptr().add(field.offset + 8)
				as *const i64)
				.read_unaligned();
			Interval::new(months, days, nanos)
		}
	}

	pub fn try_get_interval(
		&self,
		row: &EncodedRow,
		index: usize,
	) -> Option<Interval> {
		if row.is_defined(index) {
			Some(self.get_interval(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::{Interval, Type};

	use crate::row::EncodedRowLayout;

	#[test]
	fn test_set_get_interval() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);
		let mut row = layout.allocate_row();

		let value = Interval::from_seconds(-7200);
		layout.set_interval(&mut row, 0, value.clone());
		assert_eq!(layout.get_interval(&row, 0), value);
	}

	#[test]
	fn test_try_get_interval() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_interval(&row, 0), None);

		let test_interval = Interval::from_days(30);
		layout.set_interval(&mut row, 0, test_interval.clone());
		assert_eq!(
			layout.try_get_interval(&row, 0),
			Some(test_interval)
		);
	}

	#[test]
	fn test_zero() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);
		let mut row = layout.allocate_row();

		let zero = Interval::default(); // Zero interval
		layout.set_interval(&mut row, 0, zero.clone());
		assert_eq!(layout.get_interval(&row, 0), zero);
	}

	#[test]
	fn test_various_durations() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);

		let test_intervals = [
			Interval::from_seconds(0),     // Zero
			Interval::from_seconds(60),    // 1 minute
			Interval::from_seconds(3600),  // 1 hour
			Interval::from_seconds(86400), // 1 day
			Interval::from_days(7),        // 1 week
			Interval::from_days(30),       // ~1 month
			Interval::from_weeks(52),      // ~1 year
		];

		for interval in test_intervals {
			let mut row = layout.allocate_row();
			layout.set_interval(&mut row, 0, interval.clone());
			assert_eq!(layout.get_interval(&row, 0), interval);
		}
	}

	#[test]
	fn test_negative_durations() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);

		let negative_intervals = [
			Interval::from_seconds(-60),    // -1 minute
			Interval::from_seconds(-3600),  // -1 hour
			Interval::from_seconds(-86400), // -1 day
			Interval::from_days(-7),        // -1 week
			Interval::from_weeks(-4),       // -1 month
		];

		for interval in negative_intervals {
			let mut row = layout.allocate_row();
			layout.set_interval(&mut row, 0, interval.clone());
			assert_eq!(layout.get_interval(&row, 0), interval);
		}
	}

	#[test]
	fn test_complex_parts() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);
		let mut row = layout.allocate_row();

		// Create an interval with all components
		let complex_interval = Interval::new(
			6,         // 6 months
			15,        // 15 days
			123456789, // nanoseconds
		);
		layout.set_interval(&mut row, 0, complex_interval.clone());
		assert_eq!(layout.get_interval(&row, 0), complex_interval);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[
			Type::Interval,
			Type::Boolean,
			Type::Interval,
			Type::Int8,
		]);
		let mut row = layout.allocate_row();

		let interval1 = Interval::from_hours(24);
		let interval2 = Interval::from_minutes(-30);

		layout.set_interval(&mut row, 0, interval1.clone());
		layout.set_bool(&mut row, 1, true);
		layout.set_interval(&mut row, 2, interval2.clone());
		layout.set_i64(&mut row, 3, 987654321);

		assert_eq!(layout.get_interval(&row, 0), interval1);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_interval(&row, 2), interval2);
		assert_eq!(layout.get_i64(&row, 3), 987654321);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedRowLayout::new(&[
			Type::Interval,
			Type::Interval,
		]);
		let mut row = layout.allocate_row();

		let interval = Interval::from_days(100);
		layout.set_interval(&mut row, 0, interval.clone());

		assert_eq!(layout.try_get_interval(&row, 0), Some(interval));
		assert_eq!(layout.try_get_interval(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_interval(&row, 0), None);
	}

	#[test]
	fn test_large_values() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);
		let mut row = layout.allocate_row();

		// Test with large values
		let large_interval = Interval::new(
			120,             // 10 years in months
			3650,            // ~10 years in days
			123456789012345, // Large nanosecond value
		);
		layout.set_interval(&mut row, 0, large_interval.clone());
		assert_eq!(layout.get_interval(&row, 0), large_interval);
	}

	#[test]
	fn test_precision_preservation() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);
		let mut row = layout.allocate_row();

		// Test that all components are preserved exactly
		let precise_interval = Interval::new(
			-5,        // -5 months
			20,        // 20 days
			999999999, // 999,999,999 nanoseconds
		);
		layout.set_interval(&mut row, 0, precise_interval.clone());

		let retrieved = layout.get_interval(&row, 0);
		assert_eq!(retrieved, precise_interval);

		let orig_months = precise_interval.get_months();
		let orig_days = precise_interval.get_days();
		let orig_nanos = precise_interval.get_nanos();
		let ret_months = retrieved.get_months();
		let ret_days = retrieved.get_days();
		let ret_nanos = retrieved.get_nanos();
		assert_eq!(orig_months, ret_months);
		assert_eq!(orig_days, ret_days);
		assert_eq!(orig_nanos, ret_nanos);
	}

	#[test]
	fn test_common_durations() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);

		// Test common durations used in applications
		let common_intervals = [
			Interval::from_seconds(1),  // 1 second
			Interval::from_seconds(30), // 30 seconds
			Interval::from_minutes(5),  // 5 minutes
			Interval::from_minutes(15), // 15 minutes
			Interval::from_hours(1),    // 1 hour
			Interval::from_hours(8),    // Work day
			Interval::from_days(1),     // 1 day
			Interval::from_weeks(1),    // 1 week
			Interval::from_weeks(2),    // 2 weeks
		];

		for interval in common_intervals {
			let mut row = layout.allocate_row();
			layout.set_interval(&mut row, 0, interval.clone());
			assert_eq!(layout.get_interval(&row, 0), interval);
		}
	}

	#[test]
	fn test_boundary_values() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);

		// Test boundary values for each component
		let boundary_intervals = [
			Interval::new(i32::MAX, 0, 0), // Max months
			Interval::new(i32::MIN, 0, 0), // Min months
			Interval::new(0, i32::MAX, 0), // Max days
			Interval::new(0, i32::MIN, 0), // Min days
			Interval::new(0, 0, i64::MAX), // Max nanoseconds
			Interval::new(0, 0, i64::MIN), // Min nanoseconds
		];

		for interval in boundary_intervals {
			let mut row = layout.allocate_row();
			layout.set_interval(&mut row, 0, interval.clone());
			assert_eq!(layout.get_interval(&row, 0), interval);
		}
	}
}
