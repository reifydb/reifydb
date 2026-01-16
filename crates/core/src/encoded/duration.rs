// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::{duration::Duration, r#type::Type};

use crate::encoded::{encoded::EncodedValues, layout::EncodedValuesLayout};

impl EncodedValuesLayout {
	pub fn set_duration(&self, row: &mut EncodedValues, index: usize, value: Duration) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Duration);
		row.set_valid(index, true);

		let months = value.get_months();
		let days = value.get_days();
		let nanos = value.get_nanos();
		unsafe {
			// Write months (i32) at offset
			ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut i32, months);
			// Write days (i32) at offset + 4
			ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset + 4) as *mut i32, days);
			// Write nanos (i64) at offset + 8
			ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset + 8) as *mut i64, nanos);
		}
	}

	pub fn get_duration(&self, row: &EncodedValues, index: usize) -> Duration {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Duration);
		unsafe {
			// Read months (i32) from offset
			let months = (row.as_ptr().add(field.offset) as *const i32).read_unaligned();
			// Read days (i32) from offset + 4
			let days = (row.as_ptr().add(field.offset + 4) as *const i32).read_unaligned();
			// Read nanos (i64) from offset + 8
			let nanos = (row.as_ptr().add(field.offset + 8) as *const i64).read_unaligned();
			Duration::new(months, days, nanos)
		}
	}

	pub fn try_get_duration(&self, row: &EncodedValues, index: usize) -> Option<Duration> {
		if row.is_defined(index) && self.fields[index].r#type == Type::Duration {
			Some(self.get_duration(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{duration::Duration, r#type::Type};

	use crate::encoded::layout::EncodedValuesLayout;

	#[test]
	fn test_set_get_duration() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);
		let mut row = layout.allocate();

		let value = Duration::from_seconds(-7200);
		layout.set_duration(&mut row, 0, value.clone());
		assert_eq!(layout.get_duration(&row, 0), value);
	}

	#[test]
	fn test_try_get_duration() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);
		let mut row = layout.allocate();

		assert_eq!(layout.try_get_duration(&row, 0), None);

		let test_duration = Duration::from_days(30);
		layout.set_duration(&mut row, 0, test_duration.clone());
		assert_eq!(layout.try_get_duration(&row, 0), Some(test_duration));
	}

	#[test]
	fn test_zero() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);
		let mut row = layout.allocate();

		let zero = Duration::default(); // Zero duration
		layout.set_duration(&mut row, 0, zero.clone());
		assert_eq!(layout.get_duration(&row, 0), zero);
	}

	#[test]
	fn test_various_durations() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);

		let test_durations = [
			Duration::from_seconds(0),     // Zero
			Duration::from_seconds(60),    // 1 minute
			Duration::from_seconds(3600),  // 1 hour
			Duration::from_seconds(86400), // 1 day
			Duration::from_days(7),        // 1 week
			Duration::from_days(30),       // ~1 month
			Duration::from_weeks(52),      // ~1 year
		];

		for duration in test_durations {
			let mut row = layout.allocate();
			layout.set_duration(&mut row, 0, duration.clone());
			assert_eq!(layout.get_duration(&row, 0), duration);
		}
	}

	#[test]
	fn test_negative_durations() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);

		let negative_durations = [
			Duration::from_seconds(-60),    // -1 minute
			Duration::from_seconds(-3600),  // -1 hour
			Duration::from_seconds(-86400), // -1 day
			Duration::from_days(-7),        // -1 week
			Duration::from_weeks(-4),       // -1 month
		];

		for duration in negative_durations {
			let mut row = layout.allocate();
			layout.set_duration(&mut row, 0, duration.clone());
			assert_eq!(layout.get_duration(&row, 0), duration);
		}
	}

	#[test]
	fn test_complex_parts() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);
		let mut row = layout.allocate();

		// Create a duration with all components
		let complex_duration = Duration::new(
			6,         // 6 months
			15,        // 15 days
			123456789, // nanoseconds
		);
		layout.set_duration(&mut row, 0, complex_duration.clone());
		assert_eq!(layout.get_duration(&row, 0), complex_duration);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedValuesLayout::new(&[Type::Duration, Type::Boolean, Type::Duration, Type::Int8]);
		let mut row = layout.allocate();

		let duration1 = Duration::from_hours(24);
		let duration2 = Duration::from_minutes(-30);

		layout.set_duration(&mut row, 0, duration1.clone());
		layout.set_bool(&mut row, 1, true);
		layout.set_duration(&mut row, 2, duration2.clone());
		layout.set_i64(&mut row, 3, 987654321);

		assert_eq!(layout.get_duration(&row, 0), duration1);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_duration(&row, 2), duration2);
		assert_eq!(layout.get_i64(&row, 3), 987654321);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedValuesLayout::new(&[Type::Duration, Type::Duration]);
		let mut row = layout.allocate();

		let duration = Duration::from_days(100);
		layout.set_duration(&mut row, 0, duration.clone());

		assert_eq!(layout.try_get_duration(&row, 0), Some(duration));
		assert_eq!(layout.try_get_duration(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_duration(&row, 0), None);
	}

	#[test]
	fn test_large_values() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);
		let mut row = layout.allocate();

		// Test with large values
		let large_duration = Duration::new(
			120,             // 10 years in months
			3650,            // ~10 years in days
			123456789012345, // Large nanosecond value
		);
		layout.set_duration(&mut row, 0, large_duration.clone());
		assert_eq!(layout.get_duration(&row, 0), large_duration);
	}

	#[test]
	fn test_precision_preservation() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);
		let mut row = layout.allocate();

		// Test that all components are preserved exactly
		let precise_duration = Duration::new(
			-5,        // -5 months
			20,        // 20 days
			999999999, // 999,999,999 nanoseconds
		);
		layout.set_duration(&mut row, 0, precise_duration.clone());

		let retrieved = layout.get_duration(&row, 0);
		assert_eq!(retrieved, precise_duration);

		let orig_months = precise_duration.get_months();
		let orig_days = precise_duration.get_days();
		let orig_nanos = precise_duration.get_nanos();
		let ret_months = retrieved.get_months();
		let ret_days = retrieved.get_days();
		let ret_nanos = retrieved.get_nanos();
		assert_eq!(orig_months, ret_months);
		assert_eq!(orig_days, ret_days);
		assert_eq!(orig_nanos, ret_nanos);
	}

	#[test]
	fn test_common_durations() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);

		// Test common durations used in applications
		let common_durations = [
			Duration::from_seconds(1),  // 1 second
			Duration::from_seconds(30), // 30 seconds
			Duration::from_minutes(5),  // 5 minutes
			Duration::from_minutes(15), // 15 minutes
			Duration::from_hours(1),    // 1 hour
			Duration::from_hours(8),    // Work day
			Duration::from_days(1),     // 1 day
			Duration::from_weeks(1),    // 1 week
			Duration::from_weeks(2),    // 2 weeks
		];

		for duration in common_durations {
			let mut row = layout.allocate();
			layout.set_duration(&mut row, 0, duration.clone());
			assert_eq!(layout.get_duration(&row, 0), duration);
		}
	}

	#[test]
	fn test_boundary_values() {
		let layout = EncodedValuesLayout::new(&[Type::Duration]);

		// Test boundary values for each component
		let boundary_durations = [
			Duration::new(i32::MAX, 0, 0), // Max months
			Duration::new(i32::MIN, 0, 0), // Min months
			Duration::new(0, i32::MAX, 0), // Max days
			Duration::new(0, i32::MIN, 0), // Min days
			Duration::new(0, 0, i64::MAX), // Max nanoseconds
			Duration::new(0, 0, i64::MIN), // Min nanoseconds
		];

		for duration in boundary_durations {
			let mut row = layout.allocate();
			layout.set_duration(&mut row, 0, duration.clone());
			assert_eq!(layout.get_duration(&row, 0), duration);
		}
	}

	#[test]
	fn test_try_get_duration_wrong_type() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate();

		layout.set_bool(&mut row, 0, true);

		assert_eq!(layout.try_get_duration(&row, 0), None);
	}
}
