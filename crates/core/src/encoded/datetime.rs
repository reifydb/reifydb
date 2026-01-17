// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::{datetime::DateTime, r#type::Type};

use crate::encoded::{encoded::EncodedValues, schema::Schema};

impl Schema {
	pub fn set_datetime(&self, row: &mut EncodedValues, index: usize, value: DateTime) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::DateTime);
		row.set_valid(index, true);

		let (seconds, nanos) = value.to_parts();
		unsafe {
			// Write seconds at offset
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut i64,
				seconds,
			);
			// Write nanos at offset + 8
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize + 8) as *mut u32,
				nanos,
			);
		}
	}

	pub fn get_datetime(&self, row: &EncodedValues, index: usize) -> DateTime {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::DateTime);
		unsafe {
			// Read i64 seconds at offset
			let seconds = (row.as_ptr().add(field.offset as usize) as *const i64).read_unaligned();
			// Read u32 nanos at offset + 8
			let nanos = (row.as_ptr().add(field.offset as usize + 8) as *const u32).read_unaligned();
			DateTime::from_parts(seconds, nanos).unwrap()
		}
	}

	pub fn try_get_datetime(&self, row: &EncodedValues, index: usize) -> Option<DateTime> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::DateTime {
			Some(self.get_datetime(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{datetime::DateTime, r#type::Type};

	use crate::encoded::schema::Schema;

	#[test]
	fn test_set_get_datetime() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		let value = DateTime::new(2024, 9, 9, 08, 17, 0, 1234).unwrap();
		schema.set_datetime(&mut row, 0, value.clone());
		assert_eq!(schema.get_datetime(&row, 0), value);
	}

	#[test]
	fn test_try_get_datetime() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_datetime(&row, 0), None);

		let test_datetime = DateTime::from_timestamp(1642694400).unwrap();
		schema.set_datetime(&mut row, 0, test_datetime.clone());
		assert_eq!(schema.try_get_datetime(&row, 0), Some(test_datetime));
	}

	#[test]
	fn test_epoch() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		let epoch = DateTime::default(); // Unix epoch
		schema.set_datetime(&mut row, 0, epoch.clone());
		assert_eq!(schema.get_datetime(&row, 0), epoch);
	}

	#[test]
	fn test_with_nanoseconds() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		// Test with high precision nanoseconds
		let precise_datetime = DateTime::new(2024, 12, 25, 15, 30, 45, 123456789).unwrap();
		schema.set_datetime(&mut row, 0, precise_datetime.clone());
		assert_eq!(schema.get_datetime(&row, 0), precise_datetime);
	}

	#[test]
	fn test_various_timestamps() {
		let schema = Schema::testing(&[Type::DateTime]);

		let test_datetimes = [
			DateTime::from_timestamp(0).unwrap(),          // Unix epoch
			DateTime::from_timestamp(946684800).unwrap(),  // 2000-01-01
			DateTime::from_timestamp(1640995200).unwrap(), // 2022-01-01
			DateTime::from_timestamp(1735689600).unwrap(), // 2025-01-01
		];

		for datetime in test_datetimes {
			let mut row = schema.allocate();
			schema.set_datetime(&mut row, 0, datetime.clone());
			assert_eq!(schema.get_datetime(&row, 0), datetime);
		}
	}

	#[test]
	fn test_negative_timestamps() {
		let schema = Schema::testing(&[Type::DateTime]);

		// Test dates before Unix epoch
		let pre_epoch_datetimes = [
			DateTime::from_timestamp(-86400).unwrap(),    // 1969-12-31
			DateTime::from_timestamp(-31536000).unwrap(), // 1969-01-01
		];

		for datetime in pre_epoch_datetimes {
			let mut row = schema.allocate();
			schema.set_datetime(&mut row, 0, datetime.clone());
			assert_eq!(schema.get_datetime(&row, 0), datetime);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::DateTime, Type::Boolean, Type::DateTime, Type::Int8]);
		let mut row = schema.allocate();

		let datetime1 = DateTime::new(2025, 6, 15, 12, 0, 0, 0).unwrap();
		let datetime2 = DateTime::new(1995, 3, 22, 18, 30, 45, 500000000).unwrap();

		schema.set_datetime(&mut row, 0, datetime1.clone());
		schema.set_bool(&mut row, 1, true);
		schema.set_datetime(&mut row, 2, datetime2.clone());
		schema.set_i64(&mut row, 3, 1234567890);

		assert_eq!(schema.get_datetime(&row, 0), datetime1);
		assert_eq!(schema.get_bool(&row, 1), true);
		assert_eq!(schema.get_datetime(&row, 2), datetime2);
		assert_eq!(schema.get_i64(&row, 3), 1234567890);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::DateTime, Type::DateTime]);
		let mut row = schema.allocate();

		let datetime = DateTime::new(2025, 7, 4, 16, 20, 15, 750000000).unwrap();
		schema.set_datetime(&mut row, 0, datetime.clone());

		assert_eq!(schema.try_get_datetime(&row, 0), Some(datetime));
		assert_eq!(schema.try_get_datetime(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_datetime(&row, 0), None);
	}

	#[test]
	fn test_precision_preservation() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		// Test that nanosecond precision is preserved
		let high_precision = DateTime::new(2024, 1, 1, 0, 0, 0, 999999999).unwrap();
		schema.set_datetime(&mut row, 0, high_precision.clone());

		let retrieved = schema.get_datetime(&row, 0);
		assert_eq!(retrieved, high_precision);

		let (orig_sec, orig_nanos) = high_precision.to_parts();
		let (ret_sec, ret_nanos) = retrieved.to_parts();
		assert_eq!(orig_sec, ret_sec);
		assert_eq!(orig_nanos, ret_nanos);
	}

	#[test]
	fn test_year_2038_problem() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		// Test the Y2038 boundary (beyond 32-bit timestamp limits)
		let post_2038 = DateTime::from_timestamp(2147483648).unwrap(); // 2038-01-19
		schema.set_datetime(&mut row, 0, post_2038.clone());
		assert_eq!(schema.get_datetime(&row, 0), post_2038);
	}

	#[test]
	fn test_far_future() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		// Test a far future date
		let far_future = DateTime::from_timestamp(4102444800).unwrap(); // 2100-01-01
		schema.set_datetime(&mut row, 0, far_future.clone());
		assert_eq!(schema.get_datetime(&row, 0), far_future);
	}

	#[test]
	fn test_microsecond_precision() {
		let schema = Schema::testing(&[Type::DateTime]);
		let mut row = schema.allocate();

		// Test microsecond precision (common in databases)
		let microsecond_precision = DateTime::new(2024, 6, 15, 14, 30, 25, 123456000).unwrap();
		schema.set_datetime(&mut row, 0, microsecond_precision.clone());
		assert_eq!(schema.get_datetime(&row, 0), microsecond_precision);
	}

	#[test]
	fn test_try_get_datetime_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_datetime(&row, 0), None);
	}
}
