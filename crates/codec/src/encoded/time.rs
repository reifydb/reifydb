// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::{time::Time, value_type::ValueType};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_time() {
		let shape = RowShape::testing(&[ValueType::Time]);
		let mut row = shape.allocate();

		let value = Time::new(20, 50, 0, 0).unwrap();
		shape.set::<Time>(&mut row, 0, value.clone());
		assert_eq!(shape.get::<Time>(&row, 0), value);
	}

	#[test]
	fn test_try_get_time() {
		let shape = RowShape::testing(&[ValueType::Time]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get::<Time>(&row, 0), None);

		let test_time = Time::from_hms(14, 30, 45).unwrap();
		shape.set::<Time>(&mut row, 0, test_time.clone());
		assert_eq!(shape.try_get::<Time>(&row, 0), Some(test_time));
	}

	#[test]
	fn test_time_midnight() {
		let shape = RowShape::testing(&[ValueType::Time]);
		let mut row = shape.allocate();

		let midnight = Time::default(); // 00:00:00
		shape.set::<Time>(&mut row, 0, midnight.clone());
		assert_eq!(shape.get::<Time>(&row, 0), midnight);
	}

	#[test]
	fn test_time_with_nanoseconds() {
		let shape = RowShape::testing(&[ValueType::Time]);
		let mut row = shape.allocate();

		let precise_time = Time::new(15, 30, 45, 123456789).unwrap();
		shape.set::<Time>(&mut row, 0, precise_time.clone());
		assert_eq!(shape.get::<Time>(&row, 0), precise_time);
	}

	#[test]
	fn test_time_various_times() {
		let shape = RowShape::testing(&[ValueType::Time]);

		let test_times = [
			Time::new(0, 0, 0, 0).unwrap(),            // Midnight
			Time::new(12, 0, 0, 0).unwrap(),           // Noon
			Time::new(23, 59, 59, 999999999).unwrap(), // Just before midnight
			Time::new(6, 30, 15, 500000000).unwrap(),  // Morning time
			Time::new(18, 45, 30, 750000000).unwrap(), // Evening time
		];

		for time in test_times {
			let mut row = shape.allocate();
			shape.set::<Time>(&mut row, 0, time.clone());
			assert_eq!(shape.get::<Time>(&row, 0), time);
		}
	}

	#[test]
	fn test_time_boundary_cases() {
		let shape = RowShape::testing(&[ValueType::Time]);

		let boundary_times = [
			Time::new(0, 0, 0, 0).unwrap(), // Start of day
			Time::new(0, 0, 0, 1).unwrap(), /* One nanosecond
			                                 * after midnight */
			Time::new(23, 59, 59, 999999998).unwrap(), // One nanosecond before midnight
			Time::new(23, 59, 59, 999999999).unwrap(), // Last nanosecond of day
		];

		for time in boundary_times {
			let mut row = shape.allocate();
			shape.set::<Time>(&mut row, 0, time.clone());
			assert_eq!(shape.get::<Time>(&row, 0), time);
		}
	}

	#[test]
	fn test_time_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Time, ValueType::Boolean, ValueType::Time, ValueType::Int4]);
		let mut row = shape.allocate();

		let time1 = Time::new(9, 15, 30, 0).unwrap();
		let time2 = Time::new(21, 45, 0, 250000000).unwrap();

		shape.set::<Time>(&mut row, 0, time1.clone());
		shape.set::<bool>(&mut row, 1, false);
		shape.set::<Time>(&mut row, 2, time2.clone());
		shape.set::<i32>(&mut row, 3, -999i32);

		assert_eq!(shape.get::<Time>(&row, 0), time1);
		assert_eq!(shape.get::<bool>(&row, 1), false);
		assert_eq!(shape.get::<Time>(&row, 2), time2);
		assert_eq!(shape.get::<i32>(&row, 3), -999);
	}

	#[test]
	fn test_time_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Time, ValueType::Time]);
		let mut row = shape.allocate();

		let time = Time::new(16, 20, 45, 333000000).unwrap();
		shape.set::<Time>(&mut row, 0, time.clone());

		assert_eq!(shape.try_get::<Time>(&row, 0), Some(time));
		assert_eq!(shape.try_get::<Time>(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get::<Time>(&row, 0), None);
	}

	#[test]
	fn test_time_precision_preservation() {
		let shape = RowShape::testing(&[ValueType::Time]);
		let mut row = shape.allocate();

		// The slot holds nanos since midnight, so no sub-second digit may be rounded away.
		let high_precision = Time::new(12, 34, 56, 987654321).unwrap();
		shape.set::<Time>(&mut row, 0, high_precision.clone());

		let retrieved = shape.get::<Time>(&row, 0);
		assert_eq!(retrieved, high_precision);
		assert_eq!(retrieved.to_nanos_since_midnight(), high_precision.to_nanos_since_midnight());
	}

	#[test]
	fn test_time_microsecond_precision() {
		let shape = RowShape::testing(&[ValueType::Time]);
		let mut row = shape.allocate();

		let microsecond_precision = Time::new(14, 25, 30, 123456000).unwrap();
		shape.set::<Time>(&mut row, 0, microsecond_precision.clone());
		assert_eq!(shape.get::<Time>(&row, 0), microsecond_precision);
	}

	#[test]
	fn test_time_millisecond_precision() {
		let shape = RowShape::testing(&[ValueType::Time]);
		let mut row = shape.allocate();

		let millisecond_precision = Time::new(8, 15, 42, 123000000).unwrap();
		shape.set::<Time>(&mut row, 0, millisecond_precision.clone());
		assert_eq!(shape.get::<Time>(&row, 0), millisecond_precision);
	}

	#[test]
	fn test_time_common_times() {
		let shape = RowShape::testing(&[ValueType::Time]);

		let common_times = [
			Time::new(9, 0, 0, 0).unwrap(),   // 9 AM start of work
			Time::new(12, 0, 0, 0).unwrap(),  // Noon
			Time::new(17, 0, 0, 0).unwrap(),  // 5 PM end of work
			Time::new(0, 0, 1, 0).unwrap(),   // 1 second after midnight
			Time::new(23, 59, 0, 0).unwrap(), // 1 minute before midnight
		];

		for time in common_times {
			let mut row = shape.allocate();
			shape.set::<Time>(&mut row, 0, time.clone());
			assert_eq!(shape.get::<Time>(&row, 0), time);
		}
	}

	#[test]
	fn test_try_get_time_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set::<bool>(&mut row, 0, true);

		assert_eq!(shape.try_get::<Time>(&row, 0), None);
	}
}
