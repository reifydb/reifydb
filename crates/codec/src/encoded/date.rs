// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::{date::Date, value_type::ValueType};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_date() {
		let shape = RowShape::testing(&[ValueType::Date]);
		let mut row = shape.allocate();

		let value = Date::new(2021, 1, 1).unwrap();
		shape.set::<Date>(&mut row, 0, value.clone());
		assert_eq!(shape.get::<Date>(&row, 0), value);
	}

	#[test]
	fn test_try_get_date() {
		let shape = RowShape::testing(&[ValueType::Date]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get::<Date>(&row, 0), None);

		let test_date = Date::from_ymd(2025, 1, 15).unwrap();
		shape.set::<Date>(&mut row, 0, test_date.clone());
		assert_eq!(shape.try_get::<Date>(&row, 0), Some(test_date));
	}

	#[test]
	fn test_epoch() {
		let shape = RowShape::testing(&[ValueType::Date]);
		let mut row = shape.allocate();

		let epoch = Date::default(); // Unix epoch
		shape.set::<Date>(&mut row, 0, epoch.clone());
		assert_eq!(shape.get::<Date>(&row, 0), epoch);
	}

	#[test]
	fn test_various_dates() {
		let shape = RowShape::testing(&[ValueType::Date]);

		let test_dates = [
			Date::new(1970, 1, 1).unwrap(),   // Unix epoch
			Date::new(2000, 1, 1).unwrap(),   // Y2K
			Date::new(2024, 2, 29).unwrap(),  // Leap year
			Date::new(2025, 12, 31).unwrap(), // Future date
		];

		for date in test_dates {
			let mut row = shape.allocate();
			shape.set::<Date>(&mut row, 0, date.clone());
			assert_eq!(shape.get::<Date>(&row, 0), date);
		}
	}

	#[test]
	fn test_boundaries() {
		let shape = RowShape::testing(&[ValueType::Date]);

		// Test various boundary dates that should work
		let boundary_dates = [
			Date::new(1900, 1, 1).unwrap(),
			Date::new(1999, 12, 31).unwrap(),
			Date::new(2000, 1, 1).unwrap(),
			Date::new(2100, 12, 31).unwrap(),
		];

		for date in boundary_dates {
			let mut row = shape.allocate();
			shape.set::<Date>(&mut row, 0, date.clone());
			assert_eq!(shape.get::<Date>(&row, 0), date);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Date, ValueType::Boolean, ValueType::Date, ValueType::Int4]);
		let mut row = shape.allocate();

		let date1 = Date::new(2025, 6, 15).unwrap();
		let date2 = Date::new(1995, 3, 22).unwrap();

		shape.set::<Date>(&mut row, 0, date1.clone());
		shape.set::<bool>(&mut row, 1, true);
		shape.set::<Date>(&mut row, 2, date2.clone());
		shape.set::<i32>(&mut row, 3, 42i32);

		assert_eq!(shape.get::<Date>(&row, 0), date1);
		assert_eq!(shape.get::<bool>(&row, 1), true);
		assert_eq!(shape.get::<Date>(&row, 2), date2);
		assert_eq!(shape.get::<i32>(&row, 3), 42);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Date, ValueType::Date]);
		let mut row = shape.allocate();

		let date = Date::new(2025, 7, 4).unwrap();
		shape.set::<Date>(&mut row, 0, date.clone());

		assert_eq!(shape.try_get::<Date>(&row, 0), Some(date));
		assert_eq!(shape.try_get::<Date>(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get::<Date>(&row, 0), None);
	}

	#[test]
	fn test_clone_consistency() {
		let shape = RowShape::testing(&[ValueType::Date]);
		let mut row = shape.allocate();

		let original_date = Date::new(2023, 9, 15).unwrap();
		shape.set::<Date>(&mut row, 0, original_date.clone());

		let retrieved_date = shape.get::<Date>(&row, 0);
		assert_eq!(retrieved_date, original_date);

		// Verify that the retrieved date is functionally equivalent
		assert_eq!(retrieved_date.to_days_since_epoch(), original_date.to_days_since_epoch());
	}

	#[test]
	fn test_special_years() {
		let shape = RowShape::testing(&[ValueType::Date]);

		// Test leap years and century boundaries
		let special_dates = [
			Date::new(1600, 2, 29).unwrap(), // Leap year century
			Date::new(1700, 2, 28).unwrap(), // Non-leap century
			Date::new(1800, 2, 28).unwrap(), // Non-leap century
			Date::new(1900, 2, 28).unwrap(), // Non-leap century
			Date::new(2000, 2, 29).unwrap(), // Leap year century
			Date::new(2024, 2, 29).unwrap(), // Recent leap year
		];

		for date in special_dates {
			let mut row = shape.allocate();
			shape.set::<Date>(&mut row, 0, date.clone());
			assert_eq!(shape.get::<Date>(&row, 0), date);
		}
	}

	#[test]
	fn test_try_get_date_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set::<bool>(&mut row, 0, true);

		assert_eq!(shape.try_get::<Date>(&row, 0), None);
	}
}
