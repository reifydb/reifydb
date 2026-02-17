// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::{date::Date, r#type::Type};

use crate::encoded::{encoded::EncodedValues, schema::Schema};

impl Schema {
	pub fn set_date(&self, row: &mut EncodedValues, index: usize, value: Date) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Date);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut i32,
				value.to_days_since_epoch(),
			)
		}
	}

	pub fn get_date(&self, row: &EncodedValues, index: usize) -> Date {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Date);
		unsafe {
			Date::from_days_since_epoch(
				(row.as_ptr().add(field.offset as usize) as *const i32).read_unaligned(),
			)
			.unwrap()
		}
	}

	pub fn try_get_date(&self, row: &EncodedValues, index: usize) -> Option<Date> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Date {
			Some(self.get_date(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{date::Date, r#type::Type};

	use crate::encoded::schema::Schema;

	#[test]
	fn test_set_get_date() {
		let schema = Schema::testing(&[Type::Date]);
		let mut row = schema.allocate();

		let value = Date::new(2021, 1, 1).unwrap();
		schema.set_date(&mut row, 0, value.clone());
		assert_eq!(schema.get_date(&row, 0), value);
	}

	#[test]
	fn test_try_get_date() {
		let schema = Schema::testing(&[Type::Date]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_date(&row, 0), None);

		let test_date = Date::from_ymd(2025, 1, 15).unwrap();
		schema.set_date(&mut row, 0, test_date.clone());
		assert_eq!(schema.try_get_date(&row, 0), Some(test_date));
	}

	#[test]
	fn test_epoch() {
		let schema = Schema::testing(&[Type::Date]);
		let mut row = schema.allocate();

		let epoch = Date::default(); // Unix epoch
		schema.set_date(&mut row, 0, epoch.clone());
		assert_eq!(schema.get_date(&row, 0), epoch);
	}

	#[test]
	fn test_various_dates() {
		let schema = Schema::testing(&[Type::Date]);

		let test_dates = [
			Date::new(1970, 1, 1).unwrap(),   // Unix epoch
			Date::new(2000, 1, 1).unwrap(),   // Y2K
			Date::new(2024, 2, 29).unwrap(),  // Leap year
			Date::new(2025, 12, 31).unwrap(), // Future date
		];

		for date in test_dates {
			let mut row = schema.allocate();
			schema.set_date(&mut row, 0, date.clone());
			assert_eq!(schema.get_date(&row, 0), date);
		}
	}

	#[test]
	fn test_boundaries() {
		let schema = Schema::testing(&[Type::Date]);

		// Test various boundary dates that should work
		let boundary_dates = [
			Date::new(1900, 1, 1).unwrap(),
			Date::new(1999, 12, 31).unwrap(),
			Date::new(2000, 1, 1).unwrap(),
			Date::new(2100, 12, 31).unwrap(),
		];

		for date in boundary_dates {
			let mut row = schema.allocate();
			schema.set_date(&mut row, 0, date.clone());
			assert_eq!(schema.get_date(&row, 0), date);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let schema = Schema::testing(&[Type::Date, Type::Boolean, Type::Date, Type::Int4]);
		let mut row = schema.allocate();

		let date1 = Date::new(2025, 6, 15).unwrap();
		let date2 = Date::new(1995, 3, 22).unwrap();

		schema.set_date(&mut row, 0, date1.clone());
		schema.set_bool(&mut row, 1, true);
		schema.set_date(&mut row, 2, date2.clone());
		schema.set_i32(&mut row, 3, 42);

		assert_eq!(schema.get_date(&row, 0), date1);
		assert_eq!(schema.get_bool(&row, 1), true);
		assert_eq!(schema.get_date(&row, 2), date2);
		assert_eq!(schema.get_i32(&row, 3), 42);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Date, Type::Date]);
		let mut row = schema.allocate();

		let date = Date::new(2025, 7, 4).unwrap();
		schema.set_date(&mut row, 0, date.clone());

		assert_eq!(schema.try_get_date(&row, 0), Some(date));
		assert_eq!(schema.try_get_date(&row, 1), None);

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_date(&row, 0), None);
	}

	#[test]
	fn test_clone_consistency() {
		let schema = Schema::testing(&[Type::Date]);
		let mut row = schema.allocate();

		let original_date = Date::new(2023, 9, 15).unwrap();
		schema.set_date(&mut row, 0, original_date.clone());

		let retrieved_date = schema.get_date(&row, 0);
		assert_eq!(retrieved_date, original_date);

		// Verify that the retrieved date is functionally equivalent
		assert_eq!(retrieved_date.to_days_since_epoch(), original_date.to_days_since_epoch());
	}

	#[test]
	fn test_special_years() {
		let schema = Schema::testing(&[Type::Date]);

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
			let mut row = schema.allocate();
			schema.set_date(&mut row, 0, date.clone());
			assert_eq!(schema.get_date(&row, 0), date);
		}
	}

	#[test]
	fn test_try_get_date_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_date(&row, 0), None);
	}
}
