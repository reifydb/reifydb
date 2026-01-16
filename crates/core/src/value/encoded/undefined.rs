// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::value::encoded::{encoded::EncodedValues, layout::EncodedValuesLayout};

impl EncodedValuesLayout {
	/// Set a field as undefined (not set)
	pub fn set_undefined(&self, row: &mut EncodedValues, index: usize) {
		row.set_valid(index, false);
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{blob::Blob, r#type::Type};

	use crate::value::encoded::layout::EncodedValuesLayout;

	#[test]
	fn test_set_bool() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate();

		// Set a value
		layout.set_bool(&mut row, 0, true);
		assert!(row.is_defined(0));
		assert_eq!(layout.try_get_bool(&row, 0), Some(true));

		// Set as undefined
		layout.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(layout.try_get_bool(&row, 0), None);
	}

	#[test]
	fn test_set_integer() {
		let layout = EncodedValuesLayout::new(&[Type::Int4]);
		let mut row = layout.allocate();

		// Set a value
		layout.set_i32(&mut row, 0, 12345);
		assert!(row.is_defined(0));
		assert_eq!(layout.try_get_i32(&row, 0), Some(12345));

		// Set as undefined
		layout.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(layout.try_get_i32(&row, 0), None);
	}

	#[test]
	fn test_set_dynamic_type() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8]);
		let mut row = layout.allocate();

		// Set a string value
		layout.set_utf8(&mut row, 0, "hello world");
		assert!(row.is_defined(0));
		assert_eq!(layout.try_get_utf8(&row, 0), Some("hello world"));

		// Set as undefined
		layout.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(layout.try_get_utf8(&row, 0), None);
	}

	#[test]
	fn test_set_multiple_fields() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean, Type::Int4, Type::Utf8]);
		let mut row = layout.allocate();

		// Set all fields
		layout.set_bool(&mut row, 0, true);
		layout.set_i32(&mut row, 1, 42);
		layout.set_utf8(&mut row, 2, "test");

		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set middle field as undefined
		layout.set_undefined(&mut row, 1);

		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));

		assert_eq!(layout.try_get_bool(&row, 0), Some(true));
		assert_eq!(layout.try_get_i32(&row, 1), None);
		assert_eq!(layout.try_get_utf8(&row, 2), Some("test"));
	}

	#[test]
	fn test_set_all_fields() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean, Type::Int4, Type::Float8]);
		let mut row = layout.allocate();

		// Set all fields
		layout.set_bool(&mut row, 0, false);
		layout.set_i32(&mut row, 1, -999);
		layout.set_f64(&mut row, 2, 3.14159);

		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set all as undefined
		layout.set_undefined(&mut row, 0);
		layout.set_undefined(&mut row, 1);
		layout.set_undefined(&mut row, 2);

		assert!(!row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(!row.is_defined(2));
		assert!(!layout.all_defined(&row));
	}

	#[test]
	fn test_set_reuse_field() {
		let layout = EncodedValuesLayout::new(&[Type::Int8]);
		let mut row = layout.allocate();

		// Set, unset, then set again
		layout.set_i64(&mut row, 0, 100);
		assert_eq!(layout.try_get_i64(&row, 0), Some(100));

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_i64(&row, 0), None);

		layout.set_i64(&mut row, 0, 200);
		assert_eq!(layout.try_get_i64(&row, 0), Some(200));
	}

	#[test]
	fn test_set_temporal_types() {
		use reifydb_type::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

		let layout = EncodedValuesLayout::new(&[Type::Date, Type::DateTime, Type::Time, Type::Duration]);
		let mut row = layout.allocate();

		// Set temporal values
		let date = Date::new(2025, 1, 15).unwrap();
		let datetime = DateTime::from_timestamp(1642694400).unwrap();
		let time = Time::from_hms(14, 30, 45).unwrap();
		let duration = Duration::from_days(7);

		layout.set_date(&mut row, 0, date.clone());
		layout.set_datetime(&mut row, 1, datetime.clone());
		layout.set_time(&mut row, 2, time.clone());
		layout.set_duration(&mut row, 3, duration.clone());

		// Verify all are defined
		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));
		assert!(row.is_defined(3));

		// Set some as undefined
		layout.set_undefined(&mut row, 0);
		layout.set_undefined(&mut row, 2);

		// Check results
		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(!row.is_defined(2));
		assert!(row.is_defined(3));

		assert_eq!(layout.try_get_date(&row, 0), None);
		assert_eq!(layout.try_get_datetime(&row, 1), Some(datetime));
		assert_eq!(layout.try_get_time(&row, 2), None);
		assert_eq!(layout.try_get_duration(&row, 3), Some(duration));
	}

	#[test]
	fn test_set_uuid_types() {
		use reifydb_type::value::{
			identity::IdentityId,
			uuid::{Uuid4, Uuid7},
		};

		let layout = EncodedValuesLayout::new(&[Type::Uuid4, Type::Uuid7, Type::IdentityId]);
		let mut row = layout.allocate();

		// Set UUID values
		let uuid4 = Uuid4::generate();
		let uuid7 = Uuid7::generate();
		let identity_id = IdentityId::generate();

		layout.set_uuid4(&mut row, 0, uuid4.clone());
		layout.set_uuid7(&mut row, 1, uuid7.clone());
		layout.set_identity_id(&mut row, 2, identity_id.clone());

		// All should be defined
		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set UUID7 as undefined
		layout.set_undefined(&mut row, 1);

		// Check results
		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));

		assert_eq!(layout.try_get_uuid4(&row, 0), Some(uuid4));
		assert_eq!(layout.try_get_uuid7(&row, 1), None);
		assert_eq!(layout.try_get_identity_id(&row, 2), Some(identity_id));
	}

	#[test]
	fn test_set_decimal_int_uint() {
		use std::str::FromStr;

		use reifydb_type::value::{decimal::Decimal, int::Int, uint::Uint};

		let layout = EncodedValuesLayout::new(&[Type::Decimal, Type::Int, Type::Uint]);
		let mut row = layout.allocate();

		// Set values
		let decimal = Decimal::from_str("123.45").unwrap();
		let int = Int::from(i64::MAX);
		let uint = Uint::from(u64::MAX);

		layout.set_decimal(&mut row, 0, &decimal);
		layout.set_int(&mut row, 1, &int);
		layout.set_uint(&mut row, 2, &uint);

		// All should be defined
		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set some as undefined
		layout.set_undefined(&mut row, 0);
		layout.set_undefined(&mut row, 2);

		// Check results
		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(!row.is_defined(2));

		assert_eq!(layout.try_get_decimal(&row, 0), None);
		assert_eq!(layout.try_get_int(&row, 1), Some(int));
		assert_eq!(layout.try_get_uint(&row, 2), None);
	}

	#[test]
	fn test_set_blob() {
		let layout = EncodedValuesLayout::new(&[Type::Blob]);
		let mut row = layout.allocate();

		// Set a blob value
		let blob = Blob::from_slice(&[1, 2, 3, 4, 5]);
		layout.set_blob(&mut row, 0, &blob);
		assert!(row.is_defined(0));
		assert_eq!(layout.try_get_blob(&row, 0), Some(blob.clone()));

		// Set as undefined
		layout.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(layout.try_get_blob(&row, 0), None);

		// Set again with different value
		let blob2 = Blob::from_slice(&[10, 20, 30]);
		layout.set_blob(&mut row, 0, &blob2);
		assert!(row.is_defined(0));
		assert_eq!(layout.try_get_blob(&row, 0), Some(blob2));
	}

	#[test]
	fn test_set_pattern() {
		let layout = EncodedValuesLayout::new(&[
			Type::Boolean,
			Type::Boolean,
			Type::Boolean,
			Type::Boolean,
			Type::Boolean,
		]);
		let mut row = layout.allocate();

		// Set all as true
		for i in 0..5 {
			layout.set_bool(&mut row, i, true);
		}

		// Set every other field as undefined
		for i in (0..5).step_by(2) {
			layout.set_undefined(&mut row, i);
		}

		// Check pattern: undefined, defined, undefined, defined,
		// undefined
		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(!row.is_defined(2));
		assert!(row.is_defined(3));
		assert!(!row.is_defined(4));

		assert_eq!(layout.try_get_bool(&row, 0), None);
		assert_eq!(layout.try_get_bool(&row, 1), Some(true));
		assert_eq!(layout.try_get_bool(&row, 2), None);
		assert_eq!(layout.try_get_bool(&row, 3), Some(true));
		assert_eq!(layout.try_get_bool(&row, 4), None);
	}
}
