// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{blob::Blob, r#type::Type};

	use crate::encoded::schema::Schema;

	#[test]
	fn test_set_bool() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		// Set a value
		schema.set_bool(&mut row, 0, true);
		assert!(row.is_defined(0));
		assert_eq!(schema.try_get_bool(&row, 0), Some(true));

		// Set as undefined
		schema.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(schema.try_get_bool(&row, 0), None);
	}

	#[test]
	fn test_set_integer() {
		let schema = Schema::testing(&[Type::Int4]);
		let mut row = schema.allocate();

		// Set a value
		schema.set_i32(&mut row, 0, 12345);
		assert!(row.is_defined(0));
		assert_eq!(schema.try_get_i32(&row, 0), Some(12345));

		// Set as undefined
		schema.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(schema.try_get_i32(&row, 0), None);
	}

	#[test]
	fn test_set_dynamic_type() {
		let schema = Schema::testing(&[Type::Utf8]);
		let mut row = schema.allocate();

		// Set a string value
		schema.set_utf8(&mut row, 0, "hello world");
		assert!(row.is_defined(0));
		assert_eq!(schema.try_get_utf8(&row, 0), Some("hello world"));

		// Set as undefined
		schema.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(schema.try_get_utf8(&row, 0), None);
	}

	#[test]
	fn test_set_multiple_fields() {
		let schema = Schema::testing(&[Type::Boolean, Type::Int4, Type::Utf8]);
		let mut row = schema.allocate();

		// Set all fields
		schema.set_bool(&mut row, 0, true);
		schema.set_i32(&mut row, 1, 42);
		schema.set_utf8(&mut row, 2, "test");

		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set middle field as undefined
		schema.set_undefined(&mut row, 1);

		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));

		assert_eq!(schema.try_get_bool(&row, 0), Some(true));
		assert_eq!(schema.try_get_i32(&row, 1), None);
		assert_eq!(schema.try_get_utf8(&row, 2), Some("test"));
	}

	#[test]
	fn test_set_all_fields() {
		let schema = Schema::testing(&[Type::Boolean, Type::Int4, Type::Float8]);
		let mut row = schema.allocate();

		// Set all fields
		schema.set_bool(&mut row, 0, false);
		schema.set_i32(&mut row, 1, -999);
		schema.set_f64(&mut row, 2, 3.14159);

		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set all as undefined
		schema.set_undefined(&mut row, 0);
		schema.set_undefined(&mut row, 1);
		schema.set_undefined(&mut row, 2);

		assert!(!row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(!row.is_defined(2));
		assert!(!(0..schema.field_count()).all(|i| row.is_defined(i)));
	}

	#[test]
	fn test_set_reuse_field() {
		let schema = Schema::testing(&[Type::Int8]);
		let mut row = schema.allocate();

		// Set, unset, then set again
		schema.set_i64(&mut row, 0, 100);
		assert_eq!(schema.try_get_i64(&row, 0), Some(100));

		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_i64(&row, 0), None);

		schema.set_i64(&mut row, 0, 200);
		assert_eq!(schema.try_get_i64(&row, 0), Some(200));
	}

	#[test]
	fn test_set_temporal_types() {
		use reifydb_type::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

		let schema = Schema::testing(&[Type::Date, Type::DateTime, Type::Time, Type::Duration]);
		let mut row = schema.allocate();

		// Set temporal values
		let date = Date::new(2025, 1, 15).unwrap();
		let datetime = DateTime::from_timestamp(1642694400).unwrap();
		let time = Time::from_hms(14, 30, 45).unwrap();
		let duration = Duration::from_days(7);

		schema.set_date(&mut row, 0, date.clone());
		schema.set_datetime(&mut row, 1, datetime.clone());
		schema.set_time(&mut row, 2, time.clone());
		schema.set_duration(&mut row, 3, duration.clone());

		// Verify all are defined
		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));
		assert!(row.is_defined(3));

		// Set some as undefined
		schema.set_undefined(&mut row, 0);
		schema.set_undefined(&mut row, 2);

		// Check results
		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(!row.is_defined(2));
		assert!(row.is_defined(3));

		assert_eq!(schema.try_get_date(&row, 0), None);
		assert_eq!(schema.try_get_datetime(&row, 1), Some(datetime));
		assert_eq!(schema.try_get_time(&row, 2), None);
		assert_eq!(schema.try_get_duration(&row, 3), Some(duration));
	}

	#[test]
	fn test_set_uuid_types() {
		use reifydb_type::value::{
			identity::IdentityId,
			uuid::{Uuid4, Uuid7},
		};

		let schema = Schema::testing(&[Type::Uuid4, Type::Uuid7, Type::IdentityId]);
		let mut row = schema.allocate();

		// Set UUID values
		let uuid4 = Uuid4::generate();
		let uuid7 = Uuid7::generate();
		let identity_id = IdentityId::generate();

		schema.set_uuid4(&mut row, 0, uuid4.clone());
		schema.set_uuid7(&mut row, 1, uuid7.clone());
		schema.set_identity_id(&mut row, 2, identity_id.clone());

		// All should be defined
		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set UUID7 as undefined
		schema.set_undefined(&mut row, 1);

		// Check results
		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));

		assert_eq!(schema.try_get_uuid4(&row, 0), Some(uuid4));
		assert_eq!(schema.try_get_uuid7(&row, 1), None);
		assert_eq!(schema.try_get_identity_id(&row, 2), Some(identity_id));
	}

	#[test]
	fn test_set_decimal_int_uint() {
		use std::str::FromStr;

		use reifydb_type::value::{decimal::Decimal, int::Int, uint::Uint};

		let schema = Schema::testing(&[Type::Decimal, Type::Int, Type::Uint]);
		let mut row = schema.allocate();

		// Set values
		let decimal = Decimal::from_str("123.45").unwrap();
		let int = Int::from(i64::MAX);
		let uint = Uint::from(u64::MAX);

		schema.set_decimal(&mut row, 0, &decimal);
		schema.set_int(&mut row, 1, &int);
		schema.set_uint(&mut row, 2, &uint);

		// All should be defined
		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set some as undefined
		schema.set_undefined(&mut row, 0);
		schema.set_undefined(&mut row, 2);

		// Check results
		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(!row.is_defined(2));

		assert_eq!(schema.try_get_decimal(&row, 0), None);
		assert_eq!(schema.try_get_int(&row, 1), Some(int));
		assert_eq!(schema.try_get_uint(&row, 2), None);
	}

	#[test]
	fn test_set_blob() {
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();

		// Set a blob value
		let blob = Blob::from_slice(&[1, 2, 3, 4, 5]);
		schema.set_blob(&mut row, 0, &blob);
		assert!(row.is_defined(0));
		assert_eq!(schema.try_get_blob(&row, 0), Some(blob.clone()));

		// Set as undefined
		schema.set_undefined(&mut row, 0);
		assert!(!row.is_defined(0));
		assert_eq!(schema.try_get_blob(&row, 0), None);

		// Set again with different value
		let blob2 = Blob::from_slice(&[10, 20, 30]);
		schema.set_blob(&mut row, 0, &blob2);
		assert!(row.is_defined(0));
		assert_eq!(schema.try_get_blob(&row, 0), Some(blob2));
	}

	#[test]
	fn test_set_pattern() {
		let schema =
			Schema::testing(&[Type::Boolean, Type::Boolean, Type::Boolean, Type::Boolean, Type::Boolean]);
		let mut row = schema.allocate();

		// Set all as true
		for i in 0..5 {
			schema.set_bool(&mut row, i, true);
		}

		// Set every other field as undefined
		for i in (0..5).step_by(2) {
			schema.set_undefined(&mut row, i);
		}

		// Check pattern: undefined, defined, undefined, defined,
		// undefined
		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(!row.is_defined(2));
		assert!(row.is_defined(3));
		assert!(!row.is_defined(4));

		assert_eq!(schema.try_get_bool(&row, 0), None);
		assert_eq!(schema.try_get_bool(&row, 1), Some(true));
		assert_eq!(schema.try_get_bool(&row, 2), None);
		assert_eq!(schema.try_get_bool(&row, 3), Some(true));
		assert_eq!(schema.try_get_bool(&row, 4), None);
	}
}
