// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_value::value::{
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
	let mock = MockClock::from_millis(1000);
	let clock = Clock::Mock(mock.clone());
	let rng = Rng::seeded(42);
	(mock, clock, rng)
}

#[test]
fn test_set_bool() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);
	assert!(row.is_defined(0));
	assert_eq!(shape.try_get::<bool>(&row, 0), Some(true));

	shape.set_none(&mut row, 0);
	assert!(!row.is_defined(0));
	assert_eq!(shape.try_get::<bool>(&row, 0), None);
}

#[test]
fn test_set_integer() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int4]);
	let mut row = shape.allocate_pod();

	shape.set::<i32>(&mut row, 0, 12345i32);
	assert!(row.is_defined(0));
	assert_eq!(shape.try_get::<i32>(&row, 0), Some(12345));

	shape.set_none(&mut row, 0);
	assert!(!row.is_defined(0));
	assert_eq!(shape.try_get::<i32>(&row, 0), None);
}

#[test]
fn test_set_dynamic_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Utf8]);
	let mut row = shape.allocate_pod();

	shape.set_utf8(&mut row, 0, "hello world");
	assert!(row.is_defined(0));
	assert_eq!(shape.try_get_utf8(&row, 0), Some("hello world"));

	shape.set_none(&mut row, 0);
	assert!(!row.is_defined(0));
	assert_eq!(shape.try_get_utf8(&row, 0), None);
}

#[test]
fn test_set_multiple_fields() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean, ValueType::Int4, ValueType::Utf8]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);
	shape.set::<i32>(&mut row, 1, 42i32);
	shape.set_utf8(&mut row, 2, "test");

	assert!(row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(row.is_defined(2));

	shape.set_none(&mut row, 1);

	assert!(row.is_defined(0));
	assert!(!row.is_defined(1));
	assert!(row.is_defined(2));

	assert_eq!(shape.try_get::<bool>(&row, 0), Some(true));
	assert_eq!(shape.try_get::<i32>(&row, 1), None);
	assert_eq!(shape.try_get_utf8(&row, 2), Some("test"));
}

#[test]
fn test_set_all_fields() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean, ValueType::Int4, ValueType::Float8]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, false);
	shape.set::<i32>(&mut row, 1, -999i32);
	shape.set::<f64>(&mut row, 2, 3.14159f64);

	assert!(row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(row.is_defined(2));

	shape.set_none(&mut row, 0);
	shape.set_none(&mut row, 1);
	shape.set_none(&mut row, 2);

	assert!(!row.is_defined(0));
	assert!(!row.is_defined(1));
	assert!(!row.is_defined(2));
	assert!(!(0..shape.field_count()).all(|i| row.is_defined(i)));
}

#[test]
fn test_set_reuse_field() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int8]);
	let mut row = shape.allocate_pod();

	shape.set::<i64>(&mut row, 0, 100i64);
	assert_eq!(shape.try_get::<i64>(&row, 0), Some(100));

	shape.set_none(&mut row, 0);
	assert_eq!(shape.try_get::<i64>(&row, 0), None);

	shape.set::<i64>(&mut row, 0, 200i64);
	assert_eq!(shape.try_get::<i64>(&row, 0), Some(200));
}

#[test]
fn test_set_temporal_types() {
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[ValueType::Date, ValueType::DateTime, ValueType::Time, ValueType::Duration],
	);
	let mut row = shape.allocate_pod();

	let date = Date::new(2025, 1, 15).unwrap();
	let datetime = DateTime::from_epoch_secs(1642694400).unwrap();
	let time = Time::from_hms(14, 30, 45).unwrap();
	let duration = Duration::from_days(7).unwrap();

	shape.set::<Date>(&mut row, 0, date.clone());
	shape.set::<DateTime>(&mut row, 1, datetime.clone());
	shape.set::<Time>(&mut row, 2, time.clone());
	shape.set::<Duration>(&mut row, 3, duration.clone());

	assert!(row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(row.is_defined(2));
	assert!(row.is_defined(3));

	shape.set_none(&mut row, 0);
	shape.set_none(&mut row, 2);

	assert!(!row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(!row.is_defined(2));
	assert!(row.is_defined(3));

	assert_eq!(shape.try_get::<Date>(&row, 0), None);
	assert_eq!(shape.try_get::<DateTime>(&row, 1), Some(datetime));
	assert_eq!(shape.try_get::<Time>(&row, 2), None);
	assert_eq!(shape.try_get::<Duration>(&row, 3), Some(duration));
}

#[test]
fn test_set_uuid_types() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid4, ValueType::Uuid7, ValueType::IdentityId]);
	let mut row = shape.allocate_pod();
	let (_mock, clock, rng) = test_clock_and_rng();

	let uuid4 = Uuid4::generate();
	let uuid7 = Uuid7::generate(&clock, &rng);
	let identity_id = IdentityId::generate(&clock, &rng);

	shape.set::<Uuid4>(&mut row, 0, uuid4.clone());
	shape.set::<Uuid7>(&mut row, 1, uuid7.clone());
	shape.set::<IdentityId>(&mut row, 2, identity_id.clone());

	assert!(row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(row.is_defined(2));

	shape.set_none(&mut row, 1);

	assert!(row.is_defined(0));
	assert!(!row.is_defined(1));
	assert!(row.is_defined(2));

	assert_eq!(shape.try_get::<Uuid4>(&row, 0), Some(uuid4));
	assert_eq!(shape.try_get::<Uuid7>(&row, 1), None);
	assert_eq!(shape.try_get::<IdentityId>(&row, 2), Some(identity_id));
}

#[test]
fn test_set_decimal_int_uint() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Decimal, ValueType::Int, ValueType::Uint]);
	let mut row = shape.allocate_pod();

	let decimal = Decimal::from_str("123.45").unwrap();
	let int = Int::from(i64::MAX);
	let uint = Uint::from(u64::MAX);

	shape.set_decimal(&mut row, 0, &decimal);
	shape.set_int(&mut row, 1, &int);
	shape.set_uint(&mut row, 2, &uint);

	assert!(row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(row.is_defined(2));

	shape.set_none(&mut row, 0);
	shape.set_none(&mut row, 2);

	assert!(!row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(!row.is_defined(2));

	assert_eq!(shape.try_get_decimal(&row, 0), None);
	assert_eq!(shape.try_get_int(&row, 1), Some(int));
	assert_eq!(shape.try_get_uint(&row, 2), None);
}

#[test]
fn test_set_blob() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Blob]);
	let mut row = shape.allocate_pod();

	let blob = Blob::from_slice(&[1, 2, 3, 4, 5]);
	shape.set_blob(&mut row, 0, &blob);
	assert!(row.is_defined(0));
	assert_eq!(shape.try_get_blob(&row, 0), Some(blob.clone()));

	shape.set_none(&mut row, 0);
	assert!(!row.is_defined(0));
	assert_eq!(shape.try_get_blob(&row, 0), None);

	let blob2 = Blob::from_slice(&[10, 20, 30]);
	shape.set_blob(&mut row, 0, &blob2);
	assert!(row.is_defined(0));
	assert_eq!(shape.try_get_blob(&row, 0), Some(blob2));
}

#[test]
fn test_set_pattern() {
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[ValueType::Boolean, ValueType::Boolean, ValueType::Boolean, ValueType::Boolean, ValueType::Boolean],
	);
	let mut row = shape.allocate_pod();

	for i in 0..5 {
		shape.set::<bool>(&mut row, i, true);
	}

	for i in (0..5).step_by(2) {
		shape.set_none(&mut row, i);
	}

	assert!(!row.is_defined(0));
	assert!(row.is_defined(1));
	assert!(!row.is_defined(2));
	assert!(row.is_defined(3));
	assert!(!row.is_defined(4));

	assert_eq!(shape.try_get::<bool>(&row, 0), None);
	assert_eq!(shape.try_get::<bool>(&row, 1), Some(true));
	assert_eq!(shape.try_get::<bool>(&row, 2), None);
	assert_eq!(shape.try_get::<bool>(&row, 3), Some(true));
	assert_eq!(shape.try_get::<bool>(&row, 4), None);
}
