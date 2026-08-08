// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::E;

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey, serializer::KeySerializer};
use reifydb_value::value::{date::Date, datetime::DateTime, duration::Duration, row_number::RowNumber, time::Time};

#[test]
fn test_read_bool() {
	let mut ser = KeySerializer::new();
	ser.extend_bool(true).extend_bool(false);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_bool().unwrap(), true);
	assert_eq!(de.read_bool().unwrap(), false);
	assert!(de.is_empty());
}

#[test]
fn test_read_integers() {
	let mut ser = KeySerializer::new();
	ser.extend_i8(-42i8).extend_i16(-1000i16).extend_i32(100000i32).extend_i64(-1000000000i64);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_i8().unwrap(), -42);
	assert_eq!(de.read_i16().unwrap(), -1000);
	assert_eq!(de.read_i32().unwrap(), 100000);
	assert_eq!(de.read_i64().unwrap(), -1000000000);
	assert!(de.is_empty());
}

#[test]
fn test_read_unsigned() {
	let mut ser = KeySerializer::new();
	ser.extend_u8(255u8).extend_u16(65535u16).extend_u32(4294967295u32).extend_u64(18446744073709551615u64);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_u8().unwrap(), 255);
	assert_eq!(de.read_u16().unwrap(), 65535);
	assert_eq!(de.read_u32().unwrap(), 4294967295);
	assert_eq!(de.read_u64().unwrap(), 18446744073709551615);
	assert!(de.is_empty());
}

#[test]
fn test_read_floats() {
	let mut ser = KeySerializer::new();
	ser.extend_f32(3.14).extend_f64(E);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert!((de.read_f32().unwrap() - 3.14).abs() < 0.001);
	assert!((de.read_f64().unwrap() - E).abs() < 0.000001);
	assert!(de.is_empty());
}

#[test]
fn test_read_bytes() {
	let mut ser = KeySerializer::new();
	ser.extend_bytes(b"hello").extend_bytes(&[0x01, 0xff, 0x02]);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_bytes().unwrap(), b"hello");
	assert_eq!(de.read_bytes().unwrap(), vec![0x01, 0xff, 0x02]);
	assert!(de.is_empty());
}

#[test]
fn test_read_str() {
	let mut ser = KeySerializer::new();
	ser.extend_str("hello world").extend_str("👋");
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_str().unwrap(), "hello world");
	assert_eq!(de.read_str().unwrap(), "👋");
	assert!(de.is_empty());
}

#[test]
fn test_read_date() {
	let mut ser = KeySerializer::new();
	let date = Date::from_ymd(2024, 1, 1).unwrap();
	ser.extend_date(&date);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_date().unwrap(), date);
	assert!(de.is_empty());
}

#[test]
fn test_read_datetime() {
	let mut ser = KeySerializer::new();
	let datetime = DateTime::from_ymd_hms(2024, 1, 1, 12, 30, 45).unwrap();
	ser.extend_datetime(&datetime);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_datetime().unwrap(), datetime);
	assert!(de.is_empty());
}

#[test]
fn test_read_time() {
	let mut ser = KeySerializer::new();
	let time = Time::from_hms(12, 30, 45).unwrap();
	ser.extend_time(&time);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_time().unwrap(), time);
	assert!(de.is_empty());
}

#[test]
fn test_read_duration() {
	let mut ser = KeySerializer::new();
	let duration = Duration::from_nanoseconds(1000000).unwrap();
	ser.extend_duration(&duration);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_duration().unwrap(), duration);
	assert!(de.is_empty());
}

#[test]
fn test_keycode_roundtrip_with_months_and_days() {
	let mut ser = KeySerializer::new();
	let duration = Duration::new(12, 5, 1_000_000_000).unwrap();
	ser.extend_duration(&duration);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_duration().unwrap(), duration);
	assert!(de.is_empty());
}

#[test]
fn test_keycode_different_durations_produce_different_keys() {
	let d1 = Duration::new(12, 0, 0).unwrap();
	let d2 = Duration::zero();

	let mut s1 = KeySerializer::new();
	s1.extend_duration(&d1);
	let b1 = s1.finish();

	let mut s2 = KeySerializer::new();
	s2.extend_duration(&d2);
	let b2 = s2.finish();

	assert_ne!(b1, b2);
}

#[test]
fn test_keycode_duration_ordering_preserved() {
	// Keycode is descending: a larger Duration encodes to smaller bytes.
	let durations = vec![
		Duration::new(0, 0, 0).unwrap(),
		Duration::new(0, 0, 1_000_000_000).unwrap(),
		Duration::new(0, 1, 0).unwrap(),
		Duration::new(1, 0, 0).unwrap(),
		Duration::new(12, 30, 0).unwrap(),
	];

	let keys: Vec<EncodedKey> = durations
		.iter()
		.map(|d| {
			let mut ser = KeySerializer::new();
			ser.extend_duration(d);
			ser.finish()
		})
		.collect();

	for i in 0..keys.len() - 1 {
		assert!(
			keys[i] > keys[i + 1],
			"Key ordering broken: {:?} key should be > {:?} key (descending encoding)",
			durations[i],
			durations[i + 1]
		);
	}
}

#[test]
fn test_read_row_number() {
	let mut ser = KeySerializer::new();
	let row = RowNumber(42);
	ser.extend_row_number(&row);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_row_number().unwrap(), row);
	assert!(de.is_empty());
}

#[test]
fn test_position_tracking() {
	let mut ser = KeySerializer::new();
	ser.extend_u8(1u8).extend_u16(2u16).extend_u32(3u32);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.position(), 0);
	assert_eq!(de.remaining(), 4);

	de.read_u8().unwrap();
	assert_eq!(de.position(), 1);
	assert_eq!(de.remaining(), 3);

	de.read_u16().unwrap();
	assert_eq!(de.position(), 3);
	assert_eq!(de.remaining(), 1);

	de.read_u32().unwrap();
	assert_eq!(de.position(), 4);
	assert_eq!(de.remaining(), 0);
	assert!(de.is_empty());
}

#[test]
fn test_error_on_insufficient_bytes() {
	let bytes = vec![0x00, 0x01];
	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert!(de.read_u32().is_err());
}

#[test]
fn test_chaining() {
	let mut ser = KeySerializer::new();
	ser.extend_bool(true).extend_i32(42i32).extend_str("test").extend_u64(1000u64);
	let bytes = ser.finish();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_bool().unwrap(), true);
	assert_eq!(de.read_i32().unwrap(), 42);
	assert_eq!(de.read_str().unwrap(), "test");
	assert_eq!(de.read_u64().unwrap(), 1000);
	assert!(de.is_empty());
}
