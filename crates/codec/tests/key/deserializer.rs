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
	assert_eq!(de.remaining(), 7);

	de.read_u8().unwrap();
	assert_eq!(de.position(), 1);
	assert_eq!(de.remaining(), 6);

	de.read_u16().unwrap();
	assert_eq!(de.position(), 3);
	assert_eq!(de.remaining(), 4);

	de.read_u32().unwrap();
	assert_eq!(de.position(), 7);
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

fn keys_for<T: Copy>(values: &[T], extend: impl Fn(&mut KeySerializer, T)) -> Vec<EncodedKey> {
	// Encodes each value independently so the comparison below sees whole keys, not shared prefixes.
	values.iter()
		.map(|value| {
			let mut ser = KeySerializer::new();
			extend(&mut ser, *value);
			ser.finish()
		})
		.collect()
}

fn assert_strictly_descending(keys: &[EncodedKey], width: &str) {
	// Range scans walk keys in byte order, so an inverted or equal pair silently returns the wrong rows.
	for i in 0..keys.len() - 1 {
		assert!(
			keys[i] > keys[i + 1],
			"{width}: value at index {i} must encode above index {}, keycode is descending",
			i + 1
		);
	}
}

#[test]
fn test_read_u128_roundtrip_at_boundaries() {
	// u128 reads had no coverage, and a 16-byte decode is where a dropped or swapped half hides.
	for value in [0u128, 1, u64::MAX as u128, u64::MAX as u128 + 1, u128::MAX] {
		let mut ser = KeySerializer::new();
		ser.extend_u128(value);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_u128().unwrap(), value, "u128 {value} must survive a key round trip");
		assert!(de.is_empty(), "u128 must consume exactly its 16 bytes");
	}
}

#[test]
fn test_read_i128_roundtrip_at_boundaries() {
	// i128 reads had no coverage, and MIN and -1 are where an off-by-one sign flip surfaces.
	for value in [i128::MIN, -1i128, 0, 1, i128::MAX] {
		let mut ser = KeySerializer::new();
		ser.extend_i128(value);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_i128().unwrap(), value, "i128 {value} must survive a key round trip");
		assert!(de.is_empty(), "i128 must consume exactly its 16 bytes");
	}
}

#[test]
fn test_unsigned_keys_descend_as_values_ascend() {
	// Every unsigned width shares one inversion, so a regression in any single width breaks range scans.
	assert_strictly_descending(
		&keys_for(&[0u8, 1, 127, 128, u8::MAX], |s, v| {
			s.extend_u8(v);
		}),
		"u8",
	);
	assert_strictly_descending(
		&keys_for(&[0u16, 1, 255, 256, u16::MAX], |s, v| {
			s.extend_u16(v);
		}),
		"u16",
	);
	assert_strictly_descending(
		&keys_for(&[0u32, 1, 65535, 65536, u32::MAX], |s, v| {
			s.extend_u32(v);
		}),
		"u32",
	);
	assert_strictly_descending(
		&keys_for(&[0u64, 1, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX], |s, v| {
			s.extend_u64(v);
		}),
		"u64",
	);
	assert_strictly_descending(
		&keys_for(&[0u128, 1, u64::MAX as u128, u64::MAX as u128 + 1, u128::MAX], |s, v| {
			s.extend_u128(v);
		}),
		"u128",
	);
}

#[test]
fn test_signed_keys_descend_as_values_ascend() {
	// The sign-bit flip is what sorts negatives below positives; without it a scan reads them as larger.
	assert_strictly_descending(
		&keys_for(&[i8::MIN, -1i8, 0, 1, i8::MAX], |s, v| {
			s.extend_i8(v);
		}),
		"i8",
	);
	assert_strictly_descending(
		&keys_for(&[i16::MIN, -1i16, 0, 1, i16::MAX], |s, v| {
			s.extend_i16(v);
		}),
		"i16",
	);
	assert_strictly_descending(
		&keys_for(&[i32::MIN, -1i32, 0, 1, i32::MAX], |s, v| {
			s.extend_i32(v);
		}),
		"i32",
	);
	assert_strictly_descending(
		&keys_for(&[i64::MIN, -1i64, 0, 1, i64::MAX], |s, v| {
			s.extend_i64(v);
		}),
		"i64",
	);
	assert_strictly_descending(
		&keys_for(&[i128::MIN, -1i128, 0, 1, i128::MAX], |s, v| {
			s.extend_i128(v);
		}),
		"i128",
	);
}

#[test]
fn test_float_keys_descend_as_values_ascend() {
	// Negative floats take a different encode branch than non-negative, so the halves must still join in order.
	assert_strictly_descending(
		&keys_for(
			&[
				f32::NEG_INFINITY,
				f32::MIN,
				-1.5f32,
				-0.0,
				0.0,
				f32::MIN_POSITIVE,
				1.5,
				f32::MAX,
				f32::INFINITY,
			],
			|s, v| {
				s.extend_f32(v);
			},
		),
		"f32",
	);
	assert_strictly_descending(
		&keys_for(
			&[
				f64::NEG_INFINITY,
				f64::MIN,
				-1.5f64,
				-0.0,
				0.0,
				f64::MIN_POSITIVE,
				1.5,
				f64::MAX,
				f64::INFINITY,
			],
			|s, v| {
				s.extend_f64(v);
			},
		),
		"f64",
	);
}

#[test]
fn test_float_keys_round_trip_exact_bits() {
	// A tolerance comparison cannot catch a mantissa, signed-zero, or NaN-payload regression; bits can.
	for value in [f32::NEG_INFINITY, f32::MIN, -1.5f32, -0.0, 0.0, f32::MIN_POSITIVE, f32::MAX, f32::NAN] {
		let mut ser = KeySerializer::new();
		ser.extend_f32(value);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_f32().unwrap().to_bits(), value.to_bits(), "f32 key must restore the exact bits");
	}
	for value in [f64::NEG_INFINITY, f64::MIN, -1.5f64, -0.0, 0.0, f64::MIN_POSITIVE, f64::MAX, f64::NAN] {
		let mut ser = KeySerializer::new();
		ser.extend_f64(value);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_f64().unwrap().to_bits(), value.to_bits(), "f64 key must restore the exact bits");
	}
}
