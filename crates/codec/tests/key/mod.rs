// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod container_keys;
mod deserializer;
mod encoded;
mod serializer;

use reifydb_codec::key::{serializer::KeySerializer, *};
use reifydb_value::value::datetime::DateTime;

#[test]
fn test_u128_varint_roundtrip_and_descending_order() {
	let values: Vec<u128> = vec![
		0,
		1,
		2,
		126,
		127,
		128,
		129,
		(1 << 14) - 1,
		1 << 14,
		(1 << 21) - 1,
		1 << 21,
		(1 << 28) - 1,
		1 << 28,
		(1 << 35) - 1,
		1 << 35,
		(1 << 42) - 1,
		1 << 42,
		(1 << 49) - 1,
		1 << 49,
		(1 << 56) - 1,
		1 << 56,
		(1u128 << 63) - 1,
		1u128 << 63,
		u64::MAX as u128 - 1,
		u64::MAX as u128,
		u64::MAX as u128 + 1,
		1u128 << 100,
		u128::MAX - 1,
		u128::MAX,
	];

	for &v in &values {
		let mut buf = Vec::new();
		encode_u128_varint(v, &mut buf);
		let mut slice = buf.as_slice();
		let decoded = decode_u128_varint(&mut slice).unwrap();
		assert_eq!(decoded, v, "roundtrip failed for {}", v);
		assert!(slice.is_empty(), "trailing bytes after decoding {}", v);
	}

	// keycode is descending: a strictly larger value must encode to a lexicographically smaller key.
	let mut sorted = values.clone();
	sorted.sort();
	sorted.dedup();
	let mut prev: Option<(u128, Vec<u8>)> = None;
	for &v in &sorted {
		let mut buf = Vec::new();
		encode_u128_varint(v, &mut buf);
		if let Some((pv, pe)) = &prev {
			assert!(*pv < v);
			assert!(buf < *pe, "not descending: {} -> {:?} should sort before {} -> {:?}", v, buf, pv, pe);
		}
		prev = Some((v, buf));
	}
}

#[test]
fn test_key_serializer() {
	let mut s = KeySerializer::new();
	s.extend_bool(true);
	assert_eq!(s.finish(), vec![0x00]);

	let mut s = KeySerializer::new();
	s.extend_bool(false);
	assert_eq!(s.finish(), vec![0x01]);

	let mut s = KeySerializer::new();
	s.extend_u64(0u64);
	assert_eq!(s.finish(), vec![0xff; 8]);

	let mut s = KeySerializer::new();
	s.extend_i64(0i64);
	assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

	let mut s = KeySerializer::new();
	s.extend_f32(0.0f32);
	assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff]);

	let mut s = KeySerializer::new();
	s.extend_f64(0.0f64);
	assert_eq!(s.finish(), vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

	let mut s = KeySerializer::new();
	s.extend_bytes(b"foo");
	assert_eq!(s.finish(), vec![0x66, 0x6f, 0x6f, 0xff, 0xff]);

	let mut s = KeySerializer::with_capacity(32);
	s.extend_bool(true).extend_u32(1u32).extend_i16(-1i16).extend_bytes(b"test");
	let result = s.finish();
	assert!(!result.is_empty());
	assert!(result.len() >= 10);
}

#[test]
fn the_two_u64_key_encodings_sort_in_opposite_directions() {
	// encode_u64 is inverted for the descending row keycode; encode_u64_asc stays plain for forward scans.
	let ordered = [0u64, 1, 255, 256, 65_535, 65_536, u64::MAX - 1, u64::MAX];

	for pair in ordered.windows(2) {
		let (lo, hi) = (pair[0], pair[1]);
		assert!(encode_u64_asc(lo) < encode_u64_asc(hi), "ascending encoding must keep {lo} below {hi}");
		assert!(encode_u64(lo) > encode_u64(hi), "inverted encoding must put {lo} above {hi}");
	}
}

#[test]
fn every_fixed_width_key_encoding_round_trips() {
	// a decoder that misses or doubles the inversion must not silently read a wrong value instead of failing.
	for value in [0u64, 1, 255, 256, 1_700_000_000_000, u64::MAX - 1, u64::MAX] {
		assert_eq!(decode_u64(encode_u64(value)), value, "inverted u64 round trip");
		assert_eq!(decode_u64_asc(encode_u64_asc(value)), value, "ascending u64 round trip");
	}
	for value in [0u128, 1, u128::from(u64::MAX), u128::MAX] {
		assert_eq!(decode_u128_asc(encode_u128_asc(value)), value, "ascending u128 round trip");
	}
}

#[test]
fn a_datetime_key_round_trips_through_its_encoding() {
	// the encoding is the only path an instant takes into a key, so a lossy leg silently moves every timer armed at
	// it
	for bits in [0u64, 1, 999_999, 1_000_000, 1_700_000_000_123_456_789, u64::MAX - 1, u64::MAX] {
		let instant = DateTime::from_bits(bits);
		assert_eq!(decode_datetime_asc(encode_datetime_asc(instant)), instant, "datetime key round trip");
	}
}

#[test]
fn datetime_keys_sort_in_instant_order() {
	// a range scan reads firing order straight off these bytes, so an encoding that reorders fires timers out of
	// order
	let ordered = [
		DateTime::from_bits(0),
		DateTime::from_bits(1),
		DateTime::from_bits(999_999),
		DateTime::from_bits(1_000_000),
		DateTime::from_bits(1_000_001),
		DateTime::from_bits(u64::MAX),
	];

	for pair in ordered.windows(2) {
		let (lo, hi) = (pair[0], pair[1]);
		assert!(lo < hi, "fixture must be ordered");
		assert!(encode_datetime_asc(lo) < encode_datetime_asc(hi), "encoding must keep {lo:?} below {hi:?}");
	}
}

#[test]
fn two_instants_inside_one_millisecond_encode_to_distinct_keys() {
	// a millisecond-resolution key collapses these to one row, so the second arm silently overwrites the first
	let earlier = DateTime::from_bits(1_700_000_000_000_000_000);
	let later = DateTime::from_bits(1_700_000_000_000_500_000);

	assert_eq!(earlier.to_epoch_millis(), later.to_epoch_millis(), "fixture must share a millisecond");
	assert_ne!(encode_datetime_asc(earlier), encode_datetime_asc(later));
	assert_eq!(decode_datetime_asc(encode_datetime_asc(later)), later);
}
