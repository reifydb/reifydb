// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::{
	bytes::EncodedBytes,
	operator::{
		EncodedOperatorRow, OPERATOR_HEADER_SIZE, OperatorError, OperatorState, decode, decode_body, encode,
	},
};
use reifydb_value::{factory::time::at_nanos, util::cowvec::CowVec, value::datetime::DateTime};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Probe {
	total: u64,
	names: Vec<String>,
}

fn probe() -> Probe {
	Probe {
		total: 42,
		names: vec!["a".to_string(), "bb".to_string()],
	}
}

#[test]
fn test_encode_decode_round_trip() {
	// Encode -> decode must be lossless at every step, or a flush loses state.
	let value = probe();
	let row = encode(&value, at_nanos(7)).unwrap();

	let restored: Probe = decode_body(&row).unwrap();
	assert_eq!(restored, value);
	assert_eq!(restored.total, 42);
	assert_eq!(restored.names.len(), 2);
	assert_eq!(restored.names[1].as_str(), "bb");
}

#[test]
fn test_set_time_preserves_body() {
	// set_time must write exactly the header window, never the first byte of the payload.
	let value = probe();
	let mut row = encode(&value, at_nanos(7)).unwrap();
	assert_eq!(row.time(), at_nanos(7));
	let body = row.body().to_vec();

	row.set_time(at_nanos(99));
	assert_eq!(row.time(), at_nanos(99));
	assert_eq!(row.body(), &body[..], "the body must stay untouched");
	assert_eq!(decode_body::<Probe>(&row).unwrap().total, 42);
}

#[test]
fn test_body_mut_windows_the_same_bytes_as_body() {
	// body_mut must window exactly body(), otherwise a write lands outside the payload.
	let value = probe();
	let mut row = encode(&value, DateTime::EPOCH).unwrap();
	let body = row.body().to_vec();
	assert_eq!(row.body_mut(), &body[..]);
	assert_eq!(row.body(), &body[..]);
}

#[test]
fn test_row_round_trip_preserves_time() {
	// The store boundary must preserve the time header, otherwise floor expiry reads garbage.
	let row = encode(&probe(), at_nanos(1234)).unwrap();
	let encoded = row.clone().into_bytes();

	let reloaded = EncodedOperatorRow::try_from(encoded).unwrap();
	assert_eq!(reloaded, row);
	assert_eq!(reloaded.time(), at_nanos(1234));
	assert_eq!(decode_body::<Probe>(&reloaded).unwrap().total, 42);
}

#[test]
fn test_try_from_rejects_a_row_too_short_to_hold_the_header() {
	// A short row must error here, otherwise time() indexes out of bounds downstream.
	for len in 0..OPERATOR_HEADER_SIZE {
		let short = EncodedBytes(CowVec::new(vec![0u8; len]));
		assert_eq!(
			EncodedOperatorRow::try_from(short).unwrap_err(),
			OperatorError::Truncated {
				len,
			}
		);
	}
}

#[test]
fn test_a_body_of_set_bits_fails_to_decode() {
	// Every 0xFF byte sets the varint continuation bit, so the leading field must overrun.
	let corrupt = EncodedOperatorRow::new(&[0xFFu8; 16], DateTime::EPOCH);
	assert!(matches!(decode_body::<Probe>(&corrupt), Err(OperatorError::Deserialization(_))));
}

#[test]
fn test_a_length_prefix_past_the_end_of_the_body_fails_to_decode() {
	// A collection length larger than the remaining bytes must error, never read past the body.
	let overlong = EncodedOperatorRow::new(&[0x00, 0x01, 0x7F], DateTime::EPOCH);
	assert!(matches!(decode_body::<Probe>(&overlong), Err(OperatorError::Deserialization(_))));
}

#[test]
fn test_truncated_body_fails_to_decode() {
	// This is the disk-corruption trust boundary: a short body must error, not panic.
	let row = encode(&probe(), DateTime::EPOCH).unwrap();
	let body = row.body();
	let truncated = EncodedOperatorRow::new(&body[..body.len() / 2], DateTime::EPOCH);
	assert!(matches!(decode_body::<Probe>(&truncated), Err(OperatorError::Deserialization(_))));
}

#[test]
fn test_timeless_rows_sort_above_every_cutoff() {
	// Absence is DateTime::MAX, which must outrank any cutoff a floor sweep can propose.
	let row = EncodedOperatorRow::timeless(&[]);
	assert_eq!(row.time(), DateTime::MAX);
	assert!(row.time() > at_nanos(u64::MAX - 1));
	assert!(row.body().is_empty());
}

#[test]
fn test_byte_size_covers_header_and_body() {
	let row = encode(&probe(), DateTime::EPOCH).unwrap();
	assert_eq!(row.byte_size().as_bytes(), row.len() as u64);
	assert_eq!(row.len(), OPERATOR_HEADER_SIZE + row.body().len());
}

#[test]
fn test_new_body_round_trips_exactly() {
	let payload: Vec<u8> = (0..=255).collect();
	let row = EncodedOperatorRow::new(&payload, at_nanos(7));
	assert_eq!(row.body(), payload.as_slice());
	assert_eq!(row.time(), at_nanos(7));
}

#[test]
fn test_consecutive_encodes_share_a_buffer_without_bleed() {
	let big = Probe {
		total: 1,
		names: (0..64).map(|i| format!("name-{i}")).collect(),
	};
	let small = Probe {
		total: 2,
		names: vec!["x".to_string()],
	};
	let big_row = encode(&big, DateTime::EPOCH).unwrap();
	let small_row = encode(&small, DateTime::EPOCH).unwrap();
	assert!(small_row.body().len() < big_row.body().len());
	let restored: Probe = decode_body::<Probe>(&small_row).unwrap();
	assert_eq!(restored, small);
	let restored_big: Probe = decode_body::<Probe>(&big_row).unwrap();
	assert_eq!(restored_big, big);
}

#[test]
fn test_a_map_round_trips_in_key_order() {
	// Operators keep their buffers in a map, so insertion order must never leak into the decode.
	let map: BTreeMap<u64, i64> = [(3u64, 30i64), (1, 10), (2, 20)].into_iter().collect();

	let row = map.encode_state(DateTime::EPOCH).expect("encode");
	let restored: BTreeMap<u64, i64> = decode(&row).expect("decode");

	assert_eq!(restored, map);
	assert_eq!(restored.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
}

#[test]
fn test_an_empty_map_round_trips() {
	// Empty is the state every group starts in; a decode failure would break every first write.
	let map: BTreeMap<u64, i64> = BTreeMap::new();

	let row = map.encode_state(DateTime::EPOCH).expect("encode");
	let restored: BTreeMap<u64, i64> = decode(&row).expect("decode");

	assert!(restored.is_empty());
}

#[test]
fn test_a_datetime_round_trips_as_operator_state() {
	// the timer index stores an instant as its own state row, so a lossy leg would name a wheel row that is not
	// there
	for instant in [DateTime::EPOCH, at_nanos(1), at_nanos(1_700_000_000_123_456_789), DateTime::MAX] {
		let row = instant.encode_state(DateTime::EPOCH).expect("encode");
		let restored: DateTime = decode(&row).expect("decode");

		assert_eq!(restored, instant);
	}
}

#[test]
fn test_two_instants_inside_one_millisecond_stay_distinct_as_operator_state() {
	// a millisecond-resolution payload collapses these, so a re-arm reads as already armed and the timer never
	// fires
	let earlier = at_nanos(1_700_000_000_000_000_000);
	let later = at_nanos(1_700_000_000_000_500_000);
	assert_eq!(earlier.to_epoch_millis(), later.to_epoch_millis(), "fixture must share a millisecond");

	let restored_earlier: DateTime = decode(&earlier.encode_state(DateTime::EPOCH).unwrap()).unwrap();
	let restored_later: DateTime = decode(&later.encode_state(DateTime::EPOCH).unwrap()).unwrap();

	assert_ne!(restored_earlier, restored_later);
	assert_eq!(restored_later, later);
}
