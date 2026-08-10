// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::align_of;

use reifydb_codec::row::{
	bytes::EncodedBytes,
	operator::{EncodedOperatorRow, OPERATOR_HEADER_SIZE, OperatorError, decode_archive, encode_archive},
};
use reifydb_value::{factory::time::at_nanos, util::cowvec::CowVec, value::datetime::DateTime};
use rkyv::{
	Archive, Deserialize, Serialize, access, deserialize,
	primitive::{ArchivedF64, ArchivedI64, ArchivedU64},
	rancor::Error as TestRancorError,
};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
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
	// Encode -> validate -> decode must be lossless at every step, or a flush loses state.
	let value = probe();
	let row = encode_archive(&value, at_nanos(7)).unwrap();

	let restored: Probe = decode_archive(&row).unwrap();
	assert_eq!(restored, value);
	assert_eq!(restored.total, 42);
	assert_eq!(restored.names.len(), 2);
	assert_eq!(restored.names[1].as_str(), "bb");
}

#[test]
fn test_set_time_preserves_body() {
	// set_time must write exactly the header window, never the first byte of the archive.
	let value = probe();
	let mut row = encode_archive(&value, at_nanos(7)).unwrap();
	assert_eq!(row.time(), at_nanos(7));
	let body = row.body().to_vec();

	row.set_time(at_nanos(99));
	assert_eq!(row.time(), at_nanos(99));
	assert_eq!(row.body(), &body[..], "the body must stay untouched");
	assert_eq!(decode_archive::<Probe>(&row).unwrap().total, 42);
}

#[test]
fn test_body_mut_windows_the_same_bytes_as_body() {
	// body_mut must window exactly body(), otherwise a write lands outside the archive.
	let value = probe();
	let mut row = encode_archive(&value, DateTime::EPOCH).unwrap();
	let body = row.body().to_vec();
	assert_eq!(row.body_mut(), &body[..]);
	assert_eq!(row.body(), &body[..]);
}

#[test]
fn test_archived_access_is_alignment_free() {
	// The body starts at byte 8 of a plain Vec, so archived primitives must stay align-1.
	const _: () = assert!(align_of::<ArchivedU64>() == 1);
	const _: () = assert!(align_of::<ArchivedI64>() == 1);
	const _: () = assert!(align_of::<ArchivedF64>() == 1);

	let value = probe();
	let row = encode_archive(&value, at_nanos(7)).unwrap();
	let body = row.body().to_vec();
	for offset in 1..8usize {
		let mut buffer = vec![0u8; offset];
		buffer.extend_from_slice(&body);
		let archived = access::<ArchivedProbe, TestRancorError>(&buffer[offset..]).unwrap_or_else(|e| {
			panic!("archived access must not require alignment (offset {offset}): {e}")
		});
		assert_eq!(archived.total, 42);
		let restored: Probe = deserialize::<Probe, TestRancorError>(archived).unwrap();
		assert_eq!(restored, value, "round trip from misaligned offset {offset}");
	}
}

#[test]
fn test_row_round_trip_preserves_time() {
	// The store boundary must preserve the time header, otherwise floor expiry reads garbage.
	let row = encode_archive(&probe(), at_nanos(1234)).unwrap();
	let encoded = row.clone().into_bytes();

	let reloaded = EncodedOperatorRow::try_from(encoded).unwrap();
	assert_eq!(reloaded, row);
	assert_eq!(reloaded.time(), at_nanos(1234));
	assert_eq!(decode_archive::<Probe>(&reloaded).unwrap().total, 42);
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
fn test_zeroed_row_fails_archive_validation() {
	// A zeroed body must fail bytecheck rather than be read as a valid archive.
	let zeroed = EncodedOperatorRow::new(&[0u8; 16], DateTime::EPOCH);
	assert!(matches!(decode_archive::<Probe>(&zeroed), Err(OperatorError::Validation(_))));
}

#[test]
fn test_truncated_body_fails_validation() {
	// This is the disk-corruption trust boundary: bytecheck must error, not panic.
	let row = encode_archive(&probe(), DateTime::EPOCH).unwrap();
	let body = row.body();
	let truncated = EncodedOperatorRow::new(&body[..body.len() / 2], DateTime::EPOCH);
	assert!(matches!(decode_archive::<Probe>(&truncated), Err(OperatorError::Validation(_))));
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
	let row = encode_archive(&probe(), DateTime::EPOCH).unwrap();
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
	let big_row = encode_archive(&big, DateTime::EPOCH).unwrap();
	let small_row = encode_archive(&small, DateTime::EPOCH).unwrap();
	assert!(small_row.body().len() < big_row.body().len());
	let restored: Probe = decode_archive::<Probe>(&small_row).unwrap();
	assert_eq!(restored, small);
	let restored_big: Probe = decode_archive::<Probe>(&big_row).unwrap();
	assert_eq!(restored_big, big);
}
