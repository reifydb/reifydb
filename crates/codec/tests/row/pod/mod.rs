// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	pod::{EncodedPodRow, POD_HEADER_SIZE},
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{util::cowvec::CowVec, value::value_type::ValueType};

fn entry(id: u128, value: &[u8]) -> Vec<u8> {
	let mut buffer = Vec::with_capacity(16 + value.len());
	buffer.extend_from_slice(&id.to_be_bytes());
	buffer.extend_from_slice(value);
	buffer
}

#[test]
fn the_family_reserves_no_header_so_offset_zero_is_payload() {
	// A non-zero header here shifts every pod read, and the id at offset 0 would decode short.
	assert_eq!(POD_HEADER_SIZE, 0);
	assert_eq!(RowFamily::Pod.header_size(), 0);
}

#[test]
fn the_body_is_the_whole_row() {
	// Any byte withheld from the body is a byte of the interned id or value that a reader never sees.
	let raw = entry(7, b"reifydb");
	let row = EncodedPodRow::new(&raw);

	assert_eq!(row.body(), raw.as_slice());
	assert_eq!(row.len(), raw.len());
	assert_eq!(row.as_slice(), raw.as_slice());
}

#[test]
fn a_viewed_entry_yields_the_id_written_at_offset_zero() {
	// Reading the id anywhere but offset 0 resolves value bytes as an id and interns a collision.
	let raw = entry(0x0123_4567_89ab_cdef, b"value");
	let bytes = EncodedBytes(CowVec::new(raw));

	let row = EncodedPodRow::view(&bytes);

	let id = u128::from_be_bytes(row.body()[..16].try_into().unwrap());
	assert_eq!(id, 0x0123_4567_89ab_cdef);
	assert_eq!(&row.body()[16..], b"value");
}

#[test]
fn conversion_to_bytes_and_back_preserves_every_byte() {
	// EncodedBytes crosses the storage boundary, so a conversion that pads or trims corrupts the entry.
	let raw = entry(42, b"payload");
	let original = EncodedBytes(CowVec::new(raw.clone()));

	let round_tripped: EncodedBytes = EncodedPodRow::from(original).into();

	assert_eq!(round_tripped.as_slice(), raw.as_slice());
}

#[test]
fn an_empty_row_is_accepted_because_no_header_is_required() {
	// The header-bearing families reject short buffers; a dictionary index value may legitimately be empty.
	let row = EncodedPodRow::new(&[]);

	assert!(row.is_empty());
	assert_eq!(row.len(), 0);
	assert_eq!(row.body(), b"");
}

#[test]
fn body_mut_edits_in_place_without_resizing() {
	// An in-place edit that shifts length would desynchronise the entry from its index key.
	let raw = entry(1, b"before");
	let mut row = EncodedPodRow::new(&raw);
	let before = row.len();

	row.body_mut()[..16].copy_from_slice(&2u128.to_be_bytes());

	assert_eq!(row.len(), before);
	assert_eq!(u128::from_be_bytes(row.body()[..16].try_into().unwrap()), 2);
	assert_eq!(&row.body()[16..], b"before");
}

#[test]
fn thawing_a_frozen_entry_is_the_only_way_to_change_its_length() {
	// body_mut cannot resize, so a pod entry that must grow has no path back to storage without a thaw.
	let raw = entry(1, b"before");
	let frozen = EncodedPodRow::new(&raw);

	let mut thawed = frozen.thaw();
	assert_eq!(thawed.as_slice(), raw.as_slice(), "the thawed buffer starts at payload, never at a header");
	thawed.as_mut_slice()[..16].copy_from_slice(&2u128.to_be_bytes());
	thawed.extend_from_slice(b"-more");
	let refrozen = thawed.freeze();

	assert_eq!(u128::from_be_bytes(refrozen.body()[..16].try_into().unwrap()), 2);
	assert_eq!(&refrozen.body()[16..], b"before-more");
	assert_eq!(refrozen.len(), raw.len() + 5, "an appended tail must extend the entry, not overwrite it");
}

#[test]
#[should_panic(expected = "allocate_pod on a shape of another family")]
fn a_shape_of_another_family_cannot_allocate_a_pod_row() {
	// A table shape stamps a fingerprint at offset zero, which a pod entry hands back as its first payload bytes.
	RowShape::new(RowFamily::Table, vec![RowShapeField::unconstrained("value", ValueType::Blob)]).allocate_pod();
}
