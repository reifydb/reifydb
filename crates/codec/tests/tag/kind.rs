// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::ValueKind;

#[test]
fn the_all_array_is_ordered_by_discriminant() {
	// from_byte decodes by indexing ALL, so array order is the wire contract; nothing in the
	// language ties it to the `= N` discriminants.
	for (index, kind) in ValueKind::ALL.iter().enumerate() {
		assert_eq!(
			kind.byte(),
			index as u8,
			"ValueKind::{kind:?} sits at ALL[{index}] but carries discriminant {}",
			kind.byte()
		);
	}
}

#[test]
fn every_variant_survives_an_encode_decode_round_trip() {
	// Every extern-C column header and type tag writes `kind.byte()` and reads it back through
	// from_byte, so the two must be inverse for every variant or a plugin decodes garbage.
	for kind in ValueKind::ALL {
		assert_eq!(ValueKind::from_byte(kind.byte()), Some(kind));
	}
}

#[test]
fn a_byte_outside_the_table_does_not_decode() {
	// Returning Some would let a truncated or future-versioned payload decode as whatever
	// variant happens to sit at that index.
	assert_eq!(ValueKind::from_byte(ValueKind::ALL.len() as u8), None);
	assert_eq!(ValueKind::from_byte(u8::MAX), None);
}
