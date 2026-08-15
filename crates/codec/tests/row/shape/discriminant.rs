// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowFamily;

const DISCRIMINANTS: [(u8, RowFamily); 9] = [
	(0x01, RowFamily::Catalog),
	(0x02, RowFamily::Operator),
	(0x03, RowFamily::Pod),
	(0x04, RowFamily::Table),
	(0x05, RowFamily::Series),
	(0x06, RowFamily::RingBuffer),
	(0x07, RowFamily::Queue),
	(0x08, RowFamily::QueueAttempt),
	(0x09, RowFamily::QueueDeduplication),
];

#[test]
fn every_family_decodes_back_from_the_byte_it_encodes_to() {
	// Renumbering the enum without from_u8 leaves both halves valid and reads every stored shape as another family.
	for (byte, family) in DISCRIMINANTS {
		assert_eq!(family as u8, byte, "{family:?} no longer encodes to {byte:#04x}");
		assert_eq!(RowFamily::from_u8(byte), Some(family), "{byte:#04x} no longer decodes to {family:?}");
	}
}

#[test]
fn exactly_nine_of_the_two_hundred_fifty_six_bytes_name_a_family() {
	// A corrupt or newer-version header must resolve to none, never to a neighbouring family's layout.
	assert_eq!(RowFamily::from_u8(0x00), None, "zero is not a family and must not decode as Catalog");
	assert_eq!(RowFamily::from_u8(0x0A), None, "the byte after the last family must stay unassigned");
	assert_eq!(RowFamily::from_u8(0xFF), None, "an all-ones byte is the classic corruption pattern");

	let accepted: Vec<u8> = (0..=u8::MAX).filter(|byte| RowFamily::from_u8(*byte).is_some()).collect();

	assert_eq!(accepted, DISCRIMINANTS.iter().map(|(byte, _)| *byte).collect::<Vec<u8>>());
}
