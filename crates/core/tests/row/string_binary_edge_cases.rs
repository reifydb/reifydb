// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! String and binary data edge case tests for the row encoding system

use reifydb_core::row::EncodedRowLayout;
use reifydb_type::*;

#[test]
fn test_utf8_special_sequences() {
	let layout = EncodedRowLayout::new(&[Type::Utf8]);

	let test_strings = [
		"",                 // Empty string
		"a",                // Single ASCII
		"α",                // 2-byte UTF-8
		"中",               // 3-byte UTF-8
		"𝄞",                // 4-byte UTF-8
		"\0",               // Null byte
		"a\0b",             // Embedded null
		"\n\r\t",           // Control characters
		"🎭🎨🎪",           // Emojis
		"\u{FEFF}",         // BOM
		"a\u{0301}",        // Combining character
		&"x".repeat(10000), // Large string
	];

	for &test_str in &test_strings {
		let mut row = layout.allocate_row();
		layout.set_utf8(&mut row, 0, test_str);
		let retrieved = layout.get_utf8(&row, 0);
		assert_eq!(
			retrieved, test_str,
			"Failed for string: {:?}",
			test_str
		);
	}
}

#[test]
fn test_blob_all_byte_values() {
	let layout = EncodedRowLayout::new(&[Type::Blob]);

	// Test all possible byte values
	let mut row = layout.allocate_row();
	let all_bytes: Vec<u8> = (0..=255).collect();
	layout.set_blob(&mut row, 0, &Blob::from(all_bytes.clone()));
	assert_eq!(layout.get_blob(&row, 0), Blob::from(all_bytes));

	// Test patterns that might confuse length encoding
	let patterns = [
		vec![0xff; 1000], // All 0xFF
		vec![0x00; 1000], // All nulls
		vec![0x80; 1000], // High bit set
		(0..255).cycle()
			.take(1000)
			.map(|x| x as u8)
			.collect::<Vec<_>>(),
	];

	// Create a new row for each pattern since dynamic fields can only be
	// set once
	for pattern in patterns {
		let mut row = layout.allocate_row();
		layout.set_blob(&mut row, 0, &Blob::from(pattern.clone()));
		assert_eq!(layout.get_blob(&row, 0), Blob::from(pattern));
	}
}

#[test]
fn test_dynamic_field_interleaving() {
	// Tests multiple dynamic fields to ensure they don't corrupt each other
	let layout = EncodedRowLayout::new(&[
		Type::Utf8,
		Type::Blob,
		Type::Utf8,
		Type::VarInt,
	]);

	// Test initial setting with various sizes
	let mut row = layout.allocate_row();
	layout.set_utf8(&mut row, 0, "first");
	layout.set_blob(&mut row, 1, &Blob::from(&b"second"[..]));
	layout.set_utf8(&mut row, 2, "third");
	layout.set_varint(&mut row, 3, &VarInt::from(999999999999i64));

	// Verify all are correct
	assert_eq!(layout.get_utf8(&row, 0), "first");
	assert_eq!(layout.get_blob(&row, 1), Blob::from(&b"second"[..]));
	assert_eq!(layout.get_utf8(&row, 2), "third");
	assert_eq!(layout.get_varint(&row, 3), VarInt::from(999999999999i64));

	// Test with different sizes in a new row (since dynamic fields can only
	// be set once)
	let mut row2 = layout.allocate_row();
	layout.set_utf8(&mut row2, 0, "much longer string than before");
	layout.set_blob(&mut row2, 1, &Blob::from(&b"x"[..]));
	layout.set_utf8(&mut row2, 2, "");
	layout.set_varint(&mut row2, 3, &VarInt::from(123i64));

	// Verify the second row
	assert_eq!(layout.get_utf8(&row2, 0), "much longer string than before");
	assert_eq!(layout.get_blob(&row2, 1), Blob::from(&b"x"[..]));
	assert_eq!(layout.get_utf8(&row2, 2), "");
	assert_eq!(layout.get_varint(&row2, 3), VarInt::from(123i64));

	// Verify the first row is still intact
	assert_eq!(layout.get_utf8(&row, 0), "first");
	assert_eq!(layout.get_blob(&row, 1), Blob::from(&b"second"[..]));
	assert_eq!(layout.get_utf8(&row, 2), "third");
	assert_eq!(layout.get_varint(&row, 3), VarInt::from(999999999999i64));
}
