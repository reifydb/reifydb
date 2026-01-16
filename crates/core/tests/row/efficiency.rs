// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Performance and scalability tests for the encoded encoding system

use reifydb_core::encoded::layout::EncodedValuesLayout;
use reifydb_type::value::{blob::Blob, date::Date, int::Int, r#type::Type, uuid::Uuid4};

#[test]
fn test_large_row() {
	// Test performance with many fields
	let field_counts = [10, 50, 100, 200, 500];

	for count in field_counts {
		let types: Vec<Type> = (0..count)
			.map(|i| match i % 10 {
				0 => Type::Boolean,
				1 => Type::Int1,
				2 => Type::Int2,
				3 => Type::Int4,
				4 => Type::Int8,
				5 => Type::Float4,
				6 => Type::Float8,
				7 => Type::Date,
				8 => Type::Uuid4,
				_ => Type::Utf8,
			})
			.collect();

		let layout = EncodedValuesLayout::new(&types);
		let mut row = layout.allocate();

		// Set all fields
		for i in 0..count {
			match i % 10 {
				0 => layout.set_bool(&mut row, i, true),
				1 => layout.set_i8(&mut row, i, 42),
				2 => layout.set_i16(&mut row, i, 1234i16),
				3 => layout.set_i32(&mut row, i, 123456),
				4 => layout.set_i64(&mut row, i, 1234567890),
				5 => layout.set_f32(&mut row, i, 3.14),
				6 => layout.set_f64(&mut row, i, 3.14159),
				7 => layout.set_date(&mut row, i, Date::from_ymd(2024, 12, 25).unwrap()),
				8 => layout.set_uuid4(&mut row, i, Uuid4::generate()),
				_ => layout.set_utf8(&mut row, i, "test"),
			}
		}

		// Read all fields
		for i in 0..count {
			match i % 10 {
				0 => {
					layout.get_bool(&row, i);
				}
				1 => {
					layout.get_i8(&row, i);
				}
				2 => {
					layout.get_i16(&row, i);
				}
				3 => {
					layout.get_i32(&row, i);
				}
				4 => {
					layout.get_i64(&row, i);
				}
				5 => {
					layout.get_f32(&row, i);
				}
				6 => {
					layout.get_f64(&row, i);
				}
				7 => {
					layout.get_date(&row, i);
				}
				8 => {
					layout.get_uuid4(&row, i);
				}
				_ => {
					layout.get_utf8(&row, i);
				}
			}
		}
	}
}

#[test]
fn test_dynamic_field_reallocation() {
	let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Blob, Type::Int]);

	let iterations = 1000;

	// Test performance of setting dynamic fields across many rows
	// (since dynamic fields can only be set once per encoded)
	let mut rows = Vec::with_capacity(iterations);

	for i in 0..iterations {
		let mut row = layout.allocate();
		let size = (i % 100) + 1;
		let string = "x".repeat(size);
		let bytes = vec![0u8; size];
		let int = Int::from(i as i64);

		layout.set_utf8(&mut row, 0, &string);
		layout.set_blob(&mut row, 1, &Blob::from(bytes));
		layout.set_int(&mut row, 2, &int);

		// Verify values
		assert_eq!(layout.get_utf8(&row, 0).len(), size);
		assert_eq!(layout.get_blob(&row, 1).len(), size);

		rows.push(row);
	}

	// Verify a sample of rows to ensure data integrity
	for (i, row) in rows.iter().enumerate().step_by(100) {
		let expected_size = (i % 100) + 1;
		assert_eq!(layout.get_utf8(row, 0).len(), expected_size);
		assert_eq!(layout.get_blob(row, 1).len(), expected_size);
		assert_eq!(layout.get_int(row, 2), Int::from(i as i64));
	}
}

#[test]
fn test_memory_efficiency() {
	// Test that memory usage is reasonable

	// Static types should have predictable size
	let layout = EncodedValuesLayout::new(&[
		Type::Boolean, // 1 bit validity + 1 byte
		Type::Int4,    // 1 bit validity + 4 bytes
		Type::Float8,  // 1 bit validity + 8 bytes
	]);
	let row = layout.allocate();

	// Expected: validity bits (rounded up) + data
	// 3 validity bits = 1 byte, data = 1 + 4 + 8 = 13 bytes
	// Plus any alignment padding
	assert!(row.len() < 32, "Static row too large: {} bytes", row.len());

	// Dynamic types should grow as needed - test with separate rows since
	// dynamic fields can only be set once
	let layout = EncodedValuesLayout::new(&[Type::Utf8]);

	let initial_size = layout.allocate().len();

	let mut row1 = layout.allocate();
	layout.set_utf8(&mut row1, 0, "short");
	let small_size = row1.len();

	let mut row2 = layout.allocate();
	layout.set_utf8(&mut row2, 0, &"x".repeat(1000));
	let large_size = row2.len();

	assert!(small_size > initial_size, "Dynamic field didn't grow");
	assert!(large_size > small_size, "Dynamic field didn't grow for larger data");
	assert!(large_size < 1200, "Dynamic field used too much memory");

	// Test that different sized dynamic fields use appropriate memory
	let sizes = [10, 100, 500, 1000];
	let mut row_sizes = Vec::new();

	for size in sizes {
		let mut row = layout.allocate();
		layout.set_utf8(&mut row, 0, &"x".repeat(size));
		row_sizes.push(row.len());
	}

	// Row sizes should generally increase with content size
	for i in 1..row_sizes.len() {
		assert!(row_sizes[i] >= row_sizes[i - 1], "Row size should increase with content size");
	}
}
