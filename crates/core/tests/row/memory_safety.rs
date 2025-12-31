// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Memory safety edge case tests for the encoded encoding system

use reifydb_core::value::encoded::EncodedValuesLayout;
use reifydb_type::*;

#[test]
fn test_unaligned_access_all_types() {
	// Tests that all types handle unaligned memory access correctly
	// Critical for ARM and other strict-alignment architectures

	let types_to_test = vec![
		Type::Boolean,
		Type::Int1,
		Type::Int2,
		Type::Int4,
		Type::Int8,
		Type::Int16,
		Type::Uint1,
		Type::Uint2,
		Type::Uint4,
		Type::Uint8,
		Type::Uint16,
		Type::Float4,
		Type::Float8,
		Type::Date,
		Type::DateTime,
		Type::Time,
		Type::Duration,
		Type::Uuid4,
		Type::Uuid7,
		Type::IdentityId,
		Type::Utf8,
		Type::Blob,
		Type::Int,
		Type::Uint,
		Type::Decimal,
	];

	for target_type in types_to_test {
		// Create unaligned layout: Int1 (1 byte) followed by target
		// type
		let layout = EncodedValuesLayout::new(&[
			Type::Int1,  // 1 byte - creates odd alignment
			target_type, // At offset 1 (odd)
			Type::Int1,  // Another 1 byte
			target_type, // At another odd offset
		]);

		let mut row = layout.allocate();

		// Set values at odd offsets - this should not crash
		match target_type {
			Type::Boolean => {
				layout.set_bool(&mut row, 1, true);
				assert_eq!(layout.get_bool(&row, 1), true);
				layout.set_bool(&mut row, 3, false);
				assert_eq!(layout.get_bool(&row, 3), false);
			}
			Type::Int1 => {
				layout.set_i8(&mut row, 1, 42);
				assert_eq!(layout.get_i8(&row, 1), 42);
			}
			Type::Int2 => {
				layout.set_i16(&mut row, 1, 1234i16);
				assert_eq!(layout.get_i16(&row, 1), 1234);
			}
			Type::Int4 => {
				layout.set_i32(&mut row, 1, 123456);
				assert_eq!(layout.get_i32(&row, 1), 123456);
			}
			Type::Int8 => {
				layout.set_i64(&mut row, 1, 1234567890);
				assert_eq!(layout.get_i64(&row, 1), 1234567890);
			}
			Type::Float4 => {
				layout.set_f32(&mut row, 1, 3.14);
				assert!((layout.get_f32(&row, 1) - 3.14).abs() < f32::EPSILON);
			}
			Type::Float8 => {
				layout.set_f64(&mut row, 1, 3.14159);
				assert!((layout.get_f64(&row, 1) - 3.14159).abs() < f64::EPSILON);
			}
			Type::Utf8 => {
				layout.set_utf8(&mut row, 1, "test");
				assert_eq!(layout.get_utf8(&row, 1), "test");
			}
			_ => {
				layout.set_undefined(&mut row, 1);
				assert!(!row.is_defined(1));
			}
		}
	}
}

#[test]
fn test_repeated_overwrites_no_memory_leak() {
	// Verifies that repeated sets don't cause memory growth for static
	// types For dynamic types, test that memory usage is reasonable across
	// multiple rows

	let layout = EncodedValuesLayout::new(&[
		Type::Int4,   // Static
		Type::Float8, // Static
		Type::Utf8,   // Dynamic
		Type::Blob,   // Dynamic
		Type::Int,    // Dynamic/Static depending on value
	]);

	let mut row = layout.allocate();
	let initial_size = row.len();

	// Repeatedly overwrite static fields - this should work fine
	for i in 0..10000 {
		layout.set_i32(&mut row, 0, i);
		layout.set_f64(&mut row, 1, i as f64);
	}

	// Size should not have grown for static fields
	assert_eq!(row.len(), initial_size, "Static fields caused memory growth");

	// Set dynamic fields once
	layout.set_utf8(&mut row, 2, "constant");
	layout.set_blob(&mut row, 3, &Blob::from(&b"fixed"[..]));
	layout.set_int(&mut row, 4, &Int::from(123i64));

	let size_after_dynamic = row.len();
	assert!(size_after_dynamic > initial_size, "Dynamic fields should increase size");
	assert!(size_after_dynamic < initial_size * 3, "Dynamic fields shouldn't triple size");

	// Test that many rows with same dynamic content are memory efficient
	let rows: Vec<_> = (0..100)
		.map(|_| {
			let mut r = layout.allocate();
			layout.set_i32(&mut r, 0, 42);
			layout.set_f64(&mut r, 1, 3.14);
			layout.set_utf8(&mut r, 2, "constant");
			layout.set_blob(&mut r, 3, &Blob::from(&b"fixed"[..]));
			layout.set_int(&mut r, 4, &Int::from(123i64));
			r
		})
		.collect();

	// All rows should have similar size
	for r in &rows {
		assert_eq!(r.len(), size_after_dynamic, "Row sizes should be consistent");
	}
}

#[test]
fn test_minimal_row_handling() {
	// Test edge case of encoded with minimal fields
	let layout = EncodedValuesLayout::new(&[Type::Boolean]);
	let row = layout.allocate();
	assert!(row.len() > 0, "Row should have validity bits and data");
}

#[test]
fn test_maximum_field_count() {
	// Test with a large number of fields
	let types: Vec<Type> = (0..256)
		.map(|i| match i % 5 {
			0 => Type::Boolean,
			1 => Type::Int4,
			2 => Type::Float8,
			3 => Type::Utf8,
			_ => Type::Date,
		})
		.collect();

	let layout = EncodedValuesLayout::new(&types);
	let mut row = layout.allocate();

	// Set and verify some fields
	layout.set_bool(&mut row, 0, true);
	assert_eq!(layout.get_bool(&row, 0), true);

	layout.set_i32(&mut row, 1, 42);
	assert_eq!(layout.get_i32(&row, 1), 42);

	layout.set_utf8(&mut row, 253, "field 253");
	assert_eq!(layout.get_utf8(&row, 253), "field 253");
}
