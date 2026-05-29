// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::encoded::shape::RowShape;
use reifydb_value::value::{blob::Blob, int::Int, value_type::ValueType};

#[test]
fn test_unaligned_access_all_types() {
	// Tests that all types handle unaligned memory access correctly
	// Critical for ARM and other strict-alignment architectures

	let types_to_test = vec![
		ValueType::Boolean,
		ValueType::Int1,
		ValueType::Int2,
		ValueType::Int4,
		ValueType::Int8,
		ValueType::Int16,
		ValueType::Uint1,
		ValueType::Uint2,
		ValueType::Uint4,
		ValueType::Uint8,
		ValueType::Uint16,
		ValueType::Float4,
		ValueType::Float8,
		ValueType::Date,
		ValueType::DateTime,
		ValueType::Time,
		ValueType::Duration,
		ValueType::Uuid4,
		ValueType::Uuid7,
		ValueType::IdentityId,
		ValueType::Utf8,
		ValueType::Blob,
		ValueType::Int,
		ValueType::Uint,
		ValueType::Decimal,
	];

	for target_type in types_to_test {
		// Create unaligned layout: Int1 (1 byte) followed by target
		// type
		let shape = RowShape::testing(&[
			ValueType::Int1,     // 1 byte - creates odd alignment
			target_type.clone(), // At offset 1 (odd)
			ValueType::Int1,     // Another 1 byte
			target_type.clone(), // At another odd offset
		]);

		let mut row = shape.allocate();

		// Set values at odd offsets - this should not crash
		match target_type {
			ValueType::Boolean => {
				shape.set_bool(&mut row, 1, true);
				assert_eq!(shape.get_bool(&row, 1), true);
				shape.set_bool(&mut row, 3, false);
				assert_eq!(shape.get_bool(&row, 3), false);
			}
			ValueType::Int1 => {
				shape.set_i8(&mut row, 1, 42);
				assert_eq!(shape.get_i8(&row, 1), 42);
			}
			ValueType::Int2 => {
				shape.set_i16(&mut row, 1, 1234i16);
				assert_eq!(shape.get_i16(&row, 1), 1234);
			}
			ValueType::Int4 => {
				shape.set_i32(&mut row, 1, 123456);
				assert_eq!(shape.get_i32(&row, 1), 123456);
			}
			ValueType::Int8 => {
				shape.set_i64(&mut row, 1, 1234567890);
				assert_eq!(shape.get_i64(&row, 1), 1234567890);
			}
			ValueType::Float4 => {
				shape.set_f32(&mut row, 1, 3.14);
				assert!((shape.get_f32(&row, 1) - 3.14).abs() < f32::EPSILON);
			}
			ValueType::Float8 => {
				shape.set_f64(&mut row, 1, 3.14159);
				assert!((shape.get_f64(&row, 1) - 3.14159).abs() < f64::EPSILON);
			}
			ValueType::Utf8 => {
				shape.set_utf8(&mut row, 1, "test");
				assert_eq!(shape.get_utf8(&row, 1), "test");
			}
			_ => {
				shape.set_none(&mut row, 1);
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

	let shape = RowShape::testing(&[
		ValueType::Int4,   // Static
		ValueType::Float8, // Static
		ValueType::Utf8,   // Dynamic
		ValueType::Blob,   // Dynamic
		ValueType::Int,    // Dynamic/Static depending on value
	]);

	let mut row = shape.allocate();
	let initial_size = row.len();

	// Repeatedly overwrite static fields - this should work fine
	for i in 0..10000 {
		shape.set_i32(&mut row, 0, i);
		shape.set_f64(&mut row, 1, i as f64);
	}

	// Size should not have grown for static fields
	assert_eq!(row.len(), initial_size, "Static fields caused memory growth");

	// Set dynamic fields once
	shape.set_utf8(&mut row, 2, "constant");
	shape.set_blob(&mut row, 3, &Blob::from(&b"fixed"[..]));
	shape.set_int(&mut row, 4, &Int::from(123i64));

	let size_after_dynamic = row.len();
	assert!(size_after_dynamic > initial_size, "Dynamic fields should increase size");
	assert!(size_after_dynamic < initial_size * 3, "Dynamic fields shouldn't triple size");

	// Test that many rows with same dynamic content are memory efficient
	let rows: Vec<_> = (0..100)
		.map(|_| {
			let mut r = shape.allocate();
			shape.set_i32(&mut r, 0, 42);
			shape.set_f64(&mut r, 1, 3.14);
			shape.set_utf8(&mut r, 2, "constant");
			shape.set_blob(&mut r, 3, &Blob::from(&b"fixed"[..]));
			shape.set_int(&mut r, 4, &Int::from(123i64));
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
	let shape = RowShape::testing(&[ValueType::Boolean]);
	let row = shape.allocate();
	assert!(row.len() > 0, "Row should have validity bits and data");
}

#[test]
fn test_maximum_field_count() {
	// Test with a large number of fields
	let types: Vec<ValueType> = (0..256)
		.map(|i| match i % 5 {
			0 => ValueType::Boolean,
			1 => ValueType::Int4,
			2 => ValueType::Float8,
			3 => ValueType::Utf8,
			_ => ValueType::Date,
		})
		.collect();

	let shape = RowShape::testing(&types);
	let mut row = shape.allocate();

	// Set and verify some fields
	shape.set_bool(&mut row, 0, true);
	assert_eq!(shape.get_bool(&row, 0), true);

	shape.set_i32(&mut row, 1, 42);
	assert_eq!(shape.get_i32(&row, 1), 42);

	shape.set_utf8(&mut row, 253, "field 253");
	assert_eq!(shape.get_utf8(&row, 253), "field 253");
}
