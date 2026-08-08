// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShape;
use reifydb_value::value::{blob::Blob, date::Date, int::Int, uuid::Uuid4, value_type::ValueType};

#[test]
fn test_large_row() {
	let field_counts = [10, 50, 100, 200, 500];

	for count in field_counts {
		let types: Vec<ValueType> = (0..count)
			.map(|i| match i % 10 {
				0 => ValueType::Boolean,
				1 => ValueType::Int1,
				2 => ValueType::Int2,
				3 => ValueType::Int4,
				4 => ValueType::Int8,
				5 => ValueType::Float4,
				6 => ValueType::Float8,
				7 => ValueType::Date,
				8 => ValueType::Uuid4,
				_ => ValueType::Utf8,
			})
			.collect();

		let shape = RowShape::testing(&types);
		let mut row = shape.allocate();

		for i in 0..count {
			match i % 10 {
				0 => shape.set::<bool>(&mut row, i, true),
				1 => shape.set::<i8>(&mut row, i, 42i8),
				2 => shape.set::<i16>(&mut row, i, 1234i16),
				3 => shape.set::<i32>(&mut row, i, 123456i32),
				4 => shape.set::<i64>(&mut row, i, 1234567890i64),
				5 => shape.set::<f32>(&mut row, i, 3.14f32),
				6 => shape.set::<f64>(&mut row, i, 3.14159f64),
				7 => shape.set::<Date>(&mut row, i, Date::from_ymd(2024, 12, 25).unwrap()),
				8 => shape.set::<Uuid4>(&mut row, i, Uuid4::generate()),
				_ => shape.set_utf8(&mut row, i, "test"),
			}
		}

		for i in 0..count {
			match i % 10 {
				0 => {
					shape.get::<bool>(&row, i);
				}
				1 => {
					shape.get::<i8>(&row, i);
				}
				2 => {
					shape.get::<i16>(&row, i);
				}
				3 => {
					shape.get::<i32>(&row, i);
				}
				4 => {
					shape.get::<i64>(&row, i);
				}
				5 => {
					shape.get::<f32>(&row, i);
				}
				6 => {
					shape.get::<f64>(&row, i);
				}
				7 => {
					shape.get::<Date>(&row, i);
				}
				8 => {
					shape.get::<Uuid4>(&row, i);
				}
				_ => {
					shape.get_utf8(&row, i);
				}
			}
		}
	}
}

#[test]
fn test_dynamic_field_reallocation() {
	let shape = RowShape::testing(&[ValueType::Utf8, ValueType::Blob, ValueType::Int]);

	let iterations = 1000;

	// A dynamic field can only be set once per row, so the churn is spread across many rows.
	let mut rows = Vec::with_capacity(iterations);

	for i in 0..iterations {
		let mut row = shape.allocate();
		let size = (i % 100) + 1;
		let string = "x".repeat(size);
		let bytes = vec![0u8; size];
		let int = Int::from(i as i64);

		shape.set_utf8(&mut row, 0, &string);
		shape.set_blob(&mut row, 1, &Blob::from(bytes));
		shape.set_int(&mut row, 2, &int);

		assert_eq!(shape.get_utf8(&row, 0).len(), size);
		assert_eq!(shape.get_blob(&row, 1).len(), size);

		rows.push(row);
	}

	for (i, row) in rows.iter().enumerate().step_by(100) {
		let expected_size = (i % 100) + 1;
		assert_eq!(shape.get_utf8(row, 0).len(), expected_size);
		assert_eq!(shape.get_blob(row, 1).len(), expected_size);
		assert_eq!(shape.get_int(row, 2), Int::from(i as i64));
	}
}

#[test]
fn test_memory_efficiency() {
	let shape = RowShape::testing(&[
		ValueType::Boolean, // 1 bit validity + 1 byte
		ValueType::Int4,    // 1 bit validity + 4 bytes
		ValueType::Float8,  // 1 bit validity + 8 bytes
	]);
	let row = shape.allocate();

	// 32-byte header + 3 validity bits rounded to 1 byte + 13 bytes of data, plus alignment padding.
	assert!(row.len() < 56, "Static row too large: {} bytes", row.len());

	// A dynamic field can only be set once, so each size gets its own row.
	let shape = RowShape::testing(&[ValueType::Utf8]);

	let initial_size = shape.allocate().len();

	let mut row1 = shape.allocate();
	shape.set_utf8(&mut row1, 0, "short");
	let small_size = row1.len();

	let mut row2 = shape.allocate();
	shape.set_utf8(&mut row2, 0, &"x".repeat(1000));
	let large_size = row2.len();

	assert!(small_size > initial_size, "Dynamic field didn't grow");
	assert!(large_size > small_size, "Dynamic field didn't grow for larger data");
	assert!(large_size < 1200, "Dynamic field used too much memory");

	let sizes = [10, 100, 500, 1000];
	let mut row_sizes = Vec::new();

	for size in sizes {
		let mut row = shape.allocate();
		shape.set_utf8(&mut row, 0, &"x".repeat(size));
		row_sizes.push(row.len());
	}

	for i in 1..row_sizes.len() {
		assert!(row_sizes[i] >= row_sizes[i - 1], "Row size should increase with content size");
	}
}
