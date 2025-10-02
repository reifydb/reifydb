// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Robustness tests for the encoded encoding system
//! Tests error handling, recovery, and stability under stress

use std::str::FromStr;

use reifydb_core::value::encoded::EncodedValuesLayout;
use reifydb_type::*;

#[test]
fn test_massive_field_count() {
	// Test with an extreme number of fields
	let field_count = 10000;
	let types: Vec<Type> = vec![Type::Int4; field_count];
	let layout = EncodedValuesLayout::new(&types);
	let mut row = layout.allocate();

	// Set and verify a sampling of fields
	for i in (0..field_count).step_by(100) {
		layout.set_i32(&mut row, i, i as i32);
	}

	for i in (0..field_count).step_by(100) {
		assert_eq!(layout.get_i32(&row, i), i as i32);
	}
}

#[test]
fn test_mixed_static_dynamic_stress() {
	// Stress test with alternating static and dynamic fields
	let types: Vec<Type> = (0..100)
		.map(|i| {
			if i % 2 == 0 {
				Type::Int8
			} else {
				Type::Utf8
			}
		})
		.collect();

	let layout = EncodedValuesLayout::new(&types);

	// Create a encoded and set all dynamic fields once, then repeatedly update
	// static fields
	let mut row = layout.allocate();

	// First, set all dynamic fields once
	for i in (1..100).step_by(2) {
		// odd indices are Utf8 (dynamic)
		let text = format!("field_{}", i);
		layout.set_utf8(&mut row, i, &text);
	}

	// Now repeatedly update static fields (even indices are Int8)
	for iteration in 0..100 {
		for i in (0..100).step_by(2) {
			// even indices are Int8 (static)
			layout.set_i64(&mut row, i, iteration as i64 * 100 + i as i64);
		}

		// Verify static field updates and dynamic field persistence
		if iteration % 10 == 0 {
			for i in (0..100).step_by(7) {
				if i % 2 == 0 {
					assert_eq!(layout.get_i64(&row, i), iteration as i64 * 100 + i as i64);
				} else {
					let expected = format!("field_{}", i);
					assert_eq!(layout.get_utf8(&row, i), expected);
				}
			}
		}
	}

	// Test creating multiple rows with different dynamic content
	let mut test_rows = Vec::new();
	for row_idx in 0..10 {
		let mut test_row = layout.allocate();

		// Set static fields
		for i in (0..100).step_by(2) {
			layout.set_i64(&mut test_row, i, row_idx as i64);
		}

		// Set dynamic fields with encoded-specific content
		for i in (1..100).step_by(2) {
			let text = format!("row_{}_field_{}", row_idx, i);
			layout.set_utf8(&mut test_row, i, &text);
		}

		test_rows.push(test_row);
	}

	// Verify all test rows have correct content
	for (row_idx, test_row) in test_rows.iter().enumerate() {
		for i in (0..100).step_by(10) {
			// Sample every 10th field
			if i % 2 == 0 {
				assert_eq!(layout.get_i64(test_row, i), row_idx as i64);
			} else {
				let expected = format!("row_{}_field_{}", row_idx, i);
				assert_eq!(layout.get_utf8(test_row, i), expected);
			}
		}
	}
}

#[test]
fn test_repeated_clone_stability() {
	// Test that cloning doesn't degrade or corrupt data
	let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Blob, Type::Int, Type::Decimal]);

	let mut original = layout.allocate();
	layout.set_utf8(&mut original, 0, &"x".repeat(1000));
	layout.set_blob(&mut original, 1, &Blob::from(vec![42u8; 1000]));
	layout.set_int(&mut original, 2, &Int::from(i128::MAX));
	layout.set_decimal(&mut original, 3, &Decimal::from_str("99999.99999").unwrap());

	let mut current = original.clone();

	// Clone many times and verify data integrity
	for _ in 0..1000 {
		let next = current.clone();

		// Verify data is still intact
		assert_eq!(layout.get_utf8(&next, 0), "x".repeat(1000));
		assert_eq!(layout.get_blob(&next, 1), Blob::from(vec![42u8; 1000]));
		assert_eq!(layout.get_int(&next, 2), Int::from(i128::MAX));

		current = next;
	}
}

#[test]
fn test_validity_bit_stress() {
	// Test validity bit handling under stress
	let field_count = 1000;
	let types: Vec<Type> = vec![Type::Int4; field_count];
	let layout = EncodedValuesLayout::new(&types);
	let mut row = layout.allocate();

	// Set every other field as undefined
	for i in 0..field_count {
		if i % 2 == 0 {
			layout.set_i32(&mut row, i, i as i32);
		} else {
			layout.set_undefined(&mut row, i);
		}
	}

	// Verify validity bits
	for i in 0..field_count {
		if i % 2 == 0 {
			assert!(row.is_defined(i));
			assert_eq!(layout.try_get_i32(&row, i), Some(i as i32));
		} else {
			assert!(!row.is_defined(i));
			assert_eq!(layout.try_get_i32(&row, i), None);
		}
	}

	// Flip all validity bits
	for i in 0..field_count {
		if i % 2 == 0 {
			layout.set_undefined(&mut row, i);
		} else {
			layout.set_i32(&mut row, i, -(i as i32));
		}
	}

	// Verify flipped validity
	for i in 0..field_count {
		if i % 2 == 0 {
			assert!(!row.is_defined(i));
			assert_eq!(layout.try_get_i32(&row, i), None);
		} else {
			assert!(row.is_defined(i));
			assert_eq!(layout.try_get_i32(&row, i), Some(-(i as i32)));
		}
	}
}

#[test]
fn test_extreme_string_sizes() {
	// Test handling of very large strings
	let layout = EncodedValuesLayout::new(&[Type::Utf8]);

	// Test various string sizes - use separate rows since dynamic fields
	// can only be set once
	let sizes = [0, 1, 100, 1000, 10000, 100000, 1000000];

	for size in sizes {
		let mut row = layout.allocate();
		let large_string = "a".repeat(size);
		layout.set_utf8(&mut row, 0, &large_string);
		let retrieved = layout.get_utf8(&row, 0);
		assert_eq!(retrieved.len(), size);

		// Verify content for smaller strings
		if size <= 1000 {
			assert_eq!(retrieved, large_string);
		} else {
			// For very large strings, just verify they're all the
			// same character
			assert!(retrieved.chars().all(|c| c == 'a'), "Large string content verification failed");
		}
	}
}

#[test]
fn test_concurrent_field_updates() {
	// Simulate concurrent-like updates - test rapid field setting across
	// different rows since dynamic fields can only be set once per encoded
	let layout = EncodedValuesLayout::new(&[Type::Int8, Type::Utf8, Type::Int8, Type::Utf8]);

	let iterations = 1000;
	let mut rows = Vec::with_capacity(iterations);

	// Create many rows with rapid field setting
	for i in 0..iterations {
		let mut row = layout.allocate();

		// Set all fields for this encoded
		layout.set_i64(&mut row, 0, (i * 4) as i64);
		layout.set_utf8(&mut row, 1, &(i * 4 + 1).to_string());
		layout.set_i64(&mut row, 2, (i * 4 + 2) as i64);
		layout.set_utf8(&mut row, 3, &(i * 4 + 3).to_string());

		rows.push(row);
	}

	// Verify all rows maintain their correct data
	for (i, row) in rows.iter().enumerate() {
		assert_eq!(layout.get_i64(row, 0), (i * 4) as i64);
		assert_eq!(layout.get_utf8(row, 1), (i * 4 + 1).to_string());
		assert_eq!(layout.get_i64(row, 2), (i * 4 + 2) as i64);
		assert_eq!(layout.get_utf8(row, 3), (i * 4 + 3).to_string());
	}

	// Test that static fields can still be updated repeatedly on a single
	// encoded
	let mut static_test_row = layout.allocate();

	for i in 0..1000 {
		layout.set_i64(&mut static_test_row, 0, i as i64);
		layout.set_i64(&mut static_test_row, 2, (i * 2) as i64);

		// Verify static fields can be read back correctly
		assert_eq!(layout.get_i64(&static_test_row, 0), i as i64);
		assert_eq!(layout.get_i64(&static_test_row, 2), (i * 2) as i64);
	}

	// Set dynamic fields once on the static test encoded
	layout.set_utf8(&mut static_test_row, 1, "dynamic1");
	layout.set_utf8(&mut static_test_row, 3, "dynamic2");

	// Verify final state
	assert_eq!(layout.get_i64(&static_test_row, 0), 999);
	assert_eq!(layout.get_utf8(&static_test_row, 1), "dynamic1");
	assert_eq!(layout.get_i64(&static_test_row, 2), 1998);
	assert_eq!(layout.get_utf8(&static_test_row, 3), "dynamic2");
}

#[test]
fn test_row_size_stability() {
	// Ensure encoded sizes are stable and predictable for dynamic fields
	let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Blob]);

	// Test that rows with similar sized content have similar sizes
	let sizes = [10, 100, 1000];
	let mut row_sizes = Vec::new();

	for size in sizes {
		let mut row = layout.allocate();
		layout.set_utf8(&mut row, 0, &"x".repeat(size));
		layout.set_blob(&mut row, 1, &Blob::from(vec![0u8; size]));

		row_sizes.push(row.len());
	}

	// Verify encoded sizes increase reasonably with content size
	for i in 1..row_sizes.len() {
		assert!(
			row_sizes[i] > row_sizes[i - 1],
			"Row size should increase with content size: {} vs {}",
			row_sizes[i - 1],
			row_sizes[i]
		);
	}

	// Test size consistency - rows with same content should have same size
	let mut same_size_rows = Vec::new();
	for _ in 0..10 {
		let mut row = layout.allocate();
		layout.set_utf8(&mut row, 0, &"x".repeat(50));
		layout.set_blob(&mut row, 1, &Blob::from(vec![0u8; 50]));
		same_size_rows.push(row.len());
	}

	// All rows with same content should have identical size
	let first_size = same_size_rows[0];
	for (i, &size) in same_size_rows.iter().enumerate() {
		assert_eq!(size, first_size, "Row {} has different size {} vs expected {}", i, size, first_size);
	}
}
