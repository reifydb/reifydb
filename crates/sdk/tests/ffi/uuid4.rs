// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::uuid::Uuid4;
use uuid::Uuid;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uuid4_nil() {
	let input = ColumnBuffer::uuid4([Uuid4(Uuid::nil())]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid4_nil", &input, &output);
}

#[test]
fn uuid4_specific_known_bytes() {
	// Distinct bytes in every position to catch any 16-byte stride defect.
	let bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x47, 0x08, 0x89, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10];
	let input = ColumnBuffer::uuid4([Uuid4(Uuid::from_bytes(bytes))]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid4_specific", &input, &output);
}

#[test]
fn uuid4_thirty_two_rows_distinct() {
	let values: Vec<Uuid4> = (0..32u8)
		.map(|i| {
			let mut bytes = [0u8; 16];
			for (j, b) in bytes.iter_mut().enumerate() {
				*b = i.wrapping_mul(j as u8 + 1);
			}
			Uuid4(Uuid::from_bytes(bytes))
		})
		.collect();
	let input = ColumnBuffer::uuid4(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid4_thirty_two", &input, &output);
}

#[test]
fn uuid4_with_undefined() {
	let bytes = [0xAA; 16];
	let input = ColumnBuffer::uuid4_optional([
		Some(Uuid4(Uuid::from_bytes(bytes))),
		None,
		Some(Uuid4(Uuid::nil())),
		None,
	]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid4_with_undefined", &input, &output);
}
