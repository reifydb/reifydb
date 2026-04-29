// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::uuid::Uuid7;
use uuid::Uuid;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uuid7_nil() {
	let input = ColumnBuffer::uuid7([Uuid7(Uuid::nil())]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid7_nil", &input, &output);
}

#[test]
fn uuid7_specific_known_bytes() {
	let bytes = [0x01, 0x8D, 0x5E, 0x30, 0x4B, 0x78, 0x7A, 0xBC, 0x91, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
	let input = ColumnBuffer::uuid7([Uuid7(Uuid::from_bytes(bytes))]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid7_specific", &input, &output);
}

#[test]
fn uuid7_thirty_two_rows_distinct() {
	let values: Vec<Uuid7> = (0..32u8)
		.map(|i| {
			let mut bytes = [0u8; 16];
			for (j, b) in bytes.iter_mut().enumerate() {
				*b = i.wrapping_mul(j as u8 + 1).wrapping_add(j as u8);
			}
			Uuid7(Uuid::from_bytes(bytes))
		})
		.collect();
	let input = ColumnBuffer::uuid7(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid7_thirty_two", &input, &output);
}

#[test]
fn uuid7_with_undefined() {
	let bytes = [0x55; 16];
	let input = ColumnBuffer::uuid7_optional([
		Some(Uuid7(Uuid::from_bytes(bytes))),
		None,
		Some(Uuid7(Uuid::nil())),
		None,
	]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uuid7_with_undefined", &input, &output);
}
