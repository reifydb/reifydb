// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::blob::Blob;

use super::common::{assert_column_eq, round_trip_column};

fn blob(bytes: &[u8]) -> Blob {
	Blob::new(bytes.to_vec())
}

#[test]
fn blob_empty() {
	let input = ColumnBuffer::blob([blob(&[])]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_empty", &input, &output);
}

#[test]
fn blob_single_byte() {
	let input = ColumnBuffer::blob([blob(&[0x42])]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_single_byte", &input, &output);
}

#[test]
fn blob_eight_byte_boundary() {
	let input = ColumnBuffer::blob([blob(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_eight_byte_boundary", &input, &output);
}

#[test]
fn blob_embedded_zeros() {
	let input = ColumnBuffer::blob([blob(&[0x00, 0xFF, 0x00, 0xAB, 0x00])]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_embedded_zeros", &input, &output);
}

#[test]
fn blob_full_byte_range() {
	let bytes: Vec<u8> = (0..=255u8).collect();
	let input = ColumnBuffer::blob([blob(&bytes)]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_full_range", &input, &output);
}

#[test]
fn blob_many_short() {
	let values: Vec<Blob> = (0..100u8).map(|i| blob(&[i, i.wrapping_add(1), i.wrapping_add(2)])).collect();
	let input = ColumnBuffer::blob(values);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_many_short", &input, &output);
}

#[test]
fn blob_one_long() {
	let bytes: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
	let input = ColumnBuffer::blob([blob(&bytes)]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_long_4kib", &input, &output);
}

#[test]
fn blob_mixed_lengths() {
	let values: Vec<Blob> = vec![blob(&[]), blob(&[0xAA]), blob(&[0xAA, 0xBB]), blob(&[0xAA, 0xBB, 0xCC])];
	let input = ColumnBuffer::blob(values);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_mixed_lengths", &input, &output);
}

#[test]
fn blob_undefined_first_row() {
	let input = ColumnBuffer::blob_optional([None, Some(blob(&[0x01, 0x02])), Some(blob(&[0x03]))]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_undefined_first", &input, &output);
}

#[test]
fn blob_alternating_defined_undefined() {
	let input = ColumnBuffer::blob_optional([Some(blob(&[0x01])), None, Some(blob(&[0x02, 0x03])), None]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("blob_alternating", &input, &output);
}
