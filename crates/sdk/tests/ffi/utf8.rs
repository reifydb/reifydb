// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn utf8_empty_string() {
	let input = ColumnBuffer::utf8([""]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_empty_string", &input, &output);
}

#[test]
fn utf8_single_char() {
	let input = ColumnBuffer::utf8(["a"]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_single_char", &input, &output);
}

#[test]
fn utf8_eight_byte_payload_boundary() {
	// Exactly 8 bytes: aligns to a u64 boundary, an alignment-sensitive
	// var-len commit path may misbehave here.
	let input = ColumnBuffer::utf8(["12345678"]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_eight_byte_boundary", &input, &output);
}

#[test]
fn utf8_multibyte_unicode() {
	let input = ColumnBuffer::utf8(["cafe", "kafe", "test", "ascii"]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_multibyte", &input, &output);
}

#[test]
fn utf8_embedded_null() {
	// Embedded nulls must not truncate; valid UTF-8 includes any
	// byte < 0x80.
	let input = ColumnBuffer::utf8(["a\0b\0c"]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_embedded_null", &input, &output);
}

#[test]
fn utf8_many_short_strings() {
	let values: Vec<String> = (0..100).map(|i| format!("v{}", i)).collect();
	let input = ColumnBuffer::utf8(values);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_many_short", &input, &output);
}

#[test]
fn utf8_one_long_string() {
	let s: String = (0..4096).map(|i| ((i % 26) as u8 + b'a') as char).collect();
	let input = ColumnBuffer::utf8([s]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_long_4kib", &input, &output);
}

#[test]
fn utf8_mixed_lengths() {
	let input = ColumnBuffer::utf8(["", "a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg", "abcdefgh"]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_mixed_lengths", &input, &output);
}

#[test]
fn utf8_undefined_first_row() {
	// offsets[0] sentinel: must remain 0 even when row 0 is undefined.
	let input = ColumnBuffer::utf8_optional([None, Some("hello".to_string()), Some("world".to_string())]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_undefined_first", &input, &output);
}

#[test]
fn utf8_alternating_defined_undefined() {
	let input = ColumnBuffer::utf8_optional([
		Some("a".to_string()),
		None,
		Some("bb".to_string()),
		None,
		Some("ccc".to_string()),
		None,
		Some("dddd".to_string()),
	]);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_alternating", &input, &output);
}

#[test]
fn utf8_all_undefined() {
	let nones: Vec<Option<String>> = vec![None, None, None, None];
	let input = ColumnBuffer::utf8_optional(nones);
	let output = round_trip_column("s", input.clone());
	assert_column_eq("utf8_all_undefined", &input, &output);
}
