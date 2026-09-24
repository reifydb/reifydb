// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::{
	Result,
	value::{constraint::precision::Precision, int::Int, uint::Uint},
};

fn sixty_digits() -> String {
	format!("1{}", "0".repeat(59))
}

fn assert_read_error<T: std::fmt::Debug>(read: Result<Option<T>>, reason: &str) {
	let err = read.unwrap_err();
	assert_eq!(err.code, "CONV_004");
	assert!(err.message.contains(reason), "expected '{reason}' in '{}'", err.message);
}

#[test]
fn an_int_beyond_i128_reads_as_a_float() {
	// Without this a 60 digit int reads as nothing at all, since it never fits i128 on the way to f64.
	let buffer = ColumnBuffer::int(Precision::new(76), [Int::parse(&sixty_digits()).unwrap()]);
	let read = buffer.get_as::<f64>(0).unwrap().unwrap();
	assert!((read - 1e59).abs() / 1e59 < 1e-15, "read {read}");
}

#[test]
fn a_uint_beyond_u128_reads_as_a_float() {
	// Must reach the float target through the full 256 bit value, never through u128.
	let buffer = ColumnBuffer::uint(Precision::new(76), [Uint::parse(&sixty_digits()).unwrap()]);
	let read = buffer.get_as::<f64>(0).unwrap().unwrap();
	assert!((read - 1e59).abs() / 1e59 < 1e-15, "read {read}");
}

#[test]
fn an_int_above_i128_max_reads_as_u128() {
	// An int between i128::MAX and u128::MAX must not be dropped just because i128 is tried first.
	let buffer = ColumnBuffer::int(Precision::new(39), [Int::from_u128(u128::MAX)]);
	assert_eq!(buffer.get_as::<u128>(0), Ok(Some(u128::MAX)));
	assert_read_error(buffer.get_as::<i128>(0), "does not fit");
}

#[test]
fn a_value_too_big_for_the_target_is_an_error_not_none() {
	// Otherwise a caller cannot tell an out of range value from a none row.
	assert_read_error(ColumnBuffer::int4([300]).get_as::<u8>(0), "does not fit");
	assert_read_error(ColumnBuffer::int(Precision::new(76), [Int::parse(&sixty_digits()).unwrap()]).get_as::<u128>(0), "does not fit");
	assert_read_error(ColumnBuffer::uint(Precision::new(76), [Uint::parse(&sixty_digits()).unwrap()]).get_as::<i64>(0), "does not fit");
}

#[test]
fn a_negative_int_never_wraps_into_an_unsigned_target() {
	// A wrapping cast would turn -1 into u128::MAX.
	let buffer = ColumnBuffer::int(Precision::new(39), [Int::from_i64(-1)]);
	assert_read_error(buffer.get_as::<u128>(0), "does not fit");
	assert_read_error(buffer.get_as::<u64>(0), "does not fit");
	assert_eq!(buffer.get_as::<i8>(0), Ok(Some(-1)));
}

#[test]
fn reading_the_wrong_column_type_is_an_error() {
	// A schema mistake must fail loudly instead of reading as none.
	assert_read_error(ColumnBuffer::utf8(["a"]).get_as::<i32>(0), "wrong type");
	assert_read_error(ColumnBuffer::int4([1]).get_as::<String>(0), "wrong type");
	assert_read_error(ColumnBuffer::int4([1]).get_as::<Int>(0), "wrong type");
	assert_read_error(ColumnBuffer::utf8(["a"]).get_as::<Vec<u8>>(0), "wrong type");
}

#[test]
fn the_error_names_the_column_type_and_the_target() {
	// Without both names the caller cannot find which read went wrong.
	let err = ColumnBuffer::int4([300]).get_as::<u8>(0).unwrap_err();
	assert!(err.message.contains("Int4") && err.message.contains("u8"), "message '{}'", err.message);
}

#[test]
fn a_none_row_and_a_row_past_the_end_read_as_none() {
	// Only a value that exists can fail to convert, a missing one stays none.
	let buffer = ColumnBuffer::int4_with_bitvec([7, 0], vec![true, false]);
	assert_eq!(buffer.get_as::<i32>(0), Ok(Some(7)));
	assert_eq!(buffer.get_as::<i32>(1), Ok(None));
	assert_eq!(ColumnBuffer::int4([7]).get_as::<i32>(5), Ok(None));
}
