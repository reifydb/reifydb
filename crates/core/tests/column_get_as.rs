// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::{
	Result,
	value::{constraint::precision::Precision, decimal::Decimal, int::Int, uint::Uint},
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
fn an_int_column_never_reads_as_a_float() {
	// A float read would silently round a 60 digit int, so only the exact type may read it.
	let value = Int::parse(&sixty_digits()).unwrap();
	let buffer = ColumnBuffer::int(Precision::new(76), [value.clone()]);
	assert_read_error(buffer.get_as::<f64>(0), "wrong type");
	assert_eq!(buffer.get_as::<Int>(0), Ok(Some(value)));
}

#[test]
fn a_uint_column_never_reads_as_a_float() {
	// Same as for int: a float would drop digits the column holds exactly.
	let value = Uint::parse(&sixty_digits()).unwrap();
	let buffer = ColumnBuffer::uint(Precision::new(76), [value.clone()]);
	assert_read_error(buffer.get_as::<f64>(0), "wrong type");
	assert_eq!(buffer.get_as::<Uint>(0), Ok(Some(value)));
}

#[test]
fn an_int_column_never_reads_as_a_native_integer() {
	// An int column may hold values past any native width, so no native read is lossless.
	let buffer = ColumnBuffer::int(Precision::new(39), [Int::from_u128(u128::MAX)]);
	assert_read_error(buffer.get_as::<u128>(0), "wrong type");
	assert_read_error(buffer.get_as::<i128>(0), "wrong type");
}

#[test]
fn a_float_no_decimal_holds_is_an_error_not_none() {
	// Otherwise a caller cannot tell NaN or an overflow from a none row.
	for value in [f64::NAN, f64::INFINITY, 1e300] {
		assert_read_error(ColumnBuffer::float8([value]).get_as::<Decimal>(0), "does not fit");
	}
	assert_eq!(ColumnBuffer::float8([1.5]).get_as::<Decimal>(0), Ok(Some(Decimal::parse("1.5").unwrap())));
}

#[test]
fn a_signed_column_never_reads_as_unsigned() {
	// A wrapping cast would turn -1 into u32::MAX, so signed to unsigned is refused outright.
	let buffer = ColumnBuffer::int4([-1]);
	assert_read_error(buffer.get_as::<u32>(0), "wrong type");
	assert_eq!(buffer.get_as::<i64>(0), Ok(Some(-1)));
}

#[test]
fn a_narrow_column_widens_but_a_wide_one_never_narrows() {
	// Widening is always lossless; narrowing could drop bits, so it must be a type error.
	let signed = ColumnBuffer::int1([i8::MIN]);
	assert_eq!(signed.get_as::<i8>(0), Ok(Some(i8::MIN)));
	assert_eq!(signed.get_as::<i16>(0), Ok(Some(i8::MIN as i16)));
	assert_eq!(signed.get_as::<i32>(0), Ok(Some(i8::MIN as i32)));
	assert_eq!(signed.get_as::<i64>(0), Ok(Some(i8::MIN as i64)));
	assert_eq!(signed.get_as::<i128>(0), Ok(Some(i8::MIN as i128)));
	let unsigned = ColumnBuffer::uint1([u8::MAX]);
	assert_eq!(unsigned.get_as::<u8>(0), Ok(Some(u8::MAX)));
	assert_eq!(unsigned.get_as::<u16>(0), Ok(Some(u8::MAX as u16)));
	assert_eq!(unsigned.get_as::<u32>(0), Ok(Some(u8::MAX as u32)));
	assert_eq!(unsigned.get_as::<u64>(0), Ok(Some(u8::MAX as u64)));
	assert_eq!(unsigned.get_as::<u128>(0), Ok(Some(u8::MAX as u128)));
	assert_eq!(ColumnBuffer::float4([1.5]).get_as::<f64>(0), Ok(Some(1.5)));
	assert_read_error(ColumnBuffer::int8([1]).get_as::<i32>(0), "wrong type");
	assert_read_error(ColumnBuffer::uint16([1]).get_as::<u64>(0), "wrong type");
	assert_read_error(ColumnBuffer::float8([1.5]).get_as::<f32>(0), "wrong type");
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
