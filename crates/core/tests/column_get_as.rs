// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory;
use reifydb_value::{
	Result,
	value::{column_view::ColumnView, decimal::Decimal},
};

fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
	ColumnView::try_from(column).unwrap()
}

fn assert_read_error<T: std::fmt::Debug>(read: Result<Option<T>>, reason: &str) {
	let err = read.unwrap_err();
	assert_eq!(err.code, "CONV_004");
	assert!(err.message.contains(reason), "expected '{reason}' in '{}'", err.message);
}

#[test]
fn a_float_no_decimal_holds_is_an_error_not_none() {
	// Otherwise a caller cannot tell NaN or an overflow from a none row.
	for value in [f64::NAN, f64::INFINITY, 1e300] {
		assert_read_error(view(&factory::float8("c", [value])).get_as::<Decimal>(0), "does not fit");
	}
	assert_eq!(view(&factory::float8("c", [1.5])).get_as::<Decimal>(0), Ok(Some(Decimal::parse("1.5").unwrap())));
}

#[test]
fn a_signed_column_never_reads_as_unsigned() {
	// A wrapping cast would turn -1 into u32::MAX, so signed to unsigned is refused outright.
	let buffer = factory::int4("c", [-1]);
	let buffer = view(&buffer);
	assert_read_error(buffer.get_as::<u32>(0), "wrong type");
	assert_eq!(buffer.get_as::<i64>(0), Ok(Some(-1)));
}

#[test]
fn a_narrow_column_widens_but_a_wide_one_never_narrows() {
	// Widening is always lossless; narrowing could drop bits, so it must be a type error.
	let signed = factory::int1("c", [i8::MIN]);
	let signed = view(&signed);
	assert_eq!(signed.get_as::<i8>(0), Ok(Some(i8::MIN)));
	assert_eq!(signed.get_as::<i16>(0), Ok(Some(i8::MIN as i16)));
	assert_eq!(signed.get_as::<i32>(0), Ok(Some(i8::MIN as i32)));
	assert_eq!(signed.get_as::<i64>(0), Ok(Some(i8::MIN as i64)));
	assert_eq!(signed.get_as::<i128>(0), Ok(Some(i8::MIN as i128)));
	let unsigned = factory::uint1("c", [u8::MAX]);
	let unsigned = view(&unsigned);
	assert_eq!(unsigned.get_as::<u8>(0), Ok(Some(u8::MAX)));
	assert_eq!(unsigned.get_as::<u16>(0), Ok(Some(u8::MAX as u16)));
	assert_eq!(unsigned.get_as::<u32>(0), Ok(Some(u8::MAX as u32)));
	assert_eq!(unsigned.get_as::<u64>(0), Ok(Some(u8::MAX as u64)));
	assert_eq!(unsigned.get_as::<u128>(0), Ok(Some(u8::MAX as u128)));
	assert_eq!(view(&factory::float4("c", [1.5])).get_as::<f64>(0), Ok(Some(1.5)));
	assert_read_error(view(&factory::int8("c", [1])).get_as::<i32>(0), "wrong type");
	assert_read_error(view(&factory::uint16("c", [1])).get_as::<u64>(0), "wrong type");
	assert_read_error(view(&factory::float8("c", [1.5])).get_as::<f32>(0), "wrong type");
}

#[test]
fn reading_the_wrong_column_type_is_an_error() {
	// A schema mistake must fail loudly instead of reading as none.
	assert_read_error(view(&factory::utf8("c", ["a"])).get_as::<i32>(0), "wrong type");
	assert_read_error(view(&factory::int4("c", [1])).get_as::<String>(0), "wrong type");
	assert_read_error(view(&factory::utf8("c", ["a"])).get_as::<Vec<u8>>(0), "wrong type");
}

#[test]
fn the_error_names_the_column_type_and_the_target() {
	// Without both names the caller cannot find which read went wrong.
	let err = view(&factory::int4("c", [300])).get_as::<u8>(0).unwrap_err();
	assert!(err.message.contains("Int4") && err.message.contains("u8"), "message '{}'", err.message);
}

#[test]
fn a_none_row_and_a_row_past_the_end_read_as_none() {
	// Only a value that exists can fail to convert, a missing one stays none.
	let buffer = factory::int4_with_bitvec("c", [7, 0], vec![true, false]);
	let buffer = view(&buffer);
	assert_eq!(buffer.get_as::<i32>(0), Ok(Some(7)));
	assert_eq!(buffer.get_as::<i32>(1), Ok(None));
	assert_eq!(view(&factory::int4("c", [7])).get_as::<i32>(5), Ok(None));
}
