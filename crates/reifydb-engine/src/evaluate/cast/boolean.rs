// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::fmt::{Debug, Display};

use reifydb_core::{
	OwnedFragment, Type,
	result::error::diagnostic::{boolean::invalid_number_boolean, cast},
	return_error,
	value::{
		IsNumber,
		boolean::parse_bool,
		container::{NumberContainer, StringContainer},
	},
};

use crate::columnar::ColumnData;

pub fn to_boolean(
	data: &ColumnData,
	fragment: impl Fn() -> OwnedFragment,
) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Int1(container) => from_int1(container, &fragment),
		ColumnData::Int2(container) => from_int2(container, &fragment),
		ColumnData::Int4(container) => from_int4(container, &fragment),
		ColumnData::Int8(container) => from_int8(container, &fragment),
		ColumnData::Int16(container) => from_int16(container, &fragment),
		ColumnData::Uint1(container) => from_uint1(container, &fragment),
		ColumnData::Uint2(container) => from_uint2(container, &fragment),
		ColumnData::Uint4(container) => from_uint4(container, &fragment),
		ColumnData::Uint8(container) => from_uint8(container, &fragment),
		ColumnData::Uint16(container) => from_uint16(container, &fragment),
		ColumnData::Float4(container) => from_float4(container, &fragment),
		ColumnData::Float8(container) => from_float8(container, &fragment),
		ColumnData::Utf8(container) => from_utf8(container, fragment),
		_ => {
			let source_type = data.get_type();
			return_error!(cast::unsupported_cast(
				fragment(),
				source_type,
				Type::Bool
			))
		}
	}
}

fn to_bool<T>(
	container: &NumberContainer<T>,
	fragment: &impl Fn() -> OwnedFragment,
	validate: impl Fn(T) -> Option<bool>,
) -> crate::Result<ColumnData>
where
	T: Copy + Display + IsNumber + Clone + Debug + Default,
{
	let mut out = ColumnData::with_capacity(Type::Bool, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			match validate(container[idx]) {
				Some(b) => out.push::<bool>(b),
				None => {
					use reifydb_core::Fragment;
					let base_fragment = fragment();
					let error_fragment = OwnedFragment::Statement {
						text: container[idx].to_string(),
						line: base_fragment.line(),
						column: base_fragment.column(),
					};
					return_error!(invalid_number_boolean(
						error_fragment
					));
				}
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

macro_rules! impl_integer_to_bool {
	($fn_name:ident, $type:ty) => {
		#[inline]
		fn $fn_name(
			container: &NumberContainer<$type>,
			fragment: &impl Fn() -> OwnedFragment,
		) -> crate::Result<ColumnData> {
			to_bool(container, fragment, |val| match val {
				0 => Some(false),
				1 => Some(true),
				_ => None,
			})
		}
	};
}

macro_rules! impl_float_to_bool {
	($fn_name:ident, $type:ty) => {
		#[inline]
		fn $fn_name(
			container: &NumberContainer<$type>,
			fragment: &impl Fn() -> OwnedFragment,
		) -> crate::Result<ColumnData> {
			to_bool(container, fragment, |val| {
				if val == 0.0 {
					Some(false)
				} else if val == 1.0 {
					Some(true)
				} else {
					None
				}
			})
		}
	};
}

impl_integer_to_bool!(from_int1, i8);
impl_integer_to_bool!(from_int2, i16);
impl_integer_to_bool!(from_int4, i32);
impl_integer_to_bool!(from_int8, i64);
impl_integer_to_bool!(from_int16, i128);
impl_integer_to_bool!(from_uint1, u8);
impl_integer_to_bool!(from_uint2, u16);
impl_integer_to_bool!(from_uint4, u32);
impl_integer_to_bool!(from_uint8, u64);
impl_integer_to_bool!(from_uint16, u128);
impl_float_to_bool!(from_float4, f32);
impl_float_to_bool!(from_float8, f64);

fn from_utf8(
	container: &StringContainer,
	fragment: impl Fn() -> OwnedFragment,
) -> crate::Result<ColumnData> {
	use reifydb_core::interface::fragment::BorrowedFragment;
	let mut out = ColumnData::with_capacity(Type::Bool, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			// Parse with internal fragment, then replace with proper source fragment if error
			let temp_fragment = BorrowedFragment::new_internal(&container[idx]);
			match parse_bool(temp_fragment) {
				Ok(b) => out.push(b),
				Err(mut e) => {
					// Replace the error's fragment with the proper source fragment
					e.0.with_fragment(fragment());
					return Err(e);
				}
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}
