// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{fmt::Display, sync::Arc};

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error::TypeError,
	fragment::{Fragment, LazyFragment},
	value::{
		boolean::parse::parse_bool,
		container::{number::NumberContainer, utf8::Utf8Container},
		is::IsNumber,
		r#type::Type,
	},
};

pub fn to_boolean(data: &ColumnData, lazy_fragment: impl LazyFragment) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Int1(container) => from_int1(container, lazy_fragment),
		ColumnData::Int2(container) => from_int2(container, lazy_fragment),
		ColumnData::Int4(container) => from_int4(container, lazy_fragment),
		ColumnData::Int8(container) => from_int8(container, lazy_fragment),
		ColumnData::Int16(container) => from_int16(container, lazy_fragment),
		ColumnData::Uint1(container) => from_uint1(container, lazy_fragment),
		ColumnData::Uint2(container) => from_uint2(container, lazy_fragment),
		ColumnData::Uint4(container) => from_uint4(container, lazy_fragment),
		ColumnData::Uint8(container) => from_uint8(container, lazy_fragment),
		ColumnData::Uint16(container) => from_uint16(container, lazy_fragment),
		ColumnData::Float4(container) => from_float4(container, lazy_fragment),
		ColumnData::Float8(container) => from_float8(container, lazy_fragment),
		ColumnData::Utf8 {
			container,
			..
		} => from_utf8(container, lazy_fragment),
		_ => {
			let from = data.get_type();
			Err(TypeError::UnsupportedCast {
				from,
				to: Type::Boolean,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

fn to_bool<T>(
	container: &NumberContainer<T>,
	lazy_fragment: impl LazyFragment,
	validate: impl Fn(T) -> Option<bool>,
) -> crate::Result<ColumnData>
where
	T: Copy + Display + IsNumber + Default,
{
	let mut out = ColumnData::with_capacity(Type::Boolean, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			match validate(container[idx]) {
				Some(b) => out.push::<bool>(b),
				None => {
					let base_fragment = lazy_fragment.fragment();
					let error_fragment = Fragment::Statement {
						text: Arc::from(container[idx].to_string()),
						line: base_fragment.line(),
						column: base_fragment.column(),
					};
					return Err(TypeError::InvalidNumberBoolean {
						fragment: error_fragment,
					}
					.into());
				}
			}
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

macro_rules! impl_integer_to_bool {
	($fn_name:ident, $type:ty) => {
		#[inline]
		fn $fn_name(
			container: &NumberContainer<$type>,
			lazy_fragment: impl LazyFragment,
		) -> crate::Result<ColumnData> {
			to_bool(container, lazy_fragment, |val| match val {
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
			lazy_fragment: impl LazyFragment,
		) -> crate::Result<ColumnData> {
			to_bool(container, lazy_fragment, |val| {
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

fn from_utf8(container: &Utf8Container, lazy_fragment: impl LazyFragment) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Boolean, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			// Parse with internal fragment, then replace with
			// proper source fragment if error
			let temp_fragment = Fragment::internal(&container[idx]);
			match parse_bool(temp_fragment) {
				Ok(b) => out.push(b),
				Err(mut e) => {
					// Replace the error's fragment with the
					// proper source fragment
					e.0.with_fragment(lazy_fragment.fragment());
					return Err(e);
				}
			}
		} else {
			out.push_none();
		}
	}
	Ok(out)
}
