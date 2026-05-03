// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fmt::Display, sync::Arc};

use reifydb_core::value::column::buffer::ColumnBuffer;
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

use crate::Result;

pub fn to_boolean(data: &ColumnBuffer, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	match data {
		ColumnBuffer::Int1(container) => from_int1(container, lazy_fragment),
		ColumnBuffer::Int2(container) => from_int2(container, lazy_fragment),
		ColumnBuffer::Int4(container) => from_int4(container, lazy_fragment),
		ColumnBuffer::Int8(container) => from_int8(container, lazy_fragment),
		ColumnBuffer::Int16(container) => from_int16(container, lazy_fragment),
		ColumnBuffer::Uint1(container) => from_uint1(container, lazy_fragment),
		ColumnBuffer::Uint2(container) => from_uint2(container, lazy_fragment),
		ColumnBuffer::Uint4(container) => from_uint4(container, lazy_fragment),
		ColumnBuffer::Uint8(container) => from_uint8(container, lazy_fragment),
		ColumnBuffer::Uint16(container) => from_uint16(container, lazy_fragment),
		ColumnBuffer::Float4(container) => from_float4(container, lazy_fragment),
		ColumnBuffer::Float8(container) => from_float8(container, lazy_fragment),
		ColumnBuffer::Utf8 {
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
) -> Result<ColumnBuffer>
where
	T: Copy + Display + IsNumber + Default,
{
	let mut out = ColumnBuffer::with_capacity(Type::Boolean, container.len());
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
		) -> Result<ColumnBuffer> {
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
		) -> Result<ColumnBuffer> {
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

fn from_utf8(container: &Utf8Container, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(Type::Boolean, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let temp_fragment = Fragment::internal(container.get(idx).unwrap());
			match parse_bool(temp_fragment) {
				Ok(b) => out.push(b),
				Err(mut e) => {
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
