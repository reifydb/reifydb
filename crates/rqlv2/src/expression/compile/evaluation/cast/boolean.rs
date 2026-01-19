// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cast to Boolean type

use std::fmt::Display;

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	fragment::Fragment,
	value::{
		boolean::parse::parse_bool, container::{number::NumberContainer, utf8::Utf8Container}, is::IsNumber,
		r#type::Type,
	},
};

use crate::expression::types::{EvalError, EvalResult};

pub(super) fn to_boolean(data: &ColumnData) -> EvalResult<ColumnData> {
	match data {
		ColumnData::Int1(container) => from_int1(container),
		ColumnData::Int2(container) => from_int2(container),
		ColumnData::Int4(container) => from_int4(container),
		ColumnData::Int8(container) => from_int8(container),
		ColumnData::Int16(container) => from_int16(container),
		ColumnData::Uint1(container) => from_uint1(container),
		ColumnData::Uint2(container) => from_uint2(container),
		ColumnData::Uint4(container) => from_uint4(container),
		ColumnData::Uint8(container) => from_uint8(container),
		ColumnData::Uint16(container) => from_uint16(container),
		ColumnData::Float4(container) => from_float4(container),
		ColumnData::Float8(container) => from_float8(container),
		ColumnData::Utf8 { container, .. } => from_utf8(container),
		_ => {
			let source_type = data.get_type();
			Err(EvalError::UnsupportedCast {
				from: format!("{:?}", source_type),
				to: "Boolean".to_string(),
			})
		}
	}
}

fn to_bool<T>(
	container: &NumberContainer<T>,
	validate: impl Fn(T) -> Option<bool>,
) -> EvalResult<ColumnData>
where
	T: Copy + Display + IsNumber + Default,
{
	let mut out = ColumnData::with_capacity(Type::Boolean, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			match validate(container[idx]) {
				Some(b) => out.push::<bool>(b),
				None => {
					return Err(EvalError::InvalidCast {
						details: format!("Cannot cast {} to boolean (must be 0 or 1)", container[idx]),
					});
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
		fn $fn_name(container: &NumberContainer<$type>) -> EvalResult<ColumnData> {
			to_bool(container, |val| match val {
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
		fn $fn_name(container: &NumberContainer<$type>) -> EvalResult<ColumnData> {
			to_bool(container, |val| {
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

fn from_utf8(container: &Utf8Container) -> EvalResult<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Boolean, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			// Parse with internal fragment
			let temp_fragment = Fragment::internal(&container[idx]);
			match parse_bool(temp_fragment) {
				Ok(b) => out.push(b),
				Err(_e) => {
					return Err(EvalError::InvalidCast {
						details: format!("Cannot parse '{}' as boolean", container[idx]),
					});
				}
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}
