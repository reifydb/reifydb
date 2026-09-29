// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Display, sync::Arc};

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::{Fragment, LazyFragment},
	value::{
		boolean::parse::parse_bool,
		column_view::{ColumnView, ViewData},
		container::wide_int_array::wides,
		is::IsNumber,
		value_type::ValueType,
	},
};

use crate::value::column::builder::ColumnBuilder;

pub fn to_boolean(data: &ColumnView, lazy_fragment: impl LazyFragment) -> Result<(FieldRef, ArrayRef)> {
	let name = data.field.name();
	match &data.data {
		ViewData::Int1(container) => from_int1(container.values(), name, lazy_fragment),
		ViewData::Int2(container) => from_int2(container.values(), name, lazy_fragment),
		ViewData::Int4(container) => from_int4(container.values(), name, lazy_fragment),
		ViewData::Int8(container) => from_int8(container.values(), name, lazy_fragment),
		ViewData::Int16(container) => from_int16(&wides::<i128>(container), name, lazy_fragment),
		ViewData::Uint1(container) => from_uint1(container.values(), name, lazy_fragment),
		ViewData::Uint2(container) => from_uint2(container.values(), name, lazy_fragment),
		ViewData::Uint4(container) => from_uint4(container.values(), name, lazy_fragment),
		ViewData::Uint8(container) => from_uint8(container.values(), name, lazy_fragment),
		ViewData::Uint16(container) => from_uint16(&wides::<u128>(container), name, lazy_fragment),
		ViewData::Float4(container) => from_float4(container.values(), name, lazy_fragment),
		ViewData::Float8(container) => from_float8(container.values(), name, lazy_fragment),
		ViewData::Utf8 {
			container,
			..
		} => from_utf8(container, name, lazy_fragment),
		_ => {
			let from = data.get_type();
			Err(TypeError::UnsupportedCast {
				from,
				to: ValueType::Boolean,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

fn to_bool<T>(
	container: &[T],
	name: &str,
	lazy_fragment: impl LazyFragment,
	validate: impl Fn(T) -> Option<bool>,
) -> Result<(FieldRef, ArrayRef)>
where
	T: Copy + Display + IsNumber + Default,
{
	let mut out = ColumnBuilder::with_capacity(ValueType::Boolean, container.len());
	for &value in container {
		match validate(value) {
			Some(b) => out.push::<bool>(b),
			None => {
				let base_fragment = lazy_fragment.fragment();
				let error_fragment = Fragment::Statement {
					text: Arc::from(value.to_string()),
					line: base_fragment.line(),
					column: base_fragment.column(),
				};
				return Err(TypeError::InvalidNumberBoolean {
					fragment: error_fragment,
				}
				.into());
			}
		}
	}
	Ok(out.finish(name))
}

macro_rules! impl_integer_to_bool {
	($fn_name:ident, $type:ty) => {
		#[inline]
		fn $fn_name(
			container: &[$type],
			name: &str,
			lazy_fragment: impl LazyFragment,
		) -> Result<(FieldRef, ArrayRef)> {
			to_bool(container, name, lazy_fragment, |val| match val {
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
			container: &[$type],
			name: &str,
			lazy_fragment: impl LazyFragment,
		) -> Result<(FieldRef, ArrayRef)> {
			to_bool(container, name, lazy_fragment, |val| {
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
	container: &LargeStringArray,
	name: &str,
	lazy_fragment: impl LazyFragment,
) -> Result<(FieldRef, ArrayRef)> {
	let mut out = ColumnBuilder::with_capacity(ValueType::Boolean, container.len());
	for idx in 0..container.len() {
		if container.is_valid(idx) {
			let temp_fragment = Fragment::internal(container.value(idx));
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
	Ok(out.finish(name))
}
