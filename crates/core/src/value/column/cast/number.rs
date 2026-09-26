// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use arrow_array::{Array, PrimitiveArray};
use arrow_buffer::i256;
use reifydb_value::{
	Result,
	error::{Error, TypeError},
	fragment::{Fragment, LazyFragment},
	value::{
		container::{decimal_array::decimals, wide_int_array::wides},
		decimal::{Decimal, parse::parse_decimal, unscaled},
		is::IsNumber,
		number::{
			parse::{parse_float, parse_primitive_int, parse_primitive_uint},
			safe::convert::SafeConvert,
		},
		value_type::{ValueType, get::GetType},
	},
};

use super::{convert::Convert, error::CastError};
use crate::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder, push::Push};

pub fn to_number(
	ctx: impl Convert,
	data: &ColumnBuffer,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	if !target.is_number() {
		let from = data.get_type();
		return Err(TypeError::UnsupportedCast {
			from,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into());
	}

	if data.get_type().is_number() {
		return number_to_number(data, target, ctx, lazy_fragment);
	}

	if data.is_bool() {
		return boolean_to_number(data, target, ctx, lazy_fragment);
	}

	if data.is_utf8() {
		return match &target {
			ValueType::Float4 | ValueType::Float8 => text_to_float(data, target, lazy_fragment),
			ValueType::Decimal {
				..
			} => text_to_decimal(data, target, ctx, lazy_fragment),
			_ => text_to_integer(data, target, lazy_fragment),
		};
	}

	let from = data.get_type();
	Err(TypeError::UnsupportedCast {
		from,
		to: target,
		fragment: lazy_fragment.fragment(),
	}
	.into())
}

fn boolean_to_number(
	data: &ColumnBuffer,
	target: ValueType,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	if !matches!(target, ValueType::Decimal { .. }) {
		return boolean_to_primitive(data, target, lazy_fragment);
	}
	let (ones, nulls) = boolean_to_primitive(data, ValueType::Int1, || lazy_fragment.fragment())?.split_nulls();
	Ok(number_to_number(&ones, target, ctx, lazy_fragment)?.replace_nulls(nulls))
}

fn boolean_to_primitive(
	data: &ColumnBuffer,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	macro_rules! boolean_to_number {
		($target_ty:ty, $true_val:expr, $false_val:expr) => {{
			|out: &mut ColumnBuilder, val: bool| {
				out.push::<$target_ty>(if val {
					$true_val
				} else {
					$false_val
				})
			}
		}};
	}

	match data {
		ColumnBuffer::Bool(container) => {
			let converter = match &target {
				ValueType::Int1 => boolean_to_number!(i8, 1i8, 0i8),
				ValueType::Int2 => {
					boolean_to_number!(i16, 1i16, 0i16)
				}
				ValueType::Int4 => {
					boolean_to_number!(i32, 1i32, 0i32)
				}
				ValueType::Int8 => {
					boolean_to_number!(i64, 1i64, 0i64)
				}
				ValueType::Int16 => {
					boolean_to_number!(i128, 1i128, 0i128)
				}
				ValueType::Uint1 => boolean_to_number!(u8, 1u8, 0u8),
				ValueType::Uint2 => {
					boolean_to_number!(u16, 1u16, 0u16)
				}
				ValueType::Uint4 => {
					boolean_to_number!(u32, 1u32, 0u32)
				}
				ValueType::Uint8 => {
					boolean_to_number!(u64, 1u64, 0u64)
				}
				ValueType::Uint16 => {
					boolean_to_number!(u128, 1u128, 0u128)
				}
				ValueType::Float4 => {
					boolean_to_number!(f32, 1.0f32, 0.0f32)
				}
				ValueType::Float8 => {
					boolean_to_number!(f64, 1.0f64, 0.0f64)
				}
				_ => {
					let from = data.get_type();
					return Err(TypeError::UnsupportedCast {
						from,
						to: target,
						fragment: lazy_fragment.fragment(),
					}
					.into());
				}
			};

			let mut out = ColumnBuilder::with_capacity(target, container.len());
			for idx in 0..container.len() {
				if container.is_valid(idx) {
					let val = container.value(idx);
					converter(&mut out, val);
				} else {
					out.push_none();
				}
			}
			Ok(out.finish())
		}
		_ => {
			let from = data.get_type();
			Err(TypeError::UnsupportedCast {
				from,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

macro_rules! parse_and_push {
	(parse_int, $ty:ty, $target_type:expr, $out:expr, $temp_fragment:expr, $base_fragment:expr) => {{
		let result = parse_primitive_int::<$ty>($temp_fragment.clone()).map_err(|mut e| {
			e.0.with_fragment($base_fragment.clone());
			Error::from(CastError::InvalidNumber {
				fragment: $base_fragment.clone(),
				target: $target_type,
				cause: e.diagnostic(),
			})
		})?;
		$out.push::<$ty>(result);
	}};
	(parse_uint, $ty:ty, $target_type:expr, $out:expr, $temp_fragment:expr, $base_fragment:expr) => {{
		let result = parse_primitive_uint::<$ty>($temp_fragment.clone()).map_err(|mut e| {
			e.0.with_fragment($base_fragment.clone());
			Error::from(CastError::InvalidNumber {
				fragment: $base_fragment.clone(),
				target: $target_type,
				cause: e.diagnostic(),
			})
		})?;
		$out.push::<$ty>(result);
	}};
}

fn text_to_integer(data: &ColumnBuffer, target: ValueType, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	match data {
		ColumnBuffer::Utf8 {
			container,
			..
		} => {
			let base_fragment = lazy_fragment.fragment();
			let mut out = ColumnBuilder::with_capacity(target.clone(), container.len());
			for idx in 0..container.len() {
				if container.is_valid(idx) {
					let val = container.value(idx);
					let temp_fragment = Fragment::internal(val);

					match target.clone() {
						ValueType::Int1 => {
							parse_and_push!(
								parse_int,
								i8,
								ValueType::Int1,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Int2 => {
							parse_and_push!(
								parse_int,
								i16,
								ValueType::Int2,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Int4 => {
							parse_and_push!(
								parse_int,
								i32,
								ValueType::Int4,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Int8 => {
							parse_and_push!(
								parse_int,
								i64,
								ValueType::Int8,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Int16 => {
							parse_and_push!(
								parse_int,
								i128,
								ValueType::Int16,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Uint1 => {
							parse_and_push!(
								parse_uint,
								u8,
								ValueType::Uint1,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Uint2 => {
							parse_and_push!(
								parse_uint,
								u16,
								ValueType::Uint2,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Uint4 => {
							parse_and_push!(
								parse_uint,
								u32,
								ValueType::Uint4,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Uint8 => {
							parse_and_push!(
								parse_uint,
								u64,
								ValueType::Uint8,
								out,
								temp_fragment,
								base_fragment
							)
						}
						ValueType::Uint16 => {
							parse_and_push!(
								parse_uint,
								u128,
								ValueType::Uint16,
								out,
								temp_fragment,
								base_fragment
							)
						}
						_ => {
							let from = data.get_type();
							return Err(TypeError::UnsupportedCast {
								from,
								to: target,
								fragment: base_fragment.clone(),
							}
							.into());
						}
					}
				} else {
					out.push_none();
				}
			}
			Ok(out.finish())
		}
		_ => {
			let from = data.get_type();
			Err(TypeError::UnsupportedCast {
				from,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

fn text_to_float(
	column_data: &ColumnBuffer,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	if let ColumnBuffer::Utf8 {
		container,
		..
	} = column_data
	{
		let base_fragment = lazy_fragment.fragment();
		let mut out = ColumnBuilder::with_capacity(target.clone(), container.len());
		for idx in 0..container.len() {
			if container.is_valid(idx) {
				let val = container.value(idx);

				let temp_fragment = Fragment::internal(val);

				match target.clone() {
					ValueType::Float4 => out.push::<f32>(
						parse_float::<f32>(temp_fragment.clone()).map_err(|mut e| {
							e.0.with_fragment(base_fragment.clone());
							Error::from(CastError::InvalidNumber {
								fragment: base_fragment.clone(),
								target: ValueType::Float4,
								cause: e.diagnostic(),
							})
						})?,
					),

					ValueType::Float8 => out.push::<f64>(
						parse_float::<f64>(temp_fragment).map_err(|mut e| {
							e.0.with_fragment(base_fragment.clone());
							Error::from(CastError::InvalidNumber {
								fragment: base_fragment.clone(),
								target: ValueType::Float8,
								cause: e.diagnostic(),
							})
						})?,
					),
					_ => {
						let from = column_data.get_type();
						return Err(TypeError::UnsupportedCast {
							from,
							to: target,
							fragment: base_fragment.clone(),
						}
						.into());
					}
				}
			} else {
				out.push_none();
			}
		}
		Ok(out.finish())
	} else {
		let from = column_data.get_type();
		Err(TypeError::UnsupportedCast {
			from,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into())
	}
}

fn text_to_decimal(
	column_data: &ColumnBuffer,
	target: ValueType,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	if let ColumnBuffer::Utf8 {
		container,
		..
	} = column_data
	{
		let base_fragment = lazy_fragment.fragment();
		let mut out = ColumnBuilder::with_capacity(target.clone(), container.len());
		for idx in 0..container.len() {
			if container.is_valid(idx) {
				let val = container.value(idx);
				let temp_fragment = Fragment::internal(val);

				let result = parse_decimal(temp_fragment.clone()).map_err(|mut e| {
					e.0.with_fragment(base_fragment.clone());
					Error::from(CastError::InvalidNumber {
						fragment: base_fragment.clone(),
						target: target.clone(),
						cause: e.diagnostic(),
					})
				})?;
				push_fitted(&ctx, &mut out, result, &target, base_fragment.clone())?;
			} else {
				out.push_none();
			}
		}
		Ok(out.finish())
	} else {
		let from = column_data.get_type();
		Err(TypeError::UnsupportedCast {
			from,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into())
	}
}

fn number_to_number(
	data: &ColumnBuffer,
	target: ValueType,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	if !target.is_number() {
		return Err(TypeError::UnsupportedCast {
			from: data.get_type(),
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into());
	}

	macro_rules! cast {
            (
                $src_variant:ident, $src_ty:ty,
                to => [ $( ($dst_variant:ident, $dst_ty:ty) ),* ]
                $(, family => [ $( ($family_variant:ident, $family_ty:ty) ),* ])?
            ) => {
                cast!(
                    $src_variant via PrimitiveArray::values, $src_ty,
                    to => [ $( ($dst_variant, $dst_ty) ),* ]
                    $(, family => [ $( ($family_variant, $family_ty) ),* ])?
                );
            };
            (
                $src_variant:ident via $values:path, $src_ty:ty,
                to => [ $( ($dst_variant:ident, $dst_ty:ty) ),* ]
                $(, family => [ $( ($family_variant:ident, $family_ty:ty) ),* ])?
            ) => {
            if let ColumnBuffer::$src_variant(container) = data {
                    let values = $values(container);
                    match target {
                        $(
                        ValueType::$dst_variant => return convert_vec::<$src_ty, $dst_ty>(
                            &values,
                                ctx,
                                lazy_fragment,
                                ValueType::$dst_variant,
                                ColumnBuilder::push::<$dst_ty>,
                            ),
                        )*
                        $($(
                        ValueType::$family_variant { .. } => return convert_family::<$src_ty, $family_ty>(
                            &values,
                                ctx,
                                lazy_fragment,
                                target,
                                ColumnBuilder::push::<$family_ty>,
                            ),
                        )*)?
                        _ => {}
                    }
                }
            }
        }

	cast!(Float4, f32,
	    to => [(Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Float8, f64,
	    to => [(Float4, f32), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Int1, i8,
	    to => [(Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Int2, i16,
	    to => [(Int1, i8), (Int4, i32), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Int4, i32,
	    to => [(Int1, i8), (Int2, i16), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Int8, i64,
	    to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Int16 via wides::<i128>, i128,
	    to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Uint1, u8,
	    to => [(Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Uint2, u16,
	    to => [(Uint1, u8), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Uint4, u32,
	    to => [(Uint1, u8), (Uint2, u16), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Uint8, u64,
	    to => [(Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	    family => [(Decimal, Decimal)]
	);

	cast!(Uint16 via wides::<u128>, u128,
	    to => [(Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	    family => [(Decimal, Decimal)]
	);

	if let ColumnBuffer::Decimal(container) = data {
		let container = &decimals(container);
		match target {
			ValueType::Int1 => {
				return convert_vec_clone::<Decimal, i8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int1,
					ColumnBuilder::push::<i8>,
				);
			}
			ValueType::Int2 => {
				return convert_vec_clone::<Decimal, i16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int2,
					ColumnBuilder::push::<i16>,
				);
			}
			ValueType::Int4 => {
				return convert_vec_clone::<Decimal, i32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int4,
					ColumnBuilder::push::<i32>,
				);
			}
			ValueType::Int8 => {
				return convert_vec_clone::<Decimal, i64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int8,
					ColumnBuilder::push::<i64>,
				);
			}
			ValueType::Int16 => {
				return convert_vec_clone::<Decimal, i128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int16,
					ColumnBuilder::push::<i128>,
				);
			}
			ValueType::Uint1 => {
				return convert_vec_clone::<Decimal, u8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint1,
					ColumnBuilder::push::<u8>,
				);
			}
			ValueType::Uint2 => {
				return convert_vec_clone::<Decimal, u16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint2,
					ColumnBuilder::push::<u16>,
				);
			}
			ValueType::Uint4 => {
				return convert_vec_clone::<Decimal, u32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint4,
					ColumnBuilder::push::<u32>,
				);
			}
			ValueType::Uint8 => {
				return convert_vec_clone::<Decimal, u64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint8,
					ColumnBuilder::push::<u64>,
				);
			}
			ValueType::Uint16 => {
				return convert_vec_clone::<Decimal, u128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint16,
					ColumnBuilder::push::<u128>,
				);
			}
			ValueType::Float4 => {
				return convert_vec_clone::<Decimal, f32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float4,
					ColumnBuilder::push::<f32>,
				);
			}
			ValueType::Float8 => {
				return convert_vec_clone::<Decimal, f64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float8,
					ColumnBuilder::push::<f64>,
				);
			}
			ValueType::Decimal {
				..
			} => {
				return convert_family::<Decimal, Decimal>(
					container,
					ctx,
					lazy_fragment,
					target,
					ColumnBuilder::push::<Decimal>,
				);
			}
			_ => {}
		}
	}

	let from = data.get_type();
	Err(TypeError::UnsupportedCast {
		from,
		to: target,
		fragment: lazy_fragment.fragment(),
	}
	.into())
}

struct Fit<T> {
	value: T,
	precision: u8,
	scale: u8,
}

impl<T> Fit<T> {
	fn new(value: T, target: &ValueType) -> Self {
		let (precision, scale) = match target {
			ValueType::Decimal {
				precision,
				scale,
			} => (precision.value(), scale.value()),
			other => unreachable!("{other:?} carries no precision"),
		};
		Self {
			value,
			precision,
			scale,
		}
	}

	fn bound(&self) -> i256 {
		unscaled::pow10(self.precision).expect("a precision is at most 76 digits").wrapping_sub(i256::ONE)
	}
}

impl<T: GetType> GetType for Fit<T> {
	fn get_type() -> ValueType {
		T::get_type()
	}
}

impl SafeConvert<Decimal> for Fit<Decimal> {
	fn checked_convert(self) -> Option<Decimal> {
		let rounded = self.value.round_to_scale(self.scale)?;
		(rounded.digits() <= self.precision).then_some(rounded)
	}

	fn saturating_convert(self) -> Decimal {
		let bound = self.bound();
		let unscaled = match self.value.round_to_scale(self.scale) {
			Some(rounded) => rounded.unscaled().clamp(bound.wrapping_neg(), bound),
			None if self.value.is_negative() => bound.wrapping_neg(),
			None => bound,
		};
		Decimal::from_parts(unscaled, self.scale).expect("a clamped value is a decimal")
	}

	fn wrapping_convert(self) -> Decimal {
		self.saturating_convert()
	}
}

fn push_fitted<T>(
	ctx: &impl Convert,
	out: &mut ColumnBuilder,
	value: T,
	target: &ValueType,
	fragment: Fragment,
) -> Result<()>
where
	T: GetType + Debug,
	Fit<T>: SafeConvert<T>,
	ColumnBuilder: Push<T>,
{
	match ctx.convert::<Fit<T>, T>(Fit::new(value, target), fragment)? {
		Some(value) => out.push::<T>(value),
		None => out.push_none(),
	}
	Ok(())
}

fn convert_family<From, To>(
	values: &[From],
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
	target: ValueType,
	mut push: impl FnMut(&mut ColumnBuilder, To),
) -> Result<ColumnBuffer>
where
	From: Clone + SafeConvert<To> + GetType,
	To: GetType,
	Fit<To>: SafeConvert<To>,
{
	let mut out = ColumnBuilder::with_capacity(target.clone(), values.len());
	for value in values {
		let converted = match ctx.convert::<From, To>(value.clone(), lazy_fragment.fragment())? {
			Some(value) => {
				ctx.convert::<Fit<To>, To>(Fit::new(value, &target), lazy_fragment.fragment())?
			}
			None => None,
		};
		match converted {
			Some(value) => push(&mut out, value),
			None => out.push_none(),
		}
	}
	Ok(out.finish())
}

pub(crate) fn convert_vec<From, To>(
	container: &[From],
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
	target_kind: ValueType,
	mut push: impl FnMut(&mut ColumnBuilder, To),
) -> Result<ColumnBuffer>
where
	From: Copy + SafeConvert<To> + GetType + IsNumber + Default,
	To: GetType,
{
	let mut out = ColumnBuilder::with_capacity(target_kind, container.len());
	for &val in container {
		let fragment = lazy_fragment.fragment();
		match ctx.convert::<From, To>(val, fragment)? {
			Some(v) => push(&mut out, v),
			None => out.push_none(),
		}
	}
	Ok(out.finish())
}

pub(crate) fn convert_vec_clone<From, To>(
	container: &[From],
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
	target_kind: ValueType,
	mut push: impl FnMut(&mut ColumnBuilder, To),
) -> Result<ColumnBuffer>
where
	From: Clone + SafeConvert<To> + GetType + IsNumber + Default,
	To: GetType,
{
	let mut out = ColumnBuilder::with_capacity(target_kind, container.len());
	for val in container {
		let val = val.clone();
		let fragment = lazy_fragment.fragment();
		match ctx.convert::<From, To>(val, fragment)? {
			Some(v) => push(&mut out, v),
			None => out.push_none(),
		}
	}
	Ok(out.finish())
}

#[cfg(test)]
pub mod tests {
	mod convert {
		use std::mem;

		use reifydb_value::{
			Result,
			fragment::Fragment,
			value::{
				number::safe::convert::SafeConvert,
				value_type::{ValueType, get::GetType},
			},
		};

		use crate::value::column::cast::{convert::Convert, number::convert_vec};

		#[test]
		fn test_promote_ok() {
			let data = [1i8, 2i8];
			let ctx = TestCtx::new();

			let result = convert_vec::<i8, i16>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			let slice: &[i16] = result.as_slice();
			assert_eq!(slice, &[1i16, 2i16]);
		}

		#[test]
		fn test_promote_none_maps_to_undefined() {
			// The test ctx converts 42 to none.
			let data = [42i8];
			let ctx = TestCtx::new();

			let result = convert_vec::<i8, i16>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_promote_valid_input_is_defined() {
			// A container with no Option wrapper is fully defined, so a value that converts
			// successfully must stay defined.
			let data = [1i8];
			let ctx = TestCtx::new();

			let result = convert_vec::<i8, i16>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			assert!(result.is_defined(0));
			let slice = result.as_slice::<i16>();
			assert_eq!(slice, &[1i16]);
		}

		#[test]
		fn test_promote_conversion_failure_is_undefined() {
			// Only 42 fails to convert; every other value must survive as defined.
			let data = [1i8, 42i8, 3i8, 4i8];
			let ctx = TestCtx::new();

			let result = convert_vec::<i8, i16>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			let slice = result.as_slice::<i16>();
			assert_eq!(slice, &[1i16, 0, 3i16, 4i16]);
			assert!(result.is_defined(0));
			assert!(!result.is_defined(1));
			assert!(result.is_defined(2));
			assert!(result.is_defined(3));
		}

		struct TestCtx;

		impl TestCtx {
			fn new() -> Self {
				Self
			}
		}

		impl Convert for &TestCtx {
			/// Simulates a conversion failure for one- and two-byte integer sources only.
			fn convert<From, To>(&self, val: From, _fragment: impl Into<Fragment>) -> Result<Option<To>>
			where
				From: SafeConvert<To> + GetType,
				To: GetType,
			{
				// Only 42 is made to fail, at either width.
				if mem::size_of::<From>() == 1 {
					// SAFETY: the size check above pins From to a one-byte integer, so it is
					// layout-compatible with i8 and the copy reads only initialized bytes.
					let raw: i8 = unsafe { mem::transmute_copy(&val) };
					if raw == 42 {
						return Ok(None);
					}
				} else if mem::size_of::<From>() == 2 {
					// SAFETY: the size check above pins From to a two-byte integer, so it is
					// layout-compatible with i16 and the copy reads only initialized bytes.
					let raw: i16 = unsafe { mem::transmute_copy(&val) };
					if raw == 42 {
						return Ok(None);
					}
				}
				Ok(Some(val.checked_convert().unwrap()))
			}
		}

		#[test]
		fn test_demote_ok() {
			let data = [1i16, 2i16];
			let ctx = TestCtx::new();

			let result = convert_vec::<i16, i8>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			let slice: &[i8] = result.as_slice();
			assert_eq!(slice, &[1i8, 2i8]);
			assert!(result.is_defined(0));
			assert!(result.is_defined(1));
		}

		#[test]
		fn test_demote_none_maps_to_undefined() {
			let data = [42i16];
			let ctx = TestCtx::new();

			let result = convert_vec::<i16, i8>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_demote_valid_input_is_defined() {
			// A container with no Option wrapper is fully defined, so a value that converts
			// successfully must stay defined.
			let data = [1i16];
			let ctx = TestCtx::new();

			let result = convert_vec::<i16, i8>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			assert!(result.is_defined(0));
			let slice: &[i8] = result.as_slice();
			assert_eq!(slice, &[1i8]);
		}

		#[test]
		fn test_demote_conversion_failure_is_undefined() {
			// Only 42 fails to convert; every other value must survive as defined.
			let data = [1i16, 42i16, 3i16, 4i16];
			let ctx = TestCtx::new();

			let result = convert_vec::<i16, i8>(
				&data,
				&ctx,
				|| Fragment::testing_empty(),
				ValueType::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			let slice: &[i8] = result.as_slice();
			assert_eq!(slice, &[1i8, 0, 3i8, 4i8]);
			assert!(result.is_defined(0));
			assert!(!result.is_defined(1));
			assert!(result.is_defined(2));
			assert!(result.is_defined(3));
		}
	}
}
