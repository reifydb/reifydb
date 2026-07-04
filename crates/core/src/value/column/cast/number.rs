// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	error::{Error, TypeError},
	fragment::{Fragment, LazyFragment},
	value::{
		container::number::NumberContainer,
		decimal::{Decimal, parse::parse_decimal},
		int::Int,
		is::IsNumber,
		number::{
			parse::{parse_float, parse_primitive_int, parse_primitive_uint},
			safe::convert::SafeConvert,
		},
		uint::Uint,
		value_type::{ValueType, get::GetType},
	},
};

use super::{convert::Convert, error::CastError};
use crate::value::column::buffer::ColumnBuffer;

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
		return boolean_to_number(data, target, lazy_fragment);
	}

	if data.is_utf8() {
		return match &target {
			ValueType::Float4 | ValueType::Float8 => text_to_float(data, target, lazy_fragment),
			ValueType::Decimal => text_to_decimal(data, target, lazy_fragment),
			_ => text_to_integer(data, target, lazy_fragment),
		};
	}

	if data.is_float() {
		return float_to_integer(data, target, lazy_fragment);
	}

	let from = data.get_type();
	Err(TypeError::UnsupportedCast {
		from,
		to: target,
		fragment: lazy_fragment.fragment(),
	}
	.into())
}

fn boolean_to_number(data: &ColumnBuffer, target: ValueType, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	macro_rules! boolean_to_number {
		($target_ty:ty, $true_val:expr, $false_val:expr) => {{
			|out: &mut ColumnBuffer, val: bool| {
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
				ValueType::Int => |out: &mut ColumnBuffer, val: bool| {
					out.push::<Int>(if val {
						Int::from_i64(1)
					} else {
						Int::from_i64(0)
					})
				},
				ValueType::Uint => |out: &mut ColumnBuffer, val: bool| {
					out.push::<Uint>(if val {
						Uint::from_u64(1)
					} else {
						Uint::from_u64(0)
					})
				},
				ValueType::Decimal => |out: &mut ColumnBuffer, val: bool| {
					let decimal = if val {
						Decimal::from_i64(1)
					} else {
						Decimal::from_i64(0)
					};
					out.push::<Decimal>(decimal)
				},
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

			let mut out = ColumnBuffer::with_capacity(target, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container.data().get(idx);
					converter(&mut out, val);
				} else {
					out.push_none();
				}
			}
			Ok(out)
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

fn float_to_integer(data: &ColumnBuffer, target: ValueType, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	match data {
		ColumnBuffer::Float4(container) => match &target {
			ValueType::Int1 => f32_to_i8_vec(container),
			ValueType::Int2 => f32_to_i16_vec(container),
			ValueType::Int4 => f32_to_i32_vec(container),
			ValueType::Int8 => f32_to_i64_vec(container),
			ValueType::Int16 => f32_to_i128_vec(container),
			ValueType::Uint1 => f32_to_u8_vec(container),
			ValueType::Uint2 => f32_to_u16_vec(container),
			ValueType::Uint4 => f32_to_u32_vec(container),
			ValueType::Uint8 => f32_to_u64_vec(container),
			ValueType::Uint16 => f32_to_u128_vec(container),
			ValueType::Int => f32_to_int_vec(container),
			ValueType::Uint => f32_to_uint_vec(container),
			ValueType::Decimal => f32_to_decimal_vec(container, target),
			_ => {
				let from = data.get_type();
				Err(TypeError::UnsupportedCast {
					from,
					to: target,
					fragment: lazy_fragment.fragment(),
				}
				.into())
			}
		},
		ColumnBuffer::Float8(container) => match &target {
			ValueType::Int1 => f64_to_i8_vec(container),
			ValueType::Int2 => f64_to_i16_vec(container),
			ValueType::Int4 => f64_to_i32_vec(container),
			ValueType::Int8 => f64_to_i64_vec(container),
			ValueType::Int16 => f64_to_i128_vec(container),
			ValueType::Uint1 => f64_to_u8_vec(container),
			ValueType::Uint2 => f64_to_u16_vec(container),
			ValueType::Uint4 => f64_to_u32_vec(container),
			ValueType::Uint8 => f64_to_u64_vec(container),
			ValueType::Uint16 => f64_to_u128_vec(container),
			ValueType::Int => f64_to_int_vec(container),
			ValueType::Uint => f64_to_uint_vec(container),
			ValueType::Decimal => f64_to_decimal_vec(container, target),
			_ => {
				let from = data.get_type();
				Err(TypeError::UnsupportedCast {
					from,
					to: target,
					fragment: lazy_fragment.fragment(),
				}
				.into())
			}
		},
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
			let mut out = ColumnBuffer::with_capacity(target.clone(), container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container.get(idx).unwrap();
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
						ValueType::Int => {
							let result = parse_primitive_int(temp_fragment.clone())
								.map_err(|mut e| {
									e.0.with_fragment(base_fragment.clone());
									Error::from(CastError::InvalidNumber {
										fragment: base_fragment.clone(),
										target: ValueType::Int,
										cause: e.diagnostic(),
									})
								})?;
							out.push::<Int>(result);
						}
						ValueType::Uint => {
							let result = parse_primitive_uint(temp_fragment.clone())
								.map_err(|mut e| {
									e.0.with_fragment(base_fragment.clone());
									Error::from(CastError::InvalidNumber {
										fragment: base_fragment.clone(),
										target: ValueType::Uint,
										cause: e.diagnostic(),
									})
								})?;
							out.push::<Uint>(result);
						}
						ValueType::Decimal => {
							let target_clone = target.clone();
							let result = parse_decimal(temp_fragment.clone()).map_err(
								|mut e| {
									e.0.with_fragment(base_fragment.clone());
									Error::from(CastError::InvalidNumber {
										fragment: base_fragment.clone(),
										target: target_clone,
										cause: e.diagnostic(),
									})
								},
							)?;
							out.push::<Decimal>(result);
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
			Ok(out)
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
		let mut out = ColumnBuffer::with_capacity(target.clone(), container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = container.get(idx).unwrap();

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
		Ok(out)
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
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	if let ColumnBuffer::Utf8 {
		container,
		..
	} = column_data
	{
		let base_fragment = lazy_fragment.fragment();
		let mut out = ColumnBuffer::with_capacity(target.clone(), container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = container.get(idx).unwrap();
				let temp_fragment = Fragment::internal(val);

				let result = parse_decimal(temp_fragment.clone()).map_err(|mut e| {
					e.0.with_fragment(base_fragment.clone());
					Error::from(CastError::InvalidNumber {
						fragment: base_fragment.clone(),
						target: target.clone(),
						cause: e.diagnostic(),
					})
				})?;
				out.push::<Decimal>(result);
			} else {
				out.push_none();
			}
		}
		Ok(out)
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

macro_rules! float_to_int_vec {
	($fn_name:ident, $float_ty:ty, $int_ty:ty, $target_type:expr, $min_val:expr, $max_val:expr) => {
		fn $fn_name(container: &NumberContainer<$float_ty>) -> Result<ColumnBuffer>
		where
			$float_ty: Copy + IsNumber,
		{
			let mut out = ColumnBuffer::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container[idx];
					let truncated = val.trunc();
					if truncated >= $min_val && truncated <= $max_val {
						out.push::<$int_ty>(truncated as $int_ty);
					} else {
						out.push_none();
					}
				} else {
					out.push_none();
				}
			}
			Ok(out)
		}
	};
}

float_to_int_vec!(f32_to_i8_vec, f32, i8, ValueType::Int1, i8::MIN as f32, i8::MAX as f32);
float_to_int_vec!(f32_to_i16_vec, f32, i16, ValueType::Int2, i16::MIN as f32, i16::MAX as f32);
float_to_int_vec!(f32_to_i32_vec, f32, i32, ValueType::Int4, i32::MIN as f32, i32::MAX as f32);
float_to_int_vec!(f32_to_i64_vec, f32, i64, ValueType::Int8, i64::MIN as f32, i64::MAX as f32);
float_to_int_vec!(f32_to_i128_vec, f32, i128, ValueType::Int16, i128::MIN as f32, i128::MAX as f32);
float_to_int_vec!(f32_to_u8_vec, f32, u8, ValueType::Uint1, 0.0, u8::MAX as f32);
float_to_int_vec!(f32_to_u16_vec, f32, u16, ValueType::Uint2, 0.0, u16::MAX as f32);
float_to_int_vec!(f32_to_u32_vec, f32, u32, ValueType::Uint4, 0.0, u32::MAX as f32);
float_to_int_vec!(f32_to_u64_vec, f32, u64, ValueType::Uint8, 0.0, u64::MAX as f32);
float_to_int_vec!(f32_to_u128_vec, f32, u128, ValueType::Uint16, 0.0, u128::MAX as f32);

float_to_int_vec!(f64_to_i8_vec, f64, i8, ValueType::Int1, i8::MIN as f64, i8::MAX as f64);
float_to_int_vec!(f64_to_i16_vec, f64, i16, ValueType::Int2, i16::MIN as f64, i16::MAX as f64);
float_to_int_vec!(f64_to_i32_vec, f64, i32, ValueType::Int4, i32::MIN as f64, i32::MAX as f64);
float_to_int_vec!(f64_to_i64_vec, f64, i64, ValueType::Int8, i64::MIN as f64, i64::MAX as f64);
float_to_int_vec!(f64_to_i128_vec, f64, i128, ValueType::Int16, i128::MIN as f64, i128::MAX as f64);
float_to_int_vec!(f64_to_u8_vec, f64, u8, ValueType::Uint1, 0.0, u8::MAX as f64);
float_to_int_vec!(f64_to_u16_vec, f64, u16, ValueType::Uint2, 0.0, u16::MAX as f64);
float_to_int_vec!(f64_to_u32_vec, f64, u32, ValueType::Uint4, 0.0, u32::MAX as f64);
float_to_int_vec!(f64_to_u64_vec, f64, u64, ValueType::Uint8, 0.0, u64::MAX as f64);
float_to_int_vec!(f64_to_u128_vec, f64, u128, ValueType::Uint16, 0.0, u128::MAX as f64);

fn f32_to_int_vec(container: &NumberContainer<f32>) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(ValueType::Int, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			let int = Int::from_i64(truncated as i64);
			out.push::<Int>(int);
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

fn f64_to_int_vec(container: &NumberContainer<f64>) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(ValueType::Int, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			let int = Int::from_i64(truncated as i64);
			out.push::<Int>(int);
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

fn f32_to_uint_vec(container: &NumberContainer<f32>) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(ValueType::Uint, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			if truncated >= 0.0 {
				let uint = Uint::from_u64(truncated as u64);
				out.push::<Uint>(uint);
			} else {
				out.push_none();
			}
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

fn f64_to_uint_vec(container: &NumberContainer<f64>) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(ValueType::Uint, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			if truncated >= 0.0 {
				let uint = Uint::from_u64(truncated as u64);
				out.push::<Uint>(uint);
			} else {
				out.push_none();
			}
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

fn f32_to_decimal_vec(container: &NumberContainer<f32>, target: ValueType) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(target, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];

			let decimal = Decimal::from_i64(val.trunc() as i64);
			out.push::<Decimal>(decimal);
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

fn f64_to_decimal_vec(container: &NumberContainer<f64>, target: ValueType) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(target, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];

			let decimal = Decimal::from_i64(val.trunc() as i64);
			out.push::<Decimal>(decimal);
		} else {
			out.push_none();
		}
	}
	Ok(out)
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
                $(, to_struct => [ $( ($struct_variant:ident, $struct_ty:ty) ),* ])?
            ) => {
            if let ColumnBuffer::$src_variant(container) = data {
                    match target {
                        $(
                        ValueType::$dst_variant => return convert_vec::<$src_ty, $dst_ty>(
                            &container,
                                ctx,
                                lazy_fragment,
                                ValueType::$dst_variant,
                                ColumnBuffer::push::<$dst_ty>,
                            ),
                        )*
                        $($(
                        ValueType::$struct_variant { .. } => return convert_vec::<$src_ty, $struct_ty>(
                            &container,
                                ctx,
                                lazy_fragment,
                                target,
                                ColumnBuffer::push::<$struct_ty>,
                            ),
                        )*)?
                        _ => {}
                    }
                }
            }
        }

	cast!(Float4, f32,
	    to => [(Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Float8, f64,
	    to => [(Float4, f32), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Int1, i8,
	    to => [(Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Int2, i16,
	    to => [(Int1, i8), (Int4, i32), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Int4, i32,
	    to => [(Int1, i8), (Int2, i16), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Int8, i64,
	    to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Int16, i128,
	    to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Uint1, u8,
	    to => [(Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Uint2, u16,
	    to => [(Uint1, u8), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Uint4, u32,
	    to => [(Uint1, u8), (Uint2, u16), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Uint8, u64,
	    to => [(Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	cast!(Uint16, u128,
	    to => [(Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Int, Int), (Uint, Uint)],
	    to_struct => [(Decimal, Decimal)]
	);

	if let ColumnBuffer::Int {
		container,
		..
	} = data
	{
		match target {
			ValueType::Int1 => {
				return convert_vec_clone::<Int, i8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int1,
					ColumnBuffer::push::<i8>,
				);
			}
			ValueType::Int2 => {
				return convert_vec_clone::<Int, i16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int2,
					ColumnBuffer::push::<i16>,
				);
			}
			ValueType::Int4 => {
				return convert_vec_clone::<Int, i32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int4,
					ColumnBuffer::push::<i32>,
				);
			}
			ValueType::Int8 => {
				return convert_vec_clone::<Int, i64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int8,
					ColumnBuffer::push::<i64>,
				);
			}
			ValueType::Int16 => {
				return convert_vec_clone::<Int, i128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int16,
					ColumnBuffer::push::<i128>,
				);
			}
			ValueType::Uint1 => {
				return convert_vec_clone::<Int, u8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint1,
					ColumnBuffer::push::<u8>,
				);
			}
			ValueType::Uint2 => {
				return convert_vec_clone::<Int, u16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint2,
					ColumnBuffer::push::<u16>,
				);
			}
			ValueType::Uint4 => {
				return convert_vec_clone::<Int, u32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint4,
					ColumnBuffer::push::<u32>,
				);
			}
			ValueType::Uint8 => {
				return convert_vec_clone::<Int, u64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint8,
					ColumnBuffer::push::<u64>,
				);
			}
			ValueType::Uint16 => {
				return convert_vec_clone::<Int, u128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint16,
					ColumnBuffer::push::<u128>,
				);
			}
			ValueType::Float4 => {
				return convert_vec_clone::<Int, f32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float4,
					ColumnBuffer::push::<f32>,
				);
			}
			ValueType::Float8 => {
				return convert_vec_clone::<Int, f64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float8,
					ColumnBuffer::push::<f64>,
				);
			}
			ValueType::Uint => {
				return convert_vec_clone::<Int, Uint>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint,
					ColumnBuffer::push::<Uint>,
				);
			}
			ValueType::Decimal => {
				return convert_vec_clone::<Int, Decimal>(
					container,
					ctx,
					lazy_fragment,
					target,
					ColumnBuffer::push::<Decimal>,
				);
			}
			_ => {}
		}
	}

	if let ColumnBuffer::Uint {
		container,
		..
	} = data
	{
		match target {
			ValueType::Uint1 => {
				return convert_vec_clone::<Uint, u8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint1,
					ColumnBuffer::push::<u8>,
				);
			}
			ValueType::Uint2 => {
				return convert_vec_clone::<Uint, u16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint2,
					ColumnBuffer::push::<u16>,
				);
			}
			ValueType::Uint4 => {
				return convert_vec_clone::<Uint, u32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint4,
					ColumnBuffer::push::<u32>,
				);
			}
			ValueType::Uint8 => {
				return convert_vec_clone::<Uint, u64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint8,
					ColumnBuffer::push::<u64>,
				);
			}
			ValueType::Uint16 => {
				return convert_vec_clone::<Uint, u128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint16,
					ColumnBuffer::push::<u128>,
				);
			}
			ValueType::Int1 => {
				return convert_vec_clone::<Uint, i8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int1,
					ColumnBuffer::push::<i8>,
				);
			}
			ValueType::Int2 => {
				return convert_vec_clone::<Uint, i16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int2,
					ColumnBuffer::push::<i16>,
				);
			}
			ValueType::Int4 => {
				return convert_vec_clone::<Uint, i32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int4,
					ColumnBuffer::push::<i32>,
				);
			}
			ValueType::Int8 => {
				return convert_vec_clone::<Uint, i64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int8,
					ColumnBuffer::push::<i64>,
				);
			}
			ValueType::Int16 => {
				return convert_vec_clone::<Uint, i128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int16,
					ColumnBuffer::push::<i128>,
				);
			}
			ValueType::Float4 => {
				return convert_vec_clone::<Uint, f32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float4,
					ColumnBuffer::push::<f32>,
				);
			}
			ValueType::Float8 => {
				return convert_vec_clone::<Uint, f64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float8,
					ColumnBuffer::push::<f64>,
				);
			}
			ValueType::Int => {
				return convert_vec_clone::<Uint, Int>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int,
					ColumnBuffer::push::<Int>,
				);
			}
			ValueType::Decimal => {
				return convert_vec_clone::<Uint, Decimal>(
					container,
					ctx,
					lazy_fragment,
					target,
					ColumnBuffer::push::<Decimal>,
				);
			}
			_ => {}
		}
	}

	if let ColumnBuffer::Decimal {
		container,
		..
	} = data
	{
		match target {
			ValueType::Int1 => {
				return convert_vec_clone::<Decimal, i8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int1,
					ColumnBuffer::push::<i8>,
				);
			}
			ValueType::Int2 => {
				return convert_vec_clone::<Decimal, i16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int2,
					ColumnBuffer::push::<i16>,
				);
			}
			ValueType::Int4 => {
				return convert_vec_clone::<Decimal, i32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int4,
					ColumnBuffer::push::<i32>,
				);
			}
			ValueType::Int8 => {
				return convert_vec_clone::<Decimal, i64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int8,
					ColumnBuffer::push::<i64>,
				);
			}
			ValueType::Int16 => {
				return convert_vec_clone::<Decimal, i128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int16,
					ColumnBuffer::push::<i128>,
				);
			}
			ValueType::Uint1 => {
				return convert_vec_clone::<Decimal, u8>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint1,
					ColumnBuffer::push::<u8>,
				);
			}
			ValueType::Uint2 => {
				return convert_vec_clone::<Decimal, u16>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint2,
					ColumnBuffer::push::<u16>,
				);
			}
			ValueType::Uint4 => {
				return convert_vec_clone::<Decimal, u32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint4,
					ColumnBuffer::push::<u32>,
				);
			}
			ValueType::Uint8 => {
				return convert_vec_clone::<Decimal, u64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint8,
					ColumnBuffer::push::<u64>,
				);
			}
			ValueType::Uint16 => {
				return convert_vec_clone::<Decimal, u128>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint16,
					ColumnBuffer::push::<u128>,
				);
			}
			ValueType::Float4 => {
				return convert_vec_clone::<Decimal, f32>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float4,
					ColumnBuffer::push::<f32>,
				);
			}
			ValueType::Float8 => {
				return convert_vec_clone::<Decimal, f64>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Float8,
					ColumnBuffer::push::<f64>,
				);
			}
			ValueType::Int => {
				return convert_vec_clone::<Decimal, Int>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Int,
					ColumnBuffer::push::<Int>,
				);
			}
			ValueType::Uint => {
				return convert_vec_clone::<Decimal, Uint>(
					container,
					ctx,
					lazy_fragment,
					ValueType::Uint,
					ColumnBuffer::push::<Uint>,
				);
			}
			ValueType::Decimal => {
				return convert_vec_clone::<Decimal, Decimal>(
					container,
					ctx,
					lazy_fragment,
					target,
					ColumnBuffer::push::<Decimal>,
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

pub(crate) fn convert_vec<From, To>(
	container: &NumberContainer<From>,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
	target_kind: ValueType,
	mut push: impl FnMut(&mut ColumnBuffer, To),
) -> Result<ColumnBuffer>
where
	From: Copy + SafeConvert<To> + GetType + IsNumber + Default,
	To: GetType,
{
	let mut out = ColumnBuffer::with_capacity(target_kind, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let fragment = lazy_fragment.fragment();
			match ctx.convert::<From, To>(val, fragment)? {
				Some(v) => push(&mut out, v),
				None => out.push_none(),
			}
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

pub(crate) fn convert_vec_clone<From, To>(
	container: &NumberContainer<From>,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
	target_kind: ValueType,
	mut push: impl FnMut(&mut ColumnBuffer, To),
) -> Result<ColumnBuffer>
where
	From: Clone + SafeConvert<To> + GetType + IsNumber + Default,
	To: GetType,
{
	let mut out = ColumnBuffer::with_capacity(target_kind, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx].clone();
			let fragment = lazy_fragment.fragment();
			match ctx.convert::<From, To>(val, fragment)? {
				Some(v) => push(&mut out, v),
				None => out.push_none(),
			}
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

#[cfg(test)]
pub mod tests {
	mod convert {
		use std::mem;

		use reifydb_value::{
			Result,
			fragment::Fragment,
			value::{
				container::number::NumberContainer,
				number::safe::convert::SafeConvert,
				value_type::{ValueType, get::GetType},
			},
		};

		use crate::value::column::cast::{convert::Convert, number::convert_vec};

		#[test]
		fn test_promote_ok() {
			let data = [1i8, 2i8];
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i8, i16>(
				&container,
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
			// 42 mapped to None
			let data = [42i8];
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i8, i16>(
				&container,
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
			// With the Option-based nullability model, containers without an
			// Option wrapper are fully defined.  Value 1 converts successfully,
			// so the result must be defined.
			let data = [1i8];
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i8, i16>(
				&container,
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
			// Only value 42 triggers a conversion failure (ctx returns None).
			// Value 3 is fully defined in the input and converts successfully.
			let data = [1i8, 42i8, 3i8, 4i8];
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i8, i16>(
				&container,
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
			/// Can only used with i8
			fn convert<From, To>(&self, val: From, _fragment: impl Into<Fragment>) -> Result<Option<To>>
			where
				From: SafeConvert<To> + GetType,
				To: GetType,
			{
				// Only simulate conversion failure for i8 == 42
				// or i16 == 42
				if mem::size_of::<From>() == 1 {
					let raw: i8 = unsafe { mem::transmute_copy(&val) };
					if raw == 42 {
						return Ok(None);
					}
				} else if mem::size_of::<From>() == 2 {
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

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i16, i8>(
				&container,
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

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i16, i8>(
				&container,
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
			// With the Option-based nullability model, containers without an
			// Option wrapper are fully defined.  Value 1 converts successfully,
			// so the result must be defined.
			let data = [1i16];
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i16, i8>(
				&container,
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
			// Only value 42 triggers a conversion failure (ctx returns None).
			// Value 3 is fully defined in the input and converts successfully.
			let data = [1i16, 42i16, 3i16, 4i16];
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec());
			let result = convert_vec::<i16, i8>(
				&container,
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
