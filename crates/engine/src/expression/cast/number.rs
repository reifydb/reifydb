// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error,
	error::diagnostic::cast,
	fragment::{Fragment, LazyFragment},
	return_error,
	value::{
		container::number::NumberContainer,
		decimal::{Decimal, parse::parse_decimal},
		int::Int,
		is::IsNumber,
		number::{
			parse::{parse_float, parse_primitive_int, parse_primitive_uint},
			safe::convert::SafeConvert,
		},
		r#type::{Type, get::GetType},
		uint::Uint,
	},
};

use crate::expression::convert::Convert;

pub fn to_number(
	ctx: impl Convert,
	data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment,
) -> crate::Result<ColumnData> {
	if !target.is_number() {
		let source_type = data.get_type();
		return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target));
	}

	if data.get_type().is_number() {
		return number_to_number(data, target, ctx, lazy_fragment);
	}

	if data.is_bool() {
		return boolean_to_number(data, target, lazy_fragment);
	}

	if data.is_utf8() {
		return match target {
			Type::Float4 | Type::Float8 => text_to_float(data, target, lazy_fragment),
			Type::Decimal {
				..
			} => text_to_decimal(data, target, lazy_fragment),
			_ => text_to_integer(data, target, lazy_fragment),
		};
	}

	if data.is_float() {
		return float_to_integer(data, target, lazy_fragment);
	}

	let source_type = data.get_type();
	return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
}

fn boolean_to_number(data: &ColumnData, target: Type, lazy_fragment: impl LazyFragment) -> crate::Result<ColumnData> {
	macro_rules! boolean_to_number {
		($target_ty:ty, $true_val:expr, $false_val:expr) => {{
			|out: &mut ColumnData, val: bool| {
				out.push::<$target_ty>(if val {
					$true_val
				} else {
					$false_val
				})
			}
		}};
	}

	match data {
		ColumnData::Bool(container) => {
			let converter = match target {
				Type::Int1 => boolean_to_number!(i8, 1i8, 0i8),
				Type::Int2 => {
					boolean_to_number!(i16, 1i16, 0i16)
				}
				Type::Int4 => {
					boolean_to_number!(i32, 1i32, 0i32)
				}
				Type::Int8 => {
					boolean_to_number!(i64, 1i64, 0i64)
				}
				Type::Int16 => {
					boolean_to_number!(i128, 1i128, 0i128)
				}
				Type::Uint1 => boolean_to_number!(u8, 1u8, 0u8),
				Type::Uint2 => {
					boolean_to_number!(u16, 1u16, 0u16)
				}
				Type::Uint4 => {
					boolean_to_number!(u32, 1u32, 0u32)
				}
				Type::Uint8 => {
					boolean_to_number!(u64, 1u64, 0u64)
				}
				Type::Uint16 => {
					boolean_to_number!(u128, 1u128, 0u128)
				}
				Type::Float4 => {
					boolean_to_number!(f32, 1.0f32, 0.0f32)
				}
				Type::Float8 => {
					boolean_to_number!(f64, 1.0f64, 0.0f64)
				}
				Type::Int => |out: &mut ColumnData, val: bool| {
					out.push::<Int>(if val {
						Int::from_i64(1)
					} else {
						Int::from_i64(0)
					})
				},
				Type::Uint => |out: &mut ColumnData, val: bool| {
					out.push::<Uint>(if val {
						Uint::from_u64(1)
					} else {
						Uint::from_u64(0)
					})
				},
				Type::Decimal {
					..
				} => |out: &mut ColumnData, val: bool| {
					let decimal = if val {
						Decimal::from_i64(1)
					} else {
						Decimal::from_i64(0)
					};
					out.push::<Decimal>(decimal)
				},
				_ => {
					let source_type = data.get_type();
					return_error!(cast::unsupported_cast(
						lazy_fragment.fragment(),
						source_type,
						target
					));
				}
			};

			let mut out = ColumnData::with_capacity(target, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container.data().get(idx);
					converter(&mut out, val);
				} else {
					out.push_undefined();
				}
			}
			Ok(out)
		}
		_ => {
			let source_type = data.get_type();
			return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
		}
	}
}

fn float_to_integer(data: &ColumnData, target: Type, lazy_fragment: impl LazyFragment) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Float4(container) => match target {
			Type::Int1 => f32_to_i8_vec(container),
			Type::Int2 => f32_to_i16_vec(container),
			Type::Int4 => f32_to_i32_vec(container),
			Type::Int8 => f32_to_i64_vec(container),
			Type::Int16 => f32_to_i128_vec(container),
			Type::Uint1 => f32_to_u8_vec(container),
			Type::Uint2 => f32_to_u16_vec(container),
			Type::Uint4 => f32_to_u32_vec(container),
			Type::Uint8 => f32_to_u64_vec(container),
			Type::Uint16 => f32_to_u128_vec(container),
			Type::Int => f32_to_int_vec(container),
			Type::Uint => f32_to_uint_vec(container),
			Type::Decimal {
				..
			} => f32_to_decimal_vec(container, target),
			_ => {
				let source_type = data.get_type();
				return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
			}
		},
		ColumnData::Float8(container) => match target {
			Type::Int1 => f64_to_i8_vec(container),
			Type::Int2 => f64_to_i16_vec(container),
			Type::Int4 => f64_to_i32_vec(container),
			Type::Int8 => f64_to_i64_vec(container),
			Type::Int16 => f64_to_i128_vec(container),
			Type::Uint1 => f64_to_u8_vec(container),
			Type::Uint2 => f64_to_u16_vec(container),
			Type::Uint4 => f64_to_u32_vec(container),
			Type::Uint8 => f64_to_u64_vec(container),
			Type::Uint16 => f64_to_u128_vec(container),
			Type::Int => f64_to_int_vec(container),
			Type::Uint => f64_to_uint_vec(container),
			Type::Decimal {
				..
			} => f64_to_decimal_vec(container, target),
			_ => {
				let source_type = data.get_type();
				return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
			}
		},
		_ => {
			let source_type = data.get_type();
			return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
		}
	}
}

macro_rules! parse_and_push {
	(parse_int, $ty:ty, $target_type:expr, $out:expr, $temp_fragment:expr, $base_fragment:expr) => {{
		let result = parse_primitive_int::<$ty>($temp_fragment.clone()).map_err(|mut e| {
			// Use the base_fragment (column reference) for
			// the error position
			e.0.with_fragment($base_fragment.clone());

			error!(cast::invalid_number($base_fragment.clone(), $target_type, e.diagnostic(),))
		})?;
		$out.push::<$ty>(result);
	}};
	(parse_uint, $ty:ty, $target_type:expr, $out:expr, $temp_fragment:expr, $base_fragment:expr) => {{
		let result = parse_primitive_uint::<$ty>($temp_fragment.clone()).map_err(|mut e| {
			// Use the base_fragment (column
			// reference) for
			// the error position
			e.0.with_fragment($base_fragment.clone());

			error!(cast::invalid_number($base_fragment.clone(), $target_type, e.diagnostic(),))
		})?;
		$out.push::<$ty>(result);
	}};
}

fn text_to_integer(data: &ColumnData, target: Type, lazy_fragment: impl LazyFragment) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Utf8 {
			container,
			..
		} => {
			let base_fragment = lazy_fragment.fragment();
			let mut out = ColumnData::with_capacity(target, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = &container[idx];
					let temp_fragment = Fragment::internal(val);

					match target {
						Type::Int1 => {
							parse_and_push!(
								parse_int,
								i8,
								Type::Int1,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Int2 => {
							parse_and_push!(
								parse_int,
								i16,
								Type::Int2,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Int4 => {
							parse_and_push!(
								parse_int,
								i32,
								Type::Int4,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Int8 => {
							parse_and_push!(
								parse_int,
								i64,
								Type::Int8,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Int16 => {
							parse_and_push!(
								parse_int,
								i128,
								Type::Int16,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Uint1 => {
							parse_and_push!(
								parse_uint,
								u8,
								Type::Uint1,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Uint2 => {
							parse_and_push!(
								parse_uint,
								u16,
								Type::Uint2,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Uint4 => {
							parse_and_push!(
								parse_uint,
								u32,
								Type::Uint4,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Uint8 => {
							parse_and_push!(
								parse_uint,
								u64,
								Type::Uint8,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Uint16 => {
							parse_and_push!(
								parse_uint,
								u128,
								Type::Uint16,
								out,
								temp_fragment,
								base_fragment
							)
						}
						Type::Int => {
							let result = parse_primitive_int(temp_fragment.clone())
								.map_err(|mut e| {
									e.0.with_fragment(base_fragment.clone());
									error!(cast::invalid_number(
										base_fragment.clone(),
										Type::Int,
										e.diagnostic(),
									))
								})?;
							out.push::<Int>(result);
						}
						Type::Uint => {
							let result = parse_primitive_uint(temp_fragment.clone())
								.map_err(|mut e| {
									e.0.with_fragment(base_fragment.clone());
									error!(cast::invalid_number(
										base_fragment.clone(),
										Type::Uint,
										e.diagnostic(),
									))
								})?;
							out.push::<Uint>(result);
						}
						Type::Decimal {
							..
						} => {
							let result = parse_decimal(temp_fragment.clone()).map_err(
								|mut e| {
									e.0.with_fragment(base_fragment.clone());
									error!(cast::invalid_number(
										base_fragment.clone(),
										target,
										e.diagnostic(),
									))
								},
							)?;
							out.push::<Decimal>(result);
						}
						_ => {
							let source_type = data.get_type();
							return_error!(cast::unsupported_cast(
								base_fragment.clone(),
								source_type,
								target
							));
						}
					}
				} else {
					out.push_undefined();
				}
			}
			Ok(out)
		}
		_ => {
			let source_type = data.get_type();
			return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
		}
	}
}

fn text_to_float<'a>(
	column_data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment,
) -> crate::Result<ColumnData> {
	if let ColumnData::Utf8 {
		container,
		..
	} = column_data
	{
		// Create base fragment once for efficiency
		let base_fragment = lazy_fragment.fragment();
		let mut out = ColumnData::with_capacity(target, container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = &container[idx];
				// Create efficient borrowed fragment for
				// parsing
				let temp_fragment = Fragment::internal(val);

				match target {
					Type::Float4 => {
						out.push::<f32>(parse_float::<f32>(temp_fragment.clone()).map_err(
							|mut e| {
								// Use the base_fragment (column reference) for the
								// error position
								e.0.with_fragment(base_fragment.clone());

								error!(cast::invalid_number(
									base_fragment.clone(),
									Type::Float4,
									e.diagnostic(),
								))
							},
						)?)
					}

					Type::Float8 => {
						out.push::<f64>(parse_float::<f64>(temp_fragment).map_err(
							|mut e| {
								// Use the base_fragment (column reference) for the
								// error position
								e.0.with_fragment(base_fragment.clone());

								error!(cast::invalid_number(
									base_fragment.clone(),
									Type::Float8,
									e.diagnostic(),
								))
							},
						)?)
					}
					_ => {
						let source_type = column_data.get_type();
						return_error!(cast::unsupported_cast(
							base_fragment.clone(),
							source_type,
							target
						));
					}
				}
			} else {
				out.push_undefined();
			}
		}
		Ok(out)
	} else {
		let source_type = column_data.get_type();
		return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
	}
}

fn text_to_decimal<'a>(
	column_data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment,
) -> crate::Result<ColumnData> {
	if let ColumnData::Utf8 {
		container,
		..
	} = column_data
	{
		let base_fragment = lazy_fragment.fragment();
		let mut out = ColumnData::with_capacity(target, container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = &container[idx];
				let temp_fragment = Fragment::internal(val);

				let result = parse_decimal(temp_fragment.clone()).map_err(|mut e| {
					e.0.with_fragment(base_fragment.clone());
					error!(cast::invalid_number(base_fragment.clone(), target, e.diagnostic(),))
				})?;
				out.push::<Decimal>(result);
			} else {
				out.push_undefined();
			}
		}
		Ok(out)
	} else {
		let source_type = column_data.get_type();
		return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
	}
}

macro_rules! float_to_int_vec {
	($fn_name:ident, $float_ty:ty, $int_ty:ty, $target_type:expr, $min_val:expr, $max_val:expr) => {
		fn $fn_name(container: &NumberContainer<$float_ty>) -> crate::Result<ColumnData>
		where
			$float_ty: Copy + IsNumber,
		{
			let mut out = ColumnData::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container[idx];
					let truncated = val.trunc();
					if truncated >= $min_val && truncated <= $max_val {
						out.push::<$int_ty>(truncated as $int_ty);
					} else {
						out.push_undefined();
					}
				} else {
					out.push_undefined();
				}
			}
			Ok(out)
		}
	};
}

float_to_int_vec!(f32_to_i8_vec, f32, i8, Type::Int1, i8::MIN as f32, i8::MAX as f32);
float_to_int_vec!(f32_to_i16_vec, f32, i16, Type::Int2, i16::MIN as f32, i16::MAX as f32);
float_to_int_vec!(f32_to_i32_vec, f32, i32, Type::Int4, i32::MIN as f32, i32::MAX as f32);
float_to_int_vec!(f32_to_i64_vec, f32, i64, Type::Int8, i64::MIN as f32, i64::MAX as f32);
float_to_int_vec!(f32_to_i128_vec, f32, i128, Type::Int16, i128::MIN as f32, i128::MAX as f32);
float_to_int_vec!(f32_to_u8_vec, f32, u8, Type::Uint1, 0.0, u8::MAX as f32);
float_to_int_vec!(f32_to_u16_vec, f32, u16, Type::Uint2, 0.0, u16::MAX as f32);
float_to_int_vec!(f32_to_u32_vec, f32, u32, Type::Uint4, 0.0, u32::MAX as f32);
float_to_int_vec!(f32_to_u64_vec, f32, u64, Type::Uint8, 0.0, u64::MAX as f32);
float_to_int_vec!(f32_to_u128_vec, f32, u128, Type::Uint16, 0.0, u128::MAX as f32);

float_to_int_vec!(f64_to_i8_vec, f64, i8, Type::Int1, i8::MIN as f64, i8::MAX as f64);
float_to_int_vec!(f64_to_i16_vec, f64, i16, Type::Int2, i16::MIN as f64, i16::MAX as f64);
float_to_int_vec!(f64_to_i32_vec, f64, i32, Type::Int4, i32::MIN as f64, i32::MAX as f64);
float_to_int_vec!(f64_to_i64_vec, f64, i64, Type::Int8, i64::MIN as f64, i64::MAX as f64);
float_to_int_vec!(f64_to_i128_vec, f64, i128, Type::Int16, i128::MIN as f64, i128::MAX as f64);
float_to_int_vec!(f64_to_u8_vec, f64, u8, Type::Uint1, 0.0, u8::MAX as f64);
float_to_int_vec!(f64_to_u16_vec, f64, u16, Type::Uint2, 0.0, u16::MAX as f64);
float_to_int_vec!(f64_to_u32_vec, f64, u32, Type::Uint4, 0.0, u32::MAX as f64);
float_to_int_vec!(f64_to_u64_vec, f64, u64, Type::Uint8, 0.0, u64::MAX as f64);
float_to_int_vec!(f64_to_u128_vec, f64, u128, Type::Uint16, 0.0, u128::MAX as f64);

// Float to Int conversion
fn f32_to_int_vec(container: &NumberContainer<f32>) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Int, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			let int = Int::from_i64(truncated as i64);
			out.push::<Int>(int);
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

fn f64_to_int_vec(container: &NumberContainer<f64>) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Int, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			let int = Int::from_i64(truncated as i64);
			out.push::<Int>(int);
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

// Float to Uint conversion
fn f32_to_uint_vec(container: &NumberContainer<f32>) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Uint, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			if truncated >= 0.0 {
				let uint = Uint::from_u64(truncated as u64);
				out.push::<Uint>(uint);
			} else {
				out.push_undefined();
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

fn f64_to_uint_vec(container: &NumberContainer<f64>) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Uint, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let truncated = val.trunc();
			if truncated >= 0.0 {
				let uint = Uint::from_u64(truncated as u64);
				out.push::<Uint>(uint);
			} else {
				out.push_undefined();
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

// Float to Decimal conversion
fn f32_to_decimal_vec(container: &NumberContainer<f32>, target: Type) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(target, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			// Convert float to decimal with default precision/scale
			let decimal = Decimal::from_i64(val.trunc() as i64);
			out.push::<Decimal>(decimal);
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

fn f64_to_decimal_vec(container: &NumberContainer<f64>, target: Type) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(target, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			// Convert float to decimal with default precision/scale
			let decimal = Decimal::from_i64(val.trunc() as i64);
			out.push::<Decimal>(decimal);
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

fn number_to_number(
	data: &ColumnData,
	target: Type,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
) -> crate::Result<ColumnData> {
	if !target.is_number() {
		return_error!(cast::unsupported_cast(lazy_fragment.fragment(), data.get_type(), target,));
	}

	macro_rules! cast {
            (
                $src_variant:ident, $src_ty:ty,
                to => [ $( ($dst_variant:ident, $dst_ty:ty) ),* ]
                $(, to_struct => [ $( ($struct_variant:ident, $struct_ty:ty) ),* ])?
            ) => {
            if let ColumnData::$src_variant(container) = data {
                    match target {
                        $(
                        Type::$dst_variant => return convert_vec::<$src_ty, $dst_ty>(
                            &container,
                                ctx,
                                lazy_fragment,
                                Type::$dst_variant,
                                ColumnData::push::<$dst_ty>,
                            ),
                        )*
                        $($(
                        Type::$struct_variant { .. } => return convert_vec::<$src_ty, $struct_ty>(
                            &container,
                                ctx,
                                lazy_fragment,
                                target,
                                ColumnData::push::<$struct_ty>,
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

	// Special handling for Int (uses Clone instead of Copy)
	if let ColumnData::Int {
		container,
		..
	} = data
	{
		match target {
			Type::Int1 => {
				return convert_vec_clone::<Int, i8>(
					container,
					ctx,
					lazy_fragment,
					Type::Int1,
					ColumnData::push::<i8>,
				);
			}
			Type::Int2 => {
				return convert_vec_clone::<Int, i16>(
					container,
					ctx,
					lazy_fragment,
					Type::Int2,
					ColumnData::push::<i16>,
				);
			}
			Type::Int4 => {
				return convert_vec_clone::<Int, i32>(
					container,
					ctx,
					lazy_fragment,
					Type::Int4,
					ColumnData::push::<i32>,
				);
			}
			Type::Int8 => {
				return convert_vec_clone::<Int, i64>(
					container,
					ctx,
					lazy_fragment,
					Type::Int8,
					ColumnData::push::<i64>,
				);
			}
			Type::Int16 => {
				return convert_vec_clone::<Int, i128>(
					container,
					ctx,
					lazy_fragment,
					Type::Int16,
					ColumnData::push::<i128>,
				);
			}
			Type::Uint1 => {
				return convert_vec_clone::<Int, u8>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint1,
					ColumnData::push::<u8>,
				);
			}
			Type::Uint2 => {
				return convert_vec_clone::<Int, u16>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint2,
					ColumnData::push::<u16>,
				);
			}
			Type::Uint4 => {
				return convert_vec_clone::<Int, u32>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint4,
					ColumnData::push::<u32>,
				);
			}
			Type::Uint8 => {
				return convert_vec_clone::<Int, u64>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint8,
					ColumnData::push::<u64>,
				);
			}
			Type::Uint16 => {
				return convert_vec_clone::<Int, u128>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint16,
					ColumnData::push::<u128>,
				);
			}
			Type::Float4 => {
				return convert_vec_clone::<Int, f32>(
					container,
					ctx,
					lazy_fragment,
					Type::Float4,
					ColumnData::push::<f32>,
				);
			}
			Type::Float8 => {
				return convert_vec_clone::<Int, f64>(
					container,
					ctx,
					lazy_fragment,
					Type::Float8,
					ColumnData::push::<f64>,
				);
			}
			Type::Uint => {
				return convert_vec_clone::<Int, Uint>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint,
					ColumnData::push::<Uint>,
				);
			}
			Type::Decimal {
				..
			} => {
				return convert_vec_clone::<Int, Decimal>(
					container,
					ctx,
					lazy_fragment,
					target,
					ColumnData::push::<Decimal>,
				);
			}
			_ => {}
		}
	}

	// Special handling for Uint (uses Clone instead of Copy)
	if let ColumnData::Uint {
		container,
		..
	} = data
	{
		match target {
			Type::Uint1 => {
				return convert_vec_clone::<Uint, u8>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint1,
					ColumnData::push::<u8>,
				);
			}
			Type::Uint2 => {
				return convert_vec_clone::<Uint, u16>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint2,
					ColumnData::push::<u16>,
				);
			}
			Type::Uint4 => {
				return convert_vec_clone::<Uint, u32>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint4,
					ColumnData::push::<u32>,
				);
			}
			Type::Uint8 => {
				return convert_vec_clone::<Uint, u64>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint8,
					ColumnData::push::<u64>,
				);
			}
			Type::Uint16 => {
				return convert_vec_clone::<Uint, u128>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint16,
					ColumnData::push::<u128>,
				);
			}
			Type::Int1 => {
				return convert_vec_clone::<Uint, i8>(
					container,
					ctx,
					lazy_fragment,
					Type::Int1,
					ColumnData::push::<i8>,
				);
			}
			Type::Int2 => {
				return convert_vec_clone::<Uint, i16>(
					container,
					ctx,
					lazy_fragment,
					Type::Int2,
					ColumnData::push::<i16>,
				);
			}
			Type::Int4 => {
				return convert_vec_clone::<Uint, i32>(
					container,
					ctx,
					lazy_fragment,
					Type::Int4,
					ColumnData::push::<i32>,
				);
			}
			Type::Int8 => {
				return convert_vec_clone::<Uint, i64>(
					container,
					ctx,
					lazy_fragment,
					Type::Int8,
					ColumnData::push::<i64>,
				);
			}
			Type::Int16 => {
				return convert_vec_clone::<Uint, i128>(
					container,
					ctx,
					lazy_fragment,
					Type::Int16,
					ColumnData::push::<i128>,
				);
			}
			Type::Float4 => {
				return convert_vec_clone::<Uint, f32>(
					container,
					ctx,
					lazy_fragment,
					Type::Float4,
					ColumnData::push::<f32>,
				);
			}
			Type::Float8 => {
				return convert_vec_clone::<Uint, f64>(
					container,
					ctx,
					lazy_fragment,
					Type::Float8,
					ColumnData::push::<f64>,
				);
			}
			Type::Int => {
				return convert_vec_clone::<Uint, Int>(
					container,
					ctx,
					lazy_fragment,
					Type::Int,
					ColumnData::push::<Int>,
				);
			}
			Type::Decimal {
				..
			} => {
				return convert_vec_clone::<Uint, Decimal>(
					container,
					ctx,
					lazy_fragment,
					target,
					ColumnData::push::<Decimal>,
				);
			}
			_ => {}
		}
	}

	// Special handling for Decimal source (which is a struct variant in
	// ColumnData)
	if let ColumnData::Decimal {
		container,
		..
	} = data
	{
		match target {
			Type::Int1 => {
				return convert_vec_clone::<Decimal, i8>(
					container,
					ctx,
					lazy_fragment,
					Type::Int1,
					ColumnData::push::<i8>,
				);
			}
			Type::Int2 => {
				return convert_vec_clone::<Decimal, i16>(
					container,
					ctx,
					lazy_fragment,
					Type::Int2,
					ColumnData::push::<i16>,
				);
			}
			Type::Int4 => {
				return convert_vec_clone::<Decimal, i32>(
					container,
					ctx,
					lazy_fragment,
					Type::Int4,
					ColumnData::push::<i32>,
				);
			}
			Type::Int8 => {
				return convert_vec_clone::<Decimal, i64>(
					container,
					ctx,
					lazy_fragment,
					Type::Int8,
					ColumnData::push::<i64>,
				);
			}
			Type::Int16 => {
				return convert_vec_clone::<Decimal, i128>(
					container,
					ctx,
					lazy_fragment,
					Type::Int16,
					ColumnData::push::<i128>,
				);
			}
			Type::Uint1 => {
				return convert_vec_clone::<Decimal, u8>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint1,
					ColumnData::push::<u8>,
				);
			}
			Type::Uint2 => {
				return convert_vec_clone::<Decimal, u16>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint2,
					ColumnData::push::<u16>,
				);
			}
			Type::Uint4 => {
				return convert_vec_clone::<Decimal, u32>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint4,
					ColumnData::push::<u32>,
				);
			}
			Type::Uint8 => {
				return convert_vec_clone::<Decimal, u64>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint8,
					ColumnData::push::<u64>,
				);
			}
			Type::Uint16 => {
				return convert_vec_clone::<Decimal, u128>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint16,
					ColumnData::push::<u128>,
				);
			}
			Type::Float4 => {
				return convert_vec_clone::<Decimal, f32>(
					container,
					ctx,
					lazy_fragment,
					Type::Float4,
					ColumnData::push::<f32>,
				);
			}
			Type::Float8 => {
				return convert_vec_clone::<Decimal, f64>(
					container,
					ctx,
					lazy_fragment,
					Type::Float8,
					ColumnData::push::<f64>,
				);
			}
			Type::Int => {
				return convert_vec_clone::<Decimal, Int>(
					container,
					ctx,
					lazy_fragment,
					Type::Int,
					ColumnData::push::<Int>,
				);
			}
			Type::Uint => {
				return convert_vec_clone::<Decimal, Uint>(
					container,
					ctx,
					lazy_fragment,
					Type::Uint,
					ColumnData::push::<Uint>,
				);
			}
			Type::Decimal {
				..
			} => {
				return convert_vec_clone::<Decimal, Decimal>(
					container,
					ctx,
					lazy_fragment,
					target,
					ColumnData::push::<Decimal>,
				);
			}
			_ => {}
		}
	}

	let source_type = data.get_type();
	return_error!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
}

pub(crate) fn convert_vec<'a, From, To>(
	container: &NumberContainer<From>,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
	target_kind: Type,
	mut push: impl FnMut(&mut ColumnData, To),
) -> crate::Result<ColumnData>
where
	From: Copy + SafeConvert<To> + GetType + IsNumber + Default,
	To: GetType,
{
	let mut out = ColumnData::with_capacity(target_kind, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			let fragment = lazy_fragment.fragment();
			match ctx.convert::<From, To>(val, fragment)? {
				Some(v) => push(&mut out, v),
				None => out.push_undefined(),
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

pub(crate) fn convert_vec_clone<'a, From, To>(
	container: &NumberContainer<From>,
	ctx: impl Convert,
	lazy_fragment: impl LazyFragment,
	target_kind: Type,
	mut push: impl FnMut(&mut ColumnData, To),
) -> crate::Result<ColumnData>
where
	From: Clone + SafeConvert<To> + GetType + IsNumber + Default,
	To: GetType,
{
	let mut out = ColumnData::with_capacity(target_kind, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx].clone();
			let fragment = lazy_fragment.fragment();
			match ctx.convert::<From, To>(val, fragment)? {
				Some(v) => push(&mut out, v),
				None => out.push_undefined(),
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[cfg(test)]
pub mod tests {
	mod convert {
		use reifydb_type::{
			fragment::Fragment,
			util::bitvec::BitVec,
			value::{
				container::number::NumberContainer,
				number::safe::convert::SafeConvert,
				r#type::{Type, get::GetType},
			},
		};

		use crate::expression::{cast::number::convert_vec, convert::Convert};

		#[test]
		fn test_promote_ok() {
			let data = [1i8, 2i8];
			let bitvec = BitVec::from_slice(&[true, true]);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i8, i16>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int2,
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
			let bitvec = BitVec::from_slice(&[true]);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i8, i16>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_promote_invalid_bitmaps_are_undefined() {
			let data = [1i8];
			let bitvec = BitVec::from_slice(&[false]);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i8, i16>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_promote_mixed_bitvec_and_failure() {
			let data = [1i8, 42i8, 3i8, 4i8];
			let bitvec = BitVec::from_slice(&[true, true, false, true]);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i8, i16>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			let slice = result.as_slice::<i16>();
			assert_eq!(slice, &[1i16, 0, 0, 4i16]);
			assert!(result.is_defined(0));
			assert!(!result.is_defined(1));
			assert!(!result.is_defined(2));
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
			fn convert<From, To>(
				&self,
				val: From,
				_fragment: impl Into<reifydb_type::fragment::Fragment>,
			) -> crate::Result<Option<To>>
			where
				From: SafeConvert<To> + GetType,
				To: GetType,
			{
				// Only simulate conversion failure for i8 == 42
				// or i16 == 42
				if std::mem::size_of::<From>() == 1 {
					let raw: i8 = unsafe { std::mem::transmute_copy(&val) };
					if raw == 42 {
						return Ok(None);
					}
				} else if std::mem::size_of::<From>() == 2 {
					let raw: i16 = unsafe { std::mem::transmute_copy(&val) };
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
			let bitvec = BitVec::from_slice(&[true, true]);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i16, i8>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int1,
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
			let bitvec = BitVec::from_slice(&[true]);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i16, i8>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_demote_invalid_bitmaps_are_undefined() {
			let data = [1i16];
			let bitvec = BitVec::repeat(1, false);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i16, i8>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_demote_mixed_bitvec_and_failure() {
			let data = [1i16, 42i16, 3i16, 4i16];
			let bitvec = BitVec::from_slice(&[true, true, false, true]);
			let ctx = TestCtx::new();

			let container = NumberContainer::new(data.to_vec(), bitvec);
			let result = convert_vec::<i16, i8>(
				&container,
				&ctx,
				|| Fragment::testing_empty(),
				Type::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			let slice: &[i8] = result.as_slice();
			assert_eq!(slice, &[1i8, 0, 0, 4i8]);
			assert!(result.is_defined(0));
			assert!(!result.is_defined(1));
			assert!(!result.is_defined(2));
			assert!(result.is_defined(3));
		}
	}
}
