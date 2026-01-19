// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cast to numeric types

use reifydb_core::value::column::{data::ColumnData, push::Push};
use reifydb_type::{
	fragment::Fragment,
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

use crate::expression::types::{EvalError, EvalResult};

pub(super) fn to_number(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	if !target.is_number() {
		let source_type = data.get_type();
		return Err(EvalError::UnsupportedCast {
			from: format!("{:?}", source_type),
			to: format!("{:?}", target),
		});
	}

	if data.get_type().is_number() {
		return number_to_number(data, target);
	}

	if data.is_bool() {
		return boolean_to_number(data, target);
	}

	if data.is_utf8() {
		return match target {
			Type::Float4 | Type::Float8 => text_to_float(data, target),
			Type::Decimal { .. } => text_to_decimal(data, target),
			_ => text_to_integer(data, target),
		};
	}

	if data.is_float() {
		return float_to_integer(data, target);
	}

	let source_type = data.get_type();
	Err(EvalError::UnsupportedCast {
		from: format!("{:?}", source_type),
		to: format!("{:?}", target),
	})
}

fn boolean_to_number(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	macro_rules! boolean_to_number {
		($target_ty:ty, $true_val:expr, $false_val:expr) => {{
			|out: &mut ColumnData, val: bool| {
				out.push::<$target_ty>(if val { $true_val } else { $false_val })
			}
		}};
	}

	match data {
		ColumnData::Bool(container) => {
			let converter = match target {
				Type::Int1 => boolean_to_number!(i8, 1i8, 0i8),
				Type::Int2 => boolean_to_number!(i16, 1i16, 0i16),
				Type::Int4 => boolean_to_number!(i32, 1i32, 0i32),
				Type::Int8 => boolean_to_number!(i64, 1i64, 0i64),
				Type::Int16 => boolean_to_number!(i128, 1i128, 0i128),
				Type::Uint1 => boolean_to_number!(u8, 1u8, 0u8),
				Type::Uint2 => boolean_to_number!(u16, 1u16, 0u16),
				Type::Uint4 => boolean_to_number!(u32, 1u32, 0u32),
				Type::Uint8 => boolean_to_number!(u64, 1u64, 0u64),
				Type::Uint16 => boolean_to_number!(u128, 1u128, 0u128),
				Type::Float4 => boolean_to_number!(f32, 1.0f32, 0.0f32),
				Type::Float8 => boolean_to_number!(f64, 1.0f64, 0.0f64),
				Type::Int => |out: &mut ColumnData, val: bool| {
					out.push::<Int>(if val { Int::from_i64(1) } else { Int::from_i64(0) })
				},
				Type::Uint => |out: &mut ColumnData, val: bool| {
					out.push::<Uint>(if val { Uint::from_u64(1) } else { Uint::from_u64(0) })
				},
				Type::Decimal { .. } => |out: &mut ColumnData, val: bool| {
					let decimal = if val { Decimal::from_i64(1) } else { Decimal::from_i64(0) };
					out.push::<Decimal>(decimal)
				},
				_ => {
					let source_type = data.get_type();
					return Err(EvalError::UnsupportedCast {
						from: format!("{:?}", source_type),
						to: format!("{:?}", target),
					});
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
			Err(EvalError::UnsupportedCast {
				from: format!("{:?}", source_type),
				to: format!("{:?}", target),
			})
		}
	}
}

macro_rules! float_to_int_vec {
	($fn_name:ident, $float_ty:ty, $int_ty:ty, $target_type:expr, $min_val:expr, $max_val:expr) => {
		fn $fn_name(container: &NumberContainer<$float_ty>) -> EvalResult<ColumnData>
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

fn float_to_integer(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
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
			_ => {
				let source_type = data.get_type();
				Err(EvalError::UnsupportedCast {
					from: format!("{:?}", source_type),
					to: format!("{:?}", target),
				})
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
			_ => {
				let source_type = data.get_type();
				Err(EvalError::UnsupportedCast {
					from: format!("{:?}", source_type),
					to: format!("{:?}", target),
				})
			}
		},
		_ => {
			let source_type = data.get_type();
			Err(EvalError::UnsupportedCast {
				from: format!("{:?}", source_type),
				to: format!("{:?}", target),
			})
		}
	}
}

fn text_to_integer(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	match data {
		ColumnData::Utf8 { container, .. } => {
			let mut out = ColumnData::with_capacity(target, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = &container[idx];
					let temp_fragment = Fragment::internal(val);

					match target {
						Type::Int1 => {
							let result = parse_primitive_int::<i8>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Int1", val),
								}
							})?;
							out.push::<i8>(result);
						}
						Type::Int2 => {
							let result = parse_primitive_int::<i16>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Int2", val),
								}
							})?;
							out.push::<i16>(result);
						}
						Type::Int4 => {
							let result = parse_primitive_int::<i32>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Int4", val),
								}
							})?;
							out.push::<i32>(result);
						}
						Type::Int8 => {
							let result = parse_primitive_int::<i64>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Int8", val),
								}
							})?;
							out.push::<i64>(result);
						}
						Type::Int16 => {
							let result = parse_primitive_int::<i128>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Int16", val),
								}
							})?;
							out.push::<i128>(result);
						}
						Type::Uint1 => {
							let result = parse_primitive_uint::<u8>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Uint1", val),
								}
							})?;
							out.push::<u8>(result);
						}
						Type::Uint2 => {
							let result = parse_primitive_uint::<u16>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Uint2", val),
								}
							})?;
							out.push::<u16>(result);
						}
						Type::Uint4 => {
							let result = parse_primitive_uint::<u32>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Uint4", val),
								}
							})?;
							out.push::<u32>(result);
						}
						Type::Uint8 => {
							let result = parse_primitive_uint::<u64>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Uint8", val),
								}
							})?;
							out.push::<u64>(result);
						}
						Type::Uint16 => {
							let result = parse_primitive_uint::<u128>(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Uint16", val),
								}
							})?;
							out.push::<u128>(result);
						}
						Type::Int => {
							let result = parse_primitive_int(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Int", val),
								}
							})?;
							out.push::<Int>(result);
						}
						Type::Uint => {
							let result = parse_primitive_uint(temp_fragment.clone()).map_err(|_e| {
								EvalError::InvalidCast {
									details: format!("Cannot parse '{}' as Uint", val),
								}
							})?;
							out.push::<Uint>(result);
						}
						_ => {
							let source_type = data.get_type();
							return Err(EvalError::UnsupportedCast {
								from: format!("{:?}", source_type),
								to: format!("{:?}", target),
							});
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
			Err(EvalError::UnsupportedCast {
				from: format!("{:?}", source_type),
				to: format!("{:?}", target),
			})
		}
	}
}

fn text_to_float(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	if let ColumnData::Utf8 { container, .. } = data {
		let mut out = ColumnData::with_capacity(target, container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = &container[idx];
				let temp_fragment = Fragment::internal(val);

				match target {
					Type::Float4 => {
						let result = parse_float::<f32>(temp_fragment.clone()).map_err(|_e| {
							EvalError::InvalidCast {
								details: format!("Cannot parse '{}' as Float4", val),
							}
						})?;
						out.push::<f32>(result);
					}
					Type::Float8 => {
						let result = parse_float::<f64>(temp_fragment.clone()).map_err(|_e| {
							EvalError::InvalidCast {
								details: format!("Cannot parse '{}' as Float8", val),
							}
						})?;
						out.push::<f64>(result);
					}
					_ => {
						let source_type = data.get_type();
						return Err(EvalError::UnsupportedCast {
							from: format!("{:?}", source_type),
							to: format!("{:?}", target),
						});
					}
				}
			} else {
				out.push_undefined();
			}
		}
		Ok(out)
	} else {
		let source_type = data.get_type();
		Err(EvalError::UnsupportedCast {
			from: format!("{:?}", source_type),
			to: format!("{:?}", target),
		})
	}
}

fn text_to_decimal(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	if let ColumnData::Utf8 { container, .. } = data {
		let mut out = ColumnData::with_capacity(target, container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = &container[idx];
				let temp_fragment = Fragment::internal(val);

				let result = parse_decimal(temp_fragment.clone()).map_err(|_e| EvalError::InvalidCast {
					details: format!("Cannot parse '{}' as Decimal", val),
				})?;
				out.push::<Decimal>(result);
			} else {
				out.push_undefined();
			}
		}
		Ok(out)
	} else {
		let source_type = data.get_type();
		Err(EvalError::UnsupportedCast {
			from: format!("{:?}", source_type),
			to: format!("{:?}", target),
		})
	}
}

// Number to number conversion using SafeConvert
fn number_to_number(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	if !target.is_number() {
		return Err(EvalError::UnsupportedCast {
			from: format!("{:?}", data.get_type()),
			to: format!("{:?}", target),
		});
	}

	// Helper function for Copy types
	fn convert_vec<From, To>(container: &NumberContainer<From>, target_kind: Type) -> EvalResult<ColumnData>
	where
		From: Copy + SafeConvert<To> + GetType + IsNumber + Default,
		To: GetType + std::fmt::Debug,
		ColumnData: Push<To>,
	{
		let mut out = ColumnData::with_capacity(target_kind, container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = container[idx];
				// Use checked_convert - returns None on overflow
				match val.checked_convert() {
					Some(v) => out.push(v),
					None => out.push_undefined(), // Overflow -> undefined
				}
			} else {
				out.push_undefined();
			}
		}
		Ok(out)
	}

	// Helper function for Clone types
	fn convert_vec_clone<From, To>(container: &NumberContainer<From>, target_kind: Type) -> EvalResult<ColumnData>
	where
		From: Clone + SafeConvert<To> + GetType + IsNumber + Default,
		To: GetType + std::fmt::Debug,
		ColumnData: Push<To>,
	{
		let mut out = ColumnData::with_capacity(target_kind, container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = container[idx].clone();
				match val.checked_convert() {
					Some(v) => out.push(v),
					None => out.push_undefined(),
				}
			} else {
				out.push_undefined();
			}
		}
		Ok(out)
	}

	// Macro to generate all conversion paths for a source type
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
						Type::$dst_variant,
					),
					)*
					$($(
					Type::$struct_variant { .. } => return convert_vec::<$src_ty, $struct_ty>(
						&container,
						target,
					),
					)*)?
					_ => {}
				}
			}
		}
	}

	cast!(Float4, f32,
		to => [(Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Float8, f64,
		to => [(Float4, f32), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Int1, i8,
		to => [(Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Int2, i16,
		to => [(Int1, i8), (Int4, i32), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Int4, i32,
		to => [(Int1, i8), (Int2, i16), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Int8, i64,
		to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Int16, i128,
		to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Uint1, u8,
		to => [(Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Uint2, u16,
		to => [(Uint1, u8), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Uint4, u32,
		to => [(Uint1, u8), (Uint2, u16), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Uint8, u64,
		to => [(Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	cast!(Uint16, u128,
		to => [(Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
		to_struct => [(Int, Int), (Uint, Uint), (Decimal, Decimal)]
	);

	// Handle arbitrary precision types (Int, Uint, Decimal) - use Clone versions
	macro_rules! cast_clone {
		(
			$src_variant:ident, $src_ty:ty,
			to => [ $( ($dst_variant:ident, $dst_ty:ty) ),* ]
			$(, to_struct => [ $( ($struct_variant:ident, $struct_ty:ty) ),* ])?
		) => {
			if let ColumnData::$src_variant { container, .. } = data {
				match target {
					$(
					Type::$dst_variant => return convert_vec_clone::<$src_ty, $dst_ty>(
						&container,
						Type::$dst_variant,
					),
					)*
					$($(
					Type::$struct_variant { .. } => return convert_vec_clone::<$src_ty, $struct_ty>(
						&container,
						target,
					),
					)*)?
					_ => {}
				}
			}
		}
	}

	cast_clone!(Int, Int,
		to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
		to_struct => [(Uint, Uint), (Decimal, Decimal)]
	);

	cast_clone!(Uint, Uint,
		to => [(Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
		to_struct => [(Int, Int), (Decimal, Decimal)]
	);

	cast_clone!(Decimal, Decimal,
		to => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128), (Float4, f32), (Float8, f64)],
		to_struct => [(Int, Int), (Uint, Uint)]
	);

	Err(EvalError::UnsupportedCast {
		from: format!("{:?}", data.get_type()),
		to: format!("{:?}", target),
	})
}
