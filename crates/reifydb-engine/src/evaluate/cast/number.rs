// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::fmt::Debug;

use reifydb_core::{
	GetType, OwnedSpan, Type, error,
	interface::fragment::Fragment,
	result::error::diagnostic::cast,
	return_error,
	value::{
		IsNumber,
		container::NumberContainer,
		number::{
			SafeConvert, SafeDemote, SafePromote, parse_float,
			parse_int, parse_uint,
		},
	},
};

use crate::{
	columnar::ColumnData,
	evaluate::{Convert, Demote, Promote},
};

pub fn to_number(
	ctx: impl Promote + Demote + Convert,
	data: &ColumnData,
	target: Type,
	span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
	if !target.is_number() {
		let source_type = data.get_type();
		return_error!(cast::unsupported_cast(
			span(),
			source_type,
			target
		));
	}

	if data.get_type().is_number() {
		return number_to_number(data, target, ctx, span);
	}

	if data.is_bool() {
		return bool_to_number(data, target, span);
	}

	if data.is_utf8() {
		return match target {
			Type::Float4 | Type::Float8 => {
				text_to_float(data, target, span)
			}
			_ => text_to_integer(data, target, span),
		};
	}

	if data.is_float() {
		return float_to_integer(data, target, span);
	}

	let source_type = data.get_type();
	return_error!(cast::unsupported_cast(span(), source_type, target))
}

fn bool_to_number(
	data: &ColumnData,
	target: Type,
	span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
	macro_rules! bool_to_number {
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
				Type::Int1 => bool_to_number!(i8, 1i8, 0i8),
				Type::Int2 => bool_to_number!(i16, 1i16, 0i16),
				Type::Int4 => bool_to_number!(i32, 1i32, 0i32),
				Type::Int8 => bool_to_number!(i64, 1i64, 0i64),
				Type::Int16 => {
					bool_to_number!(i128, 1i128, 0i128)
				}
				Type::Uint1 => bool_to_number!(u8, 1u8, 0u8),
				Type::Uint2 => bool_to_number!(u16, 1u16, 0u16),
				Type::Uint4 => bool_to_number!(u32, 1u32, 0u32),
				Type::Uint8 => bool_to_number!(u64, 1u64, 0u64),
				Type::Uint16 => {
					bool_to_number!(u128, 1u128, 0u128)
				}
				Type::Float4 => {
					bool_to_number!(f32, 1.0f32, 0.0f32)
				}
				Type::Float8 => {
					bool_to_number!(f64, 1.0f64, 0.0f64)
				}
				_ => {
					let source_type = data.get_type();
					return_error!(cast::unsupported_cast(
						span(),
						source_type,
						target
					));
				}
			};

			let mut out = ColumnData::with_capacity(
				target,
				container.len(),
			);
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
			return_error!(cast::unsupported_cast(
				span(),
				source_type,
				target
			))
		}
	}
}

fn float_to_integer(
	data: &ColumnData,
	target: Type,
	span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
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
				return_error!(cast::unsupported_cast(
					span(),
					source_type,
					target
				))
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
				return_error!(cast::unsupported_cast(
					span(),
					source_type,
					target
				))
			}
		},
		_ => {
			let source_type = data.get_type();
			return_error!(cast::unsupported_cast(
				span(),
				source_type,
				target
			))
		}
	}
}

macro_rules! parse_and_push {
	(parse_int, $ty:ty, $target_type:expr, $out:expr, $temp_fragment:expr, $base_span:expr) => {{
		let result = parse_int::<$ty>($temp_fragment.clone()).map_err(|mut e| {
			use reifydb_core::interface::fragment::{OwnedFragment, StatementLine, StatementColumn};
			let value_with_position = OwnedFragment::Statement {
				text: $temp_fragment.value().to_string(),
				line: StatementLine($base_span.line.0),
				column: StatementColumn($base_span.column.0),
			};
			e.0.with_fragment(value_with_position.clone());
			
			error!(cast::invalid_number(
				value_with_position,
				$target_type,
				e.diagnostic(),
			))
		})?;
		$out.push::<$ty>(result);
	}};
	(parse_uint, $ty:ty, $target_type:expr, $out:expr, $temp_fragment:expr, $base_span:expr) => {{
		let result = parse_uint::<$ty>($temp_fragment.clone()).map_err(|mut e| {
			use reifydb_core::interface::fragment::{OwnedFragment, StatementLine, StatementColumn};
			let value_with_position = OwnedFragment::Statement {
				text: $temp_fragment.value().to_string(),
				line: StatementLine($base_span.line.0),
				column: StatementColumn($base_span.column.0),
			};
			e.0.with_fragment(value_with_position.clone());
			
			error!(cast::invalid_number(
				value_with_position,
				$target_type,
				e.diagnostic(),
			))
		})?;
		$out.push::<$ty>(result);
	}};
}

fn text_to_integer(
	data: &ColumnData,
	target: Type,
	span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Utf8(container) => {
			let base_span = span();
			let mut out = ColumnData::with_capacity(
				target,
				container.len(),
			);
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = &container[idx];
					use reifydb_core::interface::fragment::BorrowedFragment;
					let temp_fragment = BorrowedFragment::new_internal(val);

					match target {
						Type::Int1 => {
							parse_and_push!(
								parse_int,
								i8,
								Type::Int1,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Int2 => {
							parse_and_push!(
								parse_int,
								i16,
								Type::Int2,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Int4 => {
							parse_and_push!(
								parse_int,
								i32,
								Type::Int4,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Int8 => {
							parse_and_push!(
								parse_int,
								i64,
								Type::Int8,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Int16 => {
							parse_and_push!(
								parse_int,
								i128,
								Type::Int16,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Uint1 => {
							parse_and_push!(
								parse_uint,
								u8,
								Type::Uint1,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Uint2 => {
							parse_and_push!(
								parse_uint,
								u16,
								Type::Uint2,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Uint4 => {
							parse_and_push!(
								parse_uint,
								u32,
								Type::Uint4,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Uint8 => {
							parse_and_push!(
								parse_uint,
								u64,
								Type::Uint8,
								out,
								temp_fragment,
								base_span
							)
						}
						Type::Uint16 => {
							parse_and_push!(
								parse_uint,
								u128,
								Type::Uint16,
								out,
								temp_fragment,
								base_span
							)
						}
						_ => {
							let source_type =
								data.get_type();
							return_error!(cast::unsupported_cast(
                                base_span.clone(),
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
			return_error!(cast::unsupported_cast(
				span(),
				source_type,
				target
			))
		}
	}
}

fn text_to_float(
	column_data: &ColumnData,
	target: Type,
	span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
	if let ColumnData::Utf8(container) = column_data {
		// Create base span once for efficiency
		let base_span = span();
		let mut out =
			ColumnData::with_capacity(target, container.len());
		for idx in 0..container.len() {
			if container.is_defined(idx) {
				let val = &container[idx];
				// Create efficient borrowed fragment for parsing
				use reifydb_core::interface::fragment::BorrowedFragment;
				let temp_fragment = BorrowedFragment::new_internal(val);

				match target {
					Type::Float4 => out.push::<f32>(
						parse_float::<f32>(temp_fragment.clone())
							.map_err(|mut e| {
							use reifydb_core::interface::fragment::{OwnedFragment, StatementLine, StatementColumn};
							let value_with_position = OwnedFragment::Statement {
								text: val.to_string(),
								line: StatementLine(base_span.line.0),
								column: StatementColumn(base_span.column.0),
							};
							e.0.with_fragment(value_with_position.clone());
							
							error!(cast::invalid_number(
                                value_with_position,
                                Type::Float4,
                                e.diagnostic(),
                            ))
						})?,
					),

					Type::Float8 => out.push::<f64>(
						parse_float::<f64>(temp_fragment)
							.map_err(|mut e| {
							use reifydb_core::interface::fragment::{OwnedFragment, StatementLine, StatementColumn};
							let value_with_position = OwnedFragment::Statement {
								text: val.to_string(),
								line: StatementLine(base_span.line.0),
								column: StatementColumn(base_span.column.0),
							};
							e.0.with_fragment(value_with_position.clone());
							
							error!(cast::invalid_number(
                                value_with_position,
                                Type::Float8,
                                e.diagnostic(),
                            ))
						})?,
					),
					_ => {
						let source_type =
							column_data.get_type();
						return_error!(
							cast::unsupported_cast(
								base_span
									.clone(
									),
								source_type,
								target
							)
						);
					}
				}
			} else {
				out.push_undefined();
			}
		}
		Ok(out)
	} else {
		let source_type = column_data.get_type();
		return_error!(cast::unsupported_cast(
			span(),
			source_type,
			target
		))
	}
}

macro_rules! float_to_int_vec {
	($fn_name:ident, $float_ty:ty, $int_ty:ty, $target_type:expr, $min_val:expr, $max_val:expr) => {
		fn $fn_name(
			container: &NumberContainer<$float_ty>,
		) -> crate::Result<ColumnData>
		where
			$float_ty: Clone + Debug + Default + IsNumber,
		{
			let mut out = ColumnData::with_capacity(
				$target_type,
				container.len(),
			);
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container[idx];
					let truncated = val.trunc();
					if truncated >= $min_val
						&& truncated <= $max_val
					{
						out.push::<$int_ty>(
							truncated as $int_ty,
						);
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

float_to_int_vec!(
	f32_to_i8_vec,
	f32,
	i8,
	Type::Int1,
	i8::MIN as f32,
	i8::MAX as f32
);
float_to_int_vec!(
	f32_to_i16_vec,
	f32,
	i16,
	Type::Int2,
	i16::MIN as f32,
	i16::MAX as f32
);
float_to_int_vec!(
	f32_to_i32_vec,
	f32,
	i32,
	Type::Int4,
	i32::MIN as f32,
	i32::MAX as f32
);
float_to_int_vec!(
	f32_to_i64_vec,
	f32,
	i64,
	Type::Int8,
	i64::MIN as f32,
	i64::MAX as f32
);
float_to_int_vec!(
	f32_to_i128_vec,
	f32,
	i128,
	Type::Int16,
	i128::MIN as f32,
	i128::MAX as f32
);
float_to_int_vec!(f32_to_u8_vec, f32, u8, Type::Uint1, 0.0, u8::MAX as f32);
float_to_int_vec!(f32_to_u16_vec, f32, u16, Type::Uint2, 0.0, u16::MAX as f32);
float_to_int_vec!(f32_to_u32_vec, f32, u32, Type::Uint4, 0.0, u32::MAX as f32);
float_to_int_vec!(f32_to_u64_vec, f32, u64, Type::Uint8, 0.0, u64::MAX as f32);
float_to_int_vec!(
	f32_to_u128_vec,
	f32,
	u128,
	Type::Uint16,
	0.0,
	u128::MAX as f32
);

float_to_int_vec!(
	f64_to_i8_vec,
	f64,
	i8,
	Type::Int1,
	i8::MIN as f64,
	i8::MAX as f64
);
float_to_int_vec!(
	f64_to_i16_vec,
	f64,
	i16,
	Type::Int2,
	i16::MIN as f64,
	i16::MAX as f64
);
float_to_int_vec!(
	f64_to_i32_vec,
	f64,
	i32,
	Type::Int4,
	i32::MIN as f64,
	i32::MAX as f64
);
float_to_int_vec!(
	f64_to_i64_vec,
	f64,
	i64,
	Type::Int8,
	i64::MIN as f64,
	i64::MAX as f64
);
float_to_int_vec!(
	f64_to_i128_vec,
	f64,
	i128,
	Type::Int16,
	i128::MIN as f64,
	i128::MAX as f64
);
float_to_int_vec!(f64_to_u8_vec, f64, u8, Type::Uint1, 0.0, u8::MAX as f64);
float_to_int_vec!(f64_to_u16_vec, f64, u16, Type::Uint2, 0.0, u16::MAX as f64);
float_to_int_vec!(f64_to_u32_vec, f64, u32, Type::Uint4, 0.0, u32::MAX as f64);
float_to_int_vec!(f64_to_u64_vec, f64, u64, Type::Uint8, 0.0, u64::MAX as f64);
float_to_int_vec!(
	f64_to_u128_vec,
	f64,
	u128,
	Type::Uint16,
	0.0,
	u128::MAX as f64
);

fn number_to_number(
	data: &ColumnData,
	target: Type,
	ctx: impl Promote + Demote + Convert,
	span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
	if !target.is_number() {
		return_error!(cast::unsupported_cast(
			span(),
			data.get_type(),
			target,
		));
	}

	macro_rules! cast {
            (
                $src_variant:ident, $src_ty:ty,
                promote => [ $( ($pro_variant:ident, $pro_ty:ty) ),* ],
                demote => [ $( ($dem_variant:ident, $dem_ty:ty) ),* ],
                convert => [ $( ($con_variant:ident, $con_ty:ty) ),* ]
            ) => {
            if let ColumnData::$src_variant(container) = data {
                    match target {
                        $(
                        Type::$pro_variant => return promote_vec::<$src_ty, $pro_ty>(
                            container,
                                ctx,
                                &span,
                                Type::$pro_variant,
                                ColumnData::push::<$pro_ty>,
                            ),
                        )*
                        $(
                        Type::$dem_variant => return demote_vec::<$src_ty, $dem_ty>(
                            container,
                                    ctx,
                                    &span,
                                    Type::$dem_variant,
                                    ColumnData::push::<$dem_ty>,
                                ),
                        )*
                        $(
                        Type::$con_variant => return convert_vec::<$src_ty, $con_ty>(
                            container,
                                ctx,
                                &span,
                                Type::$con_variant,
                                ColumnData::push::<$con_ty>,
                            ),
                        )*
                        _ => {}
                    }
                }
            }
        }

	cast!(Float4, f32,
	    promote => [(Float8, f64) ],
	    demote => [ ],
	    convert => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
	);

	cast!(Float8, f64,
	    promote => [ ],
	    demote => [(Float4, f32)],
	    convert => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
	);

	cast!(Int1, i8,
	    promote => [(Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	    demote => [],
	    convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
	);

	cast!(Int2, i16,
	    promote => [(Int4, i32), (Int8, i64), (Int16, i128)],
	    demote => [(Int1, i8)],
	    convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
	);

	cast!(Int4, i32,
	    promote => [(Int8, i64), (Int16, i128)],
	    demote => [(Int2, i16), (Int1, i8)],
	    convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
	);

	cast!(Int8, i64,
	    promote => [(Int16, i128)],
	    demote => [(Int4, i32), (Int2, i16), (Int1, i8)],
	    convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
	);

	cast!(Int16, i128,
	    promote => [],
	    demote => [(Int8, i64), (Int4, i32), (Int2, i16), (Int1, i8)],
	    convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
	);

	cast!(Uint1, u8,
	    promote => [(Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    demote => [],
	    convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
	);

	cast!(Uint2, u16,
	    promote => [(Uint4, u32), (Uint8, u64), (Uint16, u128)],
	    demote => [(Uint1, u8)],
	    convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
	);

	cast!(Uint4, u32,
	    promote => [(Uint8, u64), (Uint16, u128)],
	    demote => [(Uint2, u16), (Uint1, u8)],
	    convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
	);

	cast!(Uint8, u64,
	    promote => [(Uint16, u128)],
	    demote => [(Uint4, u32), (Uint2, u16), (Uint1, u8)],
	    convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
	);

	cast!(Uint16, u128,
	    promote => [],
	    demote => [(Uint8, u64), (Uint4, u32), (Uint2, u16), (Uint1, u8)],
	    convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
	);

	let source_type = data.get_type();
	return_error!(cast::unsupported_cast(span(), source_type, target))
}

pub(crate) fn demote_vec<From, To>(
	container: &NumberContainer<From>,
	demote: impl Demote,
	span: impl Fn() -> OwnedSpan,
	target_kind: Type,
	mut push: impl FnMut(&mut ColumnData, To),
) -> crate::Result<ColumnData>
where
	From: Copy + SafeDemote<To> + Clone + Debug + Default + IsNumber,
	To: GetType,
{
	let mut out = ColumnData::with_capacity(target_kind, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			match demote.demote::<From, To>(val, &span)? {
				Some(v) => push(&mut out, v),
				None => out.push_undefined(),
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

pub(crate) fn promote_vec<From, To>(
	container: &NumberContainer<From>,
	ctx: impl Promote,
	span: impl Fn() -> OwnedSpan,
	target_kind: Type,
	mut push: impl FnMut(&mut ColumnData, To),
) -> crate::Result<ColumnData>
where
	From: Copy + SafePromote<To> + Clone + Debug + Default + IsNumber,
	To: GetType,
{
	let mut out = ColumnData::with_capacity(target_kind, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			match ctx.promote::<From, To>(val, &span)? {
				Some(v) => push(&mut out, v),
				None => out.push_undefined(),
			}
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

pub(crate) fn convert_vec<From, To>(
	container: &NumberContainer<From>,
	ctx: impl Convert,
	span: impl Fn() -> OwnedSpan,
	target_kind: Type,
	mut push: impl FnMut(&mut ColumnData, To),
) -> crate::Result<ColumnData>
where
	From: Copy
		+ SafeConvert<To>
		+ GetType
		+ Clone
		+ Debug
		+ Default
		+ IsNumber,
	To: GetType,
{
	let mut out = ColumnData::with_capacity(target_kind, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let val = container[idx];
			match ctx.convert::<From, To>(val, &span)? {
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
mod tests {
	mod promote {
		use reifydb_core::{
			BitVec, IntoOwnedSpan, OwnedSpan, Type,
			value::{
				container::NumberContainer, number::SafePromote,
			},
		};

		use crate::evaluate::{Promote, cast::number::promote_vec};

		#[test]
		fn test_ok() {
			let data = [1i8, 2i8];
			let bitvec = BitVec::from_slice(&[true, true]);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = promote_vec::<i8, i16>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
				Type::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			let slice: &[i16] = result.as_slice();
			assert_eq!(slice, &[1i16, 2i16]);
		}

		#[test]
		fn test_none_maps_to_undefined() {
			// 42 mapped to None
			let data = [42i8];
			let bitvec = BitVec::from_slice(&[true]);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = promote_vec::<i8, i16>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
				Type::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_invalid_bitmaps_are_undefined() {
			let data = [1i8];
			let bitvec = BitVec::from_slice(&[false]);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = promote_vec::<i8, i16>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
				Type::Int2,
				|col, v| col.push::<i16>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_mixed_bitvec_and_promote_failure() {
			let data = [1i8, 42i8, 3i8, 4i8];
			let bitvec =
				BitVec::from_slice(&[true, true, false, true]);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = promote_vec::<i8, i16>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
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

		impl Promote for &TestCtx {
			/// Can only used with i8
			fn promote<From, To>(
				&self,
				val: From,
				_span: impl IntoOwnedSpan,
			) -> crate::Result<Option<To>>
			where
				From: SafePromote<To>,
			{
				// Only simulate promotion failure for i8 == 42
				let raw: i8 = unsafe {
					std::mem::transmute_copy(&val)
				};
				if raw == 42 {
					return Ok(None);
				}
				Ok(Some(val.checked_promote().unwrap()))
			}
		}
	}

	mod demote {
		use reifydb_core::{
			BitVec, IntoOwnedSpan, OwnedSpan, Type,
			value::{
				container::NumberContainer, number::SafeDemote,
			},
		};

		use crate::evaluate::{Demote, cast::number::demote_vec};

		#[test]
		fn test_ok() {
			let data = [1i16, 2i16];
			let bitvec = BitVec::from_slice(&[true, true]);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = demote_vec::<i16, i8>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
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
		fn test_none_maps_to_undefined() {
			let data = [42i16];
			let bitvec = BitVec::from_slice(&[true]);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = demote_vec::<i16, i8>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
				Type::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_invalid_bitmaps_are_undefined() {
			let data = [1i16];
			let bitvec = BitVec::repeat(1, false);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = demote_vec::<i16, i8>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
				Type::Int1,
				|col, v| col.push::<i8>(v),
			)
			.unwrap();

			assert!(!result.is_defined(0));
		}

		#[test]
		fn test_mixed_bitvec_and_demote_failure() {
			let data = [1i16, 42i16, 3i16, 4i16];
			let bitvec =
				BitVec::from_slice(&[true, true, false, true]);
			let ctx = TestCtx::new();

			let container =
				NumberContainer::new(data.to_vec(), bitvec);
			let result = demote_vec::<i16, i8>(
				&container,
				&ctx,
				|| OwnedSpan::testing_empty(),
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

		struct TestCtx;

		impl TestCtx {
			fn new() -> Self {
				Self
			}
		}

		impl Demote for &TestCtx {
			/// Can only be used with i16 → i8
			fn demote<From, To>(
				&self,
				val: From,
				_span: impl IntoOwnedSpan,
			) -> crate::Result<Option<To>>
			where
				From: SafeDemote<To>,
			{
				// Only simulate promotion failure for i16 == 42
				let raw: i16 = unsafe {
					std::mem::transmute_copy(&val)
				};
				if raw == 42 {
					return Ok(None);
				}
				Ok(Some(val.checked_demote().unwrap()))
			}
		}
	}
}
