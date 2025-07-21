// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::error::diagnostic::number::{invalid_number_format, nan_not_allowed, number_out_of_range};
use crate::value::is::{IsFloat, IsInt, IsUint};
use crate::{return_error, Error, Span, Type};
use std::any::TypeId;
use std::num::IntErrorKind;
use std::str::FromStr;

pub fn parse_int<T>(span: impl Span) -> Result<T, Error>
where
    T: IsInt + 'static,
{
    if TypeId::of::<T>() == TypeId::of::<i8>() {
        Ok(cast::<T, i8>(parse_i8(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<i16>() {
        Ok(cast::<T, i16>(parse_i16(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<i32>() {
        Ok(cast::<T, i32>(parse_i32(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<i64>() {
        Ok(cast::<T, i64>(parse_i64(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<i128>() {
        Ok(cast::<T, i128>(parse_i128(span)?))
    } else {
        unreachable!();
    }
}

pub fn parse_uint<T>(span: impl Span) -> Result<T, Error>
where
    T: IsUint + 'static,
{
    if TypeId::of::<T>() == TypeId::of::<u8>() {
        Ok(cast::<T, u8>(parse_u8(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<u16>() {
        Ok(cast::<T, u16>(parse_u16(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<u32>() {
        Ok(cast::<T, u32>(parse_u32(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<u64>() {
        Ok(cast::<T, u64>(parse_u64(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<u128>() {
        Ok(cast::<T, u128>(parse_u128(span)?))
    } else {
        unreachable!();
    }
}

pub fn parse_float<T>(span: impl Span) -> Result<T, Error>
where
    T: IsFloat + 'static,
{
    if span.fragment().to_lowercase().contains("nan") {
        return_error!(nan_not_allowed());
    }

    if TypeId::of::<T>() == TypeId::of::<f32>() {
        Ok(cast::<T, f32>(parse_f32(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<f64>() {
        Ok(cast::<T, f64>(parse_f64(span)?))
    } else {
        unreachable!();
    }
}

fn cast_float_to_int<T: 'static>(f: f64) -> T {
    if TypeId::of::<T>() == TypeId::of::<i8>() {
        cast::<T, i8>(f as i8)
    } else if TypeId::of::<T>() == TypeId::of::<i16>() {
        cast::<T, i16>(f as i16)
    } else if TypeId::of::<T>() == TypeId::of::<i32>() {
        cast::<T, i32>(f as i32)
    } else if TypeId::of::<T>() == TypeId::of::<i64>() {
        cast::<T, i64>(f as i64)
    } else if TypeId::of::<T>() == TypeId::of::<i128>() {
        cast::<T, i128>(f as i128)
    } else if TypeId::of::<T>() == TypeId::of::<u8>() {
        cast::<T, u8>(f as u8)
    } else if TypeId::of::<T>() == TypeId::of::<u16>() {
        cast::<T, u16>(f as u16)
    } else if TypeId::of::<T>() == TypeId::of::<u32>() {
        cast::<T, u32>(f as u32)
    } else if TypeId::of::<T>() == TypeId::of::<u64>() {
        cast::<T, u64>(f as u64)
    } else if TypeId::of::<T>() == TypeId::of::<u128>() {
        cast::<T, u128>(f as u128)
    } else {
        unreachable!()
    }
}

fn cast<T: 'static, U: 'static>(v: U) -> T {
    // SAFETY: caller guarantees that T and U are the same type
    assert_eq!(TypeId::of::<T>(), TypeId::of::<U>());
    unsafe { std::mem::transmute_copy(&v) }
}

trait TypeInfo {
    fn type_enum() -> Type;
}

impl TypeInfo for i8 {
    fn type_enum() -> Type {
        Type::Int1
    }
}
impl TypeInfo for i16 {
    fn type_enum() -> Type {
        Type::Int2
    }
}
impl TypeInfo for i32 {
    fn type_enum() -> Type {
        Type::Int4
    }
}
impl TypeInfo for i64 {
    fn type_enum() -> Type {
        Type::Int8
    }
}
impl TypeInfo for i128 {
    fn type_enum() -> Type {
        Type::Int16
    }
}
impl TypeInfo for u8 {
    fn type_enum() -> Type {
        Type::Uint1
    }
}
impl TypeInfo for u16 {
    fn type_enum() -> Type {
        Type::Uint2
    }
}
impl TypeInfo for u32 {
    fn type_enum() -> Type {
        Type::Uint4
    }
}
impl TypeInfo for u64 {
    fn type_enum() -> Type {
        Type::Uint8
    }
}
impl TypeInfo for u128 {
    fn type_enum() -> Type {
        Type::Uint16
    }
}
impl TypeInfo for f32 {
    fn type_enum() -> Type {
        Type::Float4
    }
}
impl TypeInfo for f64 {
    fn type_enum() -> Type {
        Type::Float8
    }
}

#[inline]
fn parse_signed_generic<T>(span: impl Span) -> Result<T, Error>
where
    T: FromStr<Err = std::num::ParseIntError> + TypeInfo + 'static,
{
    let value = span.fragment().replace("_", "");
    let value = value.trim();

    if value.is_empty() {
        return_error!(invalid_number_format(span.to_owned(), T::type_enum()));
    }

    match value.parse::<T>() {
        Ok(v) => Ok(v),
        Err(err) => match err.kind() {
            IntErrorKind::Empty => Err(Error(invalid_number_format(span.to_owned(), T::type_enum()))),
            IntErrorKind::InvalidDigit => {
                if let Ok(f) = value.parse::<f64>() {
                    let truncated = f.trunc();
                    let type_enum = T::type_enum();
                    let in_range = match type_enum {
                        Type::Int1 => truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64,
                        Type::Int2 => truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64,
                        Type::Int4 => truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64,
                        Type::Int8 => truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64,
                        Type::Int16 => {
                            truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64
                        }
                        _ => false,
                    };
                    if in_range {
                        Ok(cast_float_to_int::<T>(truncated))
                    } else {
                        Err(Error(number_out_of_range(span.to_owned(), type_enum)))
                    }
                } else {
                    Err(Error(invalid_number_format(span.to_owned(), T::type_enum())))
                }
            }
            IntErrorKind::PosOverflow => {
                Err(Error(number_out_of_range(span.to_owned(), T::type_enum())))
            }
            IntErrorKind::NegOverflow => {
                Err(Error(number_out_of_range(span.to_owned(), T::type_enum())))
            }
            IntErrorKind::Zero => Err(Error(invalid_number_format(span.to_owned(), T::type_enum()))),
            &_ => unreachable!("{}", err),
        },
    }
}

#[inline]
fn parse_unsigned_generic<T>(span: impl Span) -> Result<T, Error>
where
    T: FromStr<Err = std::num::ParseIntError> + TypeInfo + 'static,
{
    let value = span.fragment().replace("_", "");
    let value = value.trim();

    if value.is_empty() {
        return_error!(invalid_number_format(span.to_owned(), T::type_enum()));
    }

    match value.parse::<T>() {
        Ok(v) => Ok(v),
        Err(err) => {
            match err.kind() {
                IntErrorKind::Empty => {
                    Err(Error(invalid_number_format(span.to_owned(), T::type_enum())))
                }
                IntErrorKind::InvalidDigit => {
                    if let Ok(f) = value.parse::<f64>() {
                        // For unsigned types, reject negative values
                        if f < 0.0 {
                            return_error!(number_out_of_range(span.to_owned(), T::type_enum()));
                        }
                        let truncated = f.trunc();
                        let type_enum = T::type_enum();
                        let in_range = match type_enum {
                            Type::Uint1 => truncated >= 0.0 && truncated <= u8::MAX as f64,
                            Type::Uint2 => truncated >= 0.0 && truncated <= u16::MAX as f64,
                            Type::Uint4 => truncated >= 0.0 && truncated <= u32::MAX as f64,
                            Type::Uint8 => truncated >= 0.0 && truncated <= u64::MAX as f64,
                            Type::Uint16 => truncated >= 0.0 && truncated <= u128::MAX as f64,
                            _ => false,
                        };
                        if in_range {
                            Ok(cast_float_to_int::<T>(truncated))
                        } else {
                            Err(Error(number_out_of_range(span.to_owned(), type_enum)))
                        }
                    } else {
                        if value.contains("-") {
                            Err(Error(number_out_of_range(span.to_owned(), T::type_enum())))
                        } else {
                            Err(Error(invalid_number_format(span.to_owned(), T::type_enum())))
                        }
                    }
                }
                IntErrorKind::PosOverflow => {
                    Err(Error(number_out_of_range(span.to_owned(), T::type_enum())))
                }
                IntErrorKind::NegOverflow => {
                    Err(Error(number_out_of_range(span.to_owned(), T::type_enum())))
                }
                IntErrorKind::Zero => {
                    Err(Error(invalid_number_format(span.to_owned(), T::type_enum())))
                }
                &_ => unreachable!("{}", err),
            }
        }
    }
}

#[inline]
fn parse_float_generic<T>(span: impl Span) -> Result<T, Error>
where
    T: FromStr<Err = std::num::ParseFloatError> + Copy + TypeInfo + PartialEq + 'static,
{
    let value = span.fragment().replace("_", "");
    let value = value.trim();

    if value.is_empty() {
        return_error!(invalid_number_format(span.to_owned(), T::type_enum()));
    }

    match value.parse::<T>() {
        Ok(v) => {
            if TypeId::of::<T>() == TypeId::of::<f32>() {
                let v_f32 = cast::<f32, T>(v);
                if v_f32 == f32::INFINITY || v_f32 == f32::NEG_INFINITY {
                    return_error!(number_out_of_range(span.to_owned(), T::type_enum()));
                }
            } else if TypeId::of::<T>() == TypeId::of::<f64>() {
                let v_f64 = cast::<f64, T>(v);
                if v_f64 == f64::INFINITY || v_f64 == f64::NEG_INFINITY {
                    return_error!(number_out_of_range(span.to_owned(), T::type_enum()));
                }
            }
            Ok(v)
        }
        Err(_) => Err(Error(invalid_number_format(span.to_owned(), T::type_enum()))),
    }
}

#[inline]
fn parse_f32(span: impl Span) -> Result<f32, Error> {
    parse_float_generic::<f32>(span)
}

#[inline]
fn parse_f64(span: impl Span) -> Result<f64, Error> {
    parse_float_generic::<f64>(span)
}

#[inline]
fn parse_i8(span: impl Span) -> Result<i8, Error> {
    parse_signed_generic::<i8>(span)
}

#[inline]
fn parse_i16(span: impl Span) -> Result<i16, Error> {
    parse_signed_generic::<i16>(span)
}

#[inline]
fn parse_i32(span: impl Span) -> Result<i32, Error> {
    parse_signed_generic::<i32>(span)
}

#[inline]
fn parse_i64(span: impl Span) -> Result<i64, Error> {
    parse_signed_generic::<i64>(span)
}

#[inline]
fn parse_i128(span: impl Span) -> Result<i128, Error> {
    parse_signed_generic::<i128>(span)
}

#[inline]
fn parse_u8(span: impl Span) -> Result<u8, Error> {
    parse_unsigned_generic::<u8>(span)
}

#[inline]
fn parse_u16(span: impl Span) -> Result<u16, Error> {
    parse_unsigned_generic::<u16>(span)
}

#[inline]
fn parse_u32(span: impl Span) -> Result<u32, Error> {
    parse_unsigned_generic::<u32>(span)
}

#[inline]
fn parse_u64(span: impl Span) -> Result<u64, Error> {
    parse_unsigned_generic::<u64>(span)
}

#[inline]
fn parse_u128(span: impl Span) -> Result<u128, Error> {
    parse_unsigned_generic::<u128>(span)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnedSpan;

    mod i8 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("42")), Ok(42));
        }

        #[test]
        fn test_valid_negative() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("-42")), Ok(-42));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("127")), Ok(127));
        }

        #[test]
        fn test_valid_min() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("-128")), Ok(-128));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_int::<i8>(OwnedSpan::testing("128")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_int::<i8>(OwnedSpan::testing("-129")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_int::<i8>(OwnedSpan::testing("abc")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_int::<i8>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_invalid_whitespace() {
            assert!(parse_int::<i8>(OwnedSpan::testing("   ")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("42.9")), Ok(42));
        }

        #[test]
        fn test_float_truncation_negative() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("-42.9")), Ok(-42));
        }

        #[test]
        fn test_float_truncation_zero() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("0.0")), Ok(0));
        }

        #[test]
        fn test_float_truncation_negative_zero() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("-0.0")), Ok(0));
        }

        #[test]
        fn test_float_truncation_max() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("127.9")), Ok(127));
        }

        #[test]
        fn test_float_truncation_min() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("-128.9")), Ok(-128));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("1e+2")), Ok(100));
        }

        #[test]
        fn test_float_scientific_small() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("1.23e-1")), Ok(0));
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_int::<i8>(OwnedSpan::testing("128.0")).is_err());
        }

        #[test]
        fn test_float_overflow_negative() {
            assert!(parse_int::<i8>(OwnedSpan::testing("-129.0")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_int::<i8>(OwnedSpan::testing("1e3")).is_err());
        }

        #[test]
        fn test_invalid_float_format() {
            assert!(parse_int::<i8>(OwnedSpan::testing("1.2.3")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing(" 42")), Ok(42));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("42 ")), Ok(42));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing(" 42 ")), Ok(42));
        }

        #[test]
        fn trimming_negative_leading_space() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing(" -42")), Ok(-42));
        }

        #[test]
        fn trimming_negative_trailing_space() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing("-42 ")), Ok(-42));
        }

        #[test]
        fn trimming_negative_both_spaces() {
            assert_eq!(parse_int::<i8>(OwnedSpan::testing(" -42 ")), Ok(-42));
        }
    }

    mod i16 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("1000")), Ok(1000));
        }

        #[test]
        fn test_valid_negative() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("-1000")), Ok(-1000));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("32767")), Ok(32767));
        }

        #[test]
        fn test_valid_min() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("-32768")), Ok(-32768));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_int::<i16>(OwnedSpan::testing("32768")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_int::<i16>(OwnedSpan::testing("-32769")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_int::<i16>(OwnedSpan::testing("hello")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_int::<i16>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("1000.7")), Ok(1000));
        }

        #[test]
        fn test_float_truncation_negative() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("-1000.7")), Ok(-1000));
        }

        #[test]
        fn test_float_truncation_max() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("32767.9")), Ok(32767));
        }

        #[test]
        fn test_float_truncation_min() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("-32768.9")), Ok(-32768));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("1.5e3")), Ok(1500));
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_int::<i16>(OwnedSpan::testing("32768.0")).is_err());
        }

        #[test]
        fn test_float_overflow_negative() {
            assert!(parse_int::<i16>(OwnedSpan::testing("-32769.0")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_int::<i16>(OwnedSpan::testing("1e5")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing(" 1000")), Ok(1000));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("1000 ")), Ok(1000));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing(" 1000 ")), Ok(1000));
        }

        #[test]
        fn trimming_negative_leading_space() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing(" -1000")), Ok(-1000));
        }

        #[test]
        fn trimming_negative_trailing_space() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing("-1000 ")), Ok(-1000));
        }

        #[test]
        fn trimming_negative_both_spaces() {
            assert_eq!(parse_int::<i16>(OwnedSpan::testing(" -1000 ")), Ok(-1000));
        }
    }

    mod i32 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("1000000")), Ok(1000000));
        }

        #[test]
        fn test_valid_negative() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("-1000000")), Ok(-1000000));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("2147483647")), Ok(2147483647));
        }

        #[test]
        fn test_valid_min() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("-2147483648")), Ok(-2147483648));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_int::<i32>(OwnedSpan::testing("2147483648")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_int::<i32>(OwnedSpan::testing("-2147483649")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_int::<i32>(OwnedSpan::testing("not_a_number")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_int::<i32>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("3.14")), Ok(3));
        }

        #[test]
        fn test_float_truncation_negative() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("-3.14")), Ok(-3));
        }

        #[test]
        fn test_float_truncation_zero() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("0.0")), Ok(0));
        }

        #[test]
        fn test_float_truncation_negative_zero() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("-0.0")), Ok(0));
        }

        #[test]
        fn test_float_truncation_large() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("42.999")), Ok(42));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("1e+2")), Ok(100));
        }

        #[test]
        fn test_float_scientific_decimal() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("2.5e3")), Ok(2500));
        }

        #[test]
        fn test_float_scientific_negative() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("-1.5e2")), Ok(-150));
        }

        #[test]
        fn test_float_scientific_small() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("1.23e-1")), Ok(0));
        }

        #[test]
        fn test_float_scientific_very_small() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("9.9e-1")), Ok(0));
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_int::<i32>(OwnedSpan::testing("2147483648.0")).is_err());
        }

        #[test]
        fn test_float_overflow_negative() {
            assert!(parse_int::<i32>(OwnedSpan::testing("-2147483649.0")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_int::<i32>(OwnedSpan::testing("1e10")).is_err());
        }

        #[test]
        fn test_invalid_float_format() {
            assert!(parse_int::<i32>(OwnedSpan::testing("1.2.3")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing(" 123")), Ok(123));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("123 ")), Ok(123));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing(" 123 ")), Ok(123));
        }

        #[test]
        fn trimming_negative_leading_space() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing(" -456")), Ok(-456));
        }

        #[test]
        fn trimming_negative_trailing_space() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing("-456 ")), Ok(-456));
        }

        #[test]
        fn trimming_negative_both_spaces() {
            assert_eq!(parse_int::<i32>(OwnedSpan::testing(" -456 ")), Ok(-456));
        }
    }

    mod i64 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("1000000000")), Ok(1000000000));
        }

        #[test]
        fn test_valid_negative() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("-1000000000")), Ok(-1000000000));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("9223372036854775807")), Ok(i64::MAX));
        }

        #[test]
        fn test_valid_min() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("-9223372036854775808")), Ok(i64::MIN));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_int::<i64>(OwnedSpan::testing("9223372036854775808")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_int::<i64>(OwnedSpan::testing("-9223372036854775809")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_int::<i64>(OwnedSpan::testing("invalid")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_int::<i64>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("12345.67")), Ok(12345));
        }

        #[test]
        fn test_float_truncation_negative() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("-12345.67")), Ok(-12345));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("1e10")), Ok(10000000000));
        }

        #[test]
        fn test_float_scientific_large() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("9.223e18")), Ok(9223000000000000000));
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_int::<i64>(OwnedSpan::testing("1e19")).is_err());
        }

        #[test]
        fn test_float_overflow_negative() {
            assert!(parse_int::<i64>(OwnedSpan::testing("-1e19")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing(" 1000000000")), Ok(1000000000));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("1000000000 ")), Ok(1000000000));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing(" 1000000000 ")), Ok(1000000000));
        }

        #[test]
        fn trimming_negative_leading_space() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing(" -1000000000")), Ok(-1000000000));
        }

        #[test]
        fn trimming_negative_trailing_space() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing("-1000000000 ")), Ok(-1000000000));
        }

        #[test]
        fn trimming_negative_both_spaces() {
            assert_eq!(parse_int::<i64>(OwnedSpan::testing(" -1000000000 ")), Ok(-1000000000));
        }
    }

    mod i128 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("12345678901234567890")), Ok(12345678901234567890));
        }

        #[test]
        fn test_valid_negative() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("-12345678901234567890")), Ok(-12345678901234567890));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing(&i128::MAX.to_string())), Ok(i128::MAX));
        }

        #[test]
        fn test_valid_min() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing(&i128::MIN.to_string())), Ok(i128::MIN));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_int::<i128>(OwnedSpan::testing("170141183460469231731687303715884105728")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_int::<i128>(OwnedSpan::testing("-170141183460469231731687303715884105729")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_int::<i128>(OwnedSpan::testing("abc")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_int::<i128>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("123456789.123")), Ok(123456789));
        }

        #[test]
        fn test_float_truncation_negative() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("-123456789.123")), Ok(-123456789));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("1e20")), Ok(100000000000000000000));
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_int::<i128>(OwnedSpan::testing("1e40")).is_err());
        }

        #[test]
        fn test_float_overflow_negative() {
            assert!(parse_int::<i128>(OwnedSpan::testing("-1e40")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing(" 12345678901234567890")), Ok(12345678901234567890));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("12345678901234567890 ")), Ok(12345678901234567890));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing(" 12345678901234567890 ")), Ok(12345678901234567890));
        }

        #[test]
        fn trimming_negative_leading_space() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing(" -12345678901234567890")), Ok(-12345678901234567890));
        }

        #[test]
        fn trimming_negative_trailing_space() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing("-12345678901234567890 ")), Ok(-12345678901234567890));
        }

        #[test]
        fn trimming_negative_both_spaces() {
            assert_eq!(parse_int::<i128>(OwnedSpan::testing(" -12345678901234567890 ")), Ok(-12345678901234567890));
        }
    }

    mod u8 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("128")), Ok(128));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("255")), Ok(255));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("256")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("-1")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("abc")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("128.9")), Ok(128));
        }

        #[test]
        fn test_float_truncation_zero() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("0.0")), Ok(0));
        }

        #[test]
        fn test_float_truncation_max() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("255.9")), Ok(255));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("2e2")), Ok(200));
        }

        #[test]
        fn test_float_scientific_small() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("1.23e-1")), Ok(0));
        }

        #[test]
        fn test_float_negative() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("-1.5")).is_err());
        }

        #[test]
        fn test_float_negative_zero() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("-0.1")).is_err());
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("256.0")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("1e3")).is_err());
        }

        #[test]
        fn test_invalid_float_format() {
            assert!(parse_uint::<u8>(OwnedSpan::testing("1.2.3")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing(" 128")), Ok(128));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing("128 ")), Ok(128));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_uint::<u8>(OwnedSpan::testing(" 128 ")), Ok(128));
        }
    }

    mod u16 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing("32768")), Ok(32768));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing("65535")), Ok(65535));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_uint::<u16>(OwnedSpan::testing("65536")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_uint::<u16>(OwnedSpan::testing("-1")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_uint::<u16>(OwnedSpan::testing("invalid")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_uint::<u16>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing("32768.7")), Ok(32768));
        }

        #[test]
        fn test_float_truncation_max() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing("65535.9")), Ok(65535));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing("6.5e4")), Ok(65000));
        }

        #[test]
        fn test_float_negative() {
            assert!(parse_uint::<u16>(OwnedSpan::testing("-100.0")).is_err());
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_uint::<u16>(OwnedSpan::testing("65536.0")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_uint::<u16>(OwnedSpan::testing("1e5")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing(" 32768")), Ok(32768));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing("32768 ")), Ok(32768));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_uint::<u16>(OwnedSpan::testing(" 32768 ")), Ok(32768));
        }
    }

    mod u32 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("1000000")), Ok(1000000));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("4294967295")), Ok(4294967295));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("4294967296")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("-1")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("text")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("3.14")), Ok(3));
        }

        #[test]
        fn test_float_truncation_zero() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("0.0")), Ok(0));
        }

        #[test]
        fn test_float_truncation_large() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("42.999")), Ok(42));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("1e+2")), Ok(100));
        }

        #[test]
        fn test_float_scientific_decimal() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("2.5e3")), Ok(2500));
        }

        #[test]
        fn test_float_scientific_small() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("1.23e-1")), Ok(0));
        }

        #[test]
        fn test_float_negative() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("-3.14")).is_err());
        }

        #[test]
        fn test_float_negative_small() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("-0.1")).is_err());
        }

        #[test]
        fn test_float_negative_scientific() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("-1e2")).is_err());
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("4294967296.0")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("1e10")).is_err());
        }

        #[test]
        fn test_invalid_float_format() {
            assert!(parse_uint::<u32>(OwnedSpan::testing("1.2.3")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing(" 1000000")), Ok(1000000));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing("1000000 ")), Ok(1000000));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_uint::<u32>(OwnedSpan::testing(" 1000000 ")), Ok(1000000));
        }
    }

    mod u64 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing("1000000000000")), Ok(1000000000000));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing("18446744073709551615")), Ok(u64::MAX));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_uint::<u64>(OwnedSpan::testing("18446744073709551616")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_uint::<u64>(OwnedSpan::testing("-1")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_uint::<u64>(OwnedSpan::testing("not_valid")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_uint::<u64>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing("123456789.123")), Ok(123456789));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing("1e12")), Ok(1000000000000));
        }

        #[test]
        fn test_float_negative() {
            assert!(parse_uint::<u64>(OwnedSpan::testing("-1.0")).is_err());
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_uint::<u64>(OwnedSpan::testing("2e19")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_uint::<u64>(OwnedSpan::testing("1e20")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing(" 1000000000000")), Ok(1000000000000));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing("1000000000000 ")), Ok(1000000000000));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_uint::<u64>(OwnedSpan::testing(" 1000000000000 ")), Ok(1000000000000));
        }
    }

    mod u128 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing("0")), Ok(0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing("12345678901234567890")), Ok(12345678901234567890));
        }

        #[test]
        fn test_valid_max() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing(&u128::MAX.to_string())), Ok(u128::MAX));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_uint::<u128>(OwnedSpan::testing("340282366920938463463374607431768211456")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_uint::<u128>(OwnedSpan::testing("-1")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_uint::<u128>(OwnedSpan::testing("abc")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_uint::<u128>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_float_truncation_positive() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing("123456789.999")), Ok(123456789));
        }

        #[test]
        fn test_float_scientific_notation() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing("1e20")), Ok(100000000000000000000));
        }

        #[test]
        fn test_float_negative() {
            assert!(parse_uint::<u128>(OwnedSpan::testing("-1.0")).is_err());
        }

        #[test]
        fn test_float_overflow_positive() {
            assert!(parse_uint::<u128>(OwnedSpan::testing("1e40")).is_err());
        }

        #[test]
        fn test_float_overflow_scientific() {
            assert!(parse_uint::<u128>(OwnedSpan::testing("1e50")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing(" 12345678901234567890")), Ok(12345678901234567890));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing("12345678901234567890 ")), Ok(12345678901234567890));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_uint::<u128>(OwnedSpan::testing(" 12345678901234567890 ")), Ok(12345678901234567890));
        }
    }

    mod f32 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("0.0")), Ok(0.0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("1.5")), Ok(1.5));
        }

        #[test]
        fn test_valid_negative() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("-3.14")), Ok(-3.14));
        }

        #[test]
        fn test_valid_integer() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("42")), Ok(42.0));
        }

        #[test]
        fn test_valid_scientific() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("1e2")), Ok(100.0));
        }

        #[test]
        fn test_valid_scientific_negative() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("1e-2")), Ok(0.01));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_float::<f32>(OwnedSpan::testing("3.5e38")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_float::<f32>(OwnedSpan::testing("-3.5e38")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_float::<f32>(OwnedSpan::testing("abc")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_float::<f32>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_invalid_whitespace() {
            assert!(parse_float::<f32>(OwnedSpan::testing("   ")).is_err());
        }

        #[test]
        fn test_invalid_nan() {
            assert!(parse_float::<f32>(OwnedSpan::testing("NaN")).is_err());
        }

        #[test]
        fn test_invalid_nan_lowercase() {
            assert!(parse_float::<f32>(OwnedSpan::testing("nan")).is_err());
        }

        #[test]
        fn test_invalid_multiple_dots() {
            assert!(parse_float::<f32>(OwnedSpan::testing("1.2.3")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing(" 1.5")), Ok(1.5));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("1.5 ")), Ok(1.5));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing(" 1.5 ")), Ok(1.5));
        }

        #[test]
        fn trimming_negative_leading_space() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing(" -3.14")), Ok(-3.14));
        }

        #[test]
        fn trimming_negative_trailing_space() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing("-3.14 ")), Ok(-3.14));
        }

        #[test]
        fn trimming_negative_both_spaces() {
            assert_eq!(parse_float::<f32>(OwnedSpan::testing(" -3.14 ")), Ok(-3.14));
        }
    }

    mod f64 {
        use super::*;

        #[test]
        fn test_valid_zero() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("0.0")), Ok(0.0));
        }

        #[test]
        fn test_valid_positive() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("1.23")), Ok(1.23));
        }

        #[test]
        fn test_valid_negative() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("-0.001")), Ok(-0.001));
        }

        #[test]
        fn test_valid_integer() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("42")), Ok(42.0));
        }

        #[test]
        fn test_valid_scientific() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("1e10")), Ok(1e10));
        }

        #[test]
        fn test_valid_scientific_negative() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("1e-10")), Ok(1e-10));
        }

        #[test]
        fn test_overflow_positive() {
            assert!(parse_float::<f64>(OwnedSpan::testing("1e400")).is_err());
        }

        #[test]
        fn test_overflow_negative() {
            assert!(parse_float::<f64>(OwnedSpan::testing("-1e400")).is_err());
        }

        #[test]
        fn test_invalid_text() {
            assert!(parse_float::<f64>(OwnedSpan::testing("abc")).is_err());
        }

        #[test]
        fn test_invalid_empty() {
            assert!(parse_float::<f64>(OwnedSpan::testing("")).is_err());
        }

        #[test]
        fn test_invalid_whitespace() {
            assert!(parse_float::<f64>(OwnedSpan::testing("   ")).is_err());
        }

        #[test]
        fn test_invalid_nan() {
            assert!(parse_float::<f64>(OwnedSpan::testing("NaN")).is_err());
        }

        #[test]
        fn test_invalid_nan_mixed_case() {
            assert!(parse_float::<f64>(OwnedSpan::testing("NaN")).is_err());
        }

        #[test]
        fn test_invalid_multiple_dots() {
            assert!(parse_float::<f64>(OwnedSpan::testing("1.2.3")).is_err());
        }

        #[test]
        fn trimming_leading_space() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing(" 1.23")), Ok(1.23));
        }

        #[test]
        fn trimming_trailing_space() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("1.23 ")), Ok(1.23));
        }

        #[test]
        fn trimming_both_spaces() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing(" 1.23 ")), Ok(1.23));
        }

        #[test]
        fn trimming_negative_leading_space() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing(" -0.001")), Ok(-0.001));
        }

        #[test]
        fn trimming_negative_trailing_space() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing("-0.001 ")), Ok(-0.001));
        }

        #[test]
        fn trimming_negative_both_spaces() {
            assert_eq!(parse_float::<f64>(OwnedSpan::testing(" -0.001 ")), Ok(-0.001));
        }
    }
}
