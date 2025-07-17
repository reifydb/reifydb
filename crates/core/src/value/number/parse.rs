// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::number::{invalid_number_format, number_out_of_range};
use crate::value::is::{IsFloat, IsInt, IsUint};
use crate::{Error, Span, Type};
use std::any::TypeId;
use std::num::IntErrorKind;
use std::str::FromStr;

pub fn parse_int<T>(span: &Span) -> Result<T, Error>
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

pub fn parse_uint<T>(span: &Span) -> Result<T, Error>
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

pub fn parse_float<T>(span: &Span) -> Result<T, Error>
where
    T: IsFloat + 'static,
{
    if TypeId::of::<T>() == TypeId::of::<f32>() {
        Ok(cast::<T, f32>(parse_f32(span)?))
    } else if TypeId::of::<T>() == TypeId::of::<f64>() {
        Ok(cast::<T, f64>(parse_f64(span)?))
    } else {
        unreachable!();
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
fn parse_signed_generic<T>(span: &Span) -> Result<T, Error>
where
    T: FromStr<Err = std::num::ParseIntError> + TypeInfo,
{
    if span.fragment.trim().is_empty() {
        return Err(Error(invalid_number_format(span.clone(), T::type_enum())));
    }

    match span.fragment.replace("_", "").parse::<T>() {
        Ok(v) => Ok(v),
        Err(err) => match err.kind() {
            IntErrorKind::Empty => Err(Error(invalid_number_format(span.clone(), T::type_enum()))),
            IntErrorKind::InvalidDigit => {
                Err(Error(invalid_number_format(span.clone(), T::type_enum())))
            }
            IntErrorKind::PosOverflow => {
                Err(Error(number_out_of_range(span.clone(), T::type_enum())))
            }
            IntErrorKind::NegOverflow => {
                Err(Error(number_out_of_range(span.clone(), T::type_enum())))
            }
            IntErrorKind::Zero => Err(Error(invalid_number_format(span.clone(), T::type_enum()))),
            &_ => unreachable!("{}", err),
        },
    }
}

#[inline]
fn parse_unsigned_generic<T>(span: &Span) -> Result<T, Error>
where
    T: FromStr<Err = std::num::ParseIntError> + TypeInfo,
{
    if span.fragment.trim().is_empty() {
        return Err(Error(invalid_number_format(span.clone(), T::type_enum())));
    }

    match span.fragment.replace("_", "").parse::<T>() {
        Ok(v) => Ok(v),
        Err(err) => {
            if span.fragment.contains("-") {
                return Err(Error(number_out_of_range(span.clone(), T::type_enum())));
            }
            match err.kind() {
                IntErrorKind::Empty => {
                    Err(Error(invalid_number_format(span.clone(), T::type_enum())))
                }
                IntErrorKind::InvalidDigit => {
                    Err(Error(invalid_number_format(span.clone(), T::type_enum())))
                }
                IntErrorKind::PosOverflow => {
                    Err(Error(number_out_of_range(span.clone(), T::type_enum())))
                }
                IntErrorKind::NegOverflow => {
                    Err(Error(number_out_of_range(span.clone(), T::type_enum())))
                }
                IntErrorKind::Zero => {
                    Err(Error(invalid_number_format(span.clone(), T::type_enum())))
                }
                &_ => unreachable!("{}", err),
            }
        }
    }
}

#[inline]
fn parse_float_generic<T>(span: &Span) -> Result<T, Error>
where
    T: FromStr<Err = std::num::ParseFloatError> + Copy + TypeInfo + PartialEq + 'static,
{
    if span.fragment.trim().is_empty() {
        return Err(Error(invalid_number_format(span.clone(), T::type_enum())));
    }

    match span.fragment.replace("_", "").parse::<T>() {
        Ok(v) => {
            if TypeId::of::<T>() == TypeId::of::<f32>() {
                let v_f32 = cast::<f32, T>(v);
                if v_f32 == f32::INFINITY || v_f32 == f32::NEG_INFINITY {
                    return Err(Error(number_out_of_range(span.clone(), T::type_enum())));
                }
            } else if TypeId::of::<T>() == TypeId::of::<f64>() {
                let v_f64 = cast::<f64, T>(v);
                if v_f64 == f64::INFINITY || v_f64 == f64::NEG_INFINITY {
                    return Err(Error(number_out_of_range(span.clone(), T::type_enum())));
                }
            }
            Ok(v)
        }
        Err(_) => Err(Error(invalid_number_format(span.clone(), T::type_enum()))),
    }
}

#[inline]
fn parse_f32(span: &Span) -> Result<f32, Error> {
    parse_float_generic::<f32>(span)
}

#[inline]
fn parse_f64(span: &Span) -> Result<f64, Error> {
    parse_float_generic::<f64>(span)
}

#[inline]
fn parse_i8(span: &Span) -> Result<i8, Error> {
    parse_signed_generic::<i8>(span)
}

#[inline]
fn parse_i16(span: &Span) -> Result<i16, Error> {
    parse_signed_generic::<i16>(span)
}

#[inline]
fn parse_i32(span: &Span) -> Result<i32, Error> {
    parse_signed_generic::<i32>(span)
}

#[inline]
fn parse_i64(span: &Span) -> Result<i64, Error> {
    parse_signed_generic::<i64>(span)
}

#[inline]
fn parse_i128(span: &Span) -> Result<i128, Error> {
    parse_signed_generic::<i128>(span)
}

#[inline]
fn parse_u8(span: &Span) -> Result<u8, Error> {
    parse_unsigned_generic::<u8>(span)
}

#[inline]
fn parse_u16(span: &Span) -> Result<u16, Error> {
    parse_unsigned_generic::<u16>(span)
}

#[inline]
fn parse_u32(span: &Span) -> Result<u32, Error> {
    parse_unsigned_generic::<u32>(span)
}

#[inline]
fn parse_u64(span: &Span) -> Result<u64, Error> {
    parse_unsigned_generic::<u64>(span)
}

#[inline]
fn parse_u128(span: &Span) -> Result<u128, Error> {
    parse_unsigned_generic::<u128>(span)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;

    #[test]
    fn test_parse_float_valid_f32() {
        assert_eq!(parse_float::<f32>(&Span::testing("0.0")), Ok(0.0));
        assert_eq!(parse_float::<f32>(&Span::testing("1.5")), Ok(1.5));
        assert_eq!(parse_float::<f32>(&Span::testing("-3.14")), Ok(-3.14));
    }

    #[test]
    fn test_parse_float_valid_f64() {
        assert_eq!(parse_float::<f64>(&Span::testing("0.0")), Ok(0.0));
        assert_eq!(parse_float::<f64>(&Span::testing("1.5")), Ok(1.5));
        assert_eq!(parse_float::<f64>(&Span::testing("-3.14")), Ok(-3.14));
    }

    #[test]
    fn test_parse_float_saturation_f32() {
        let val = "3.5e38";
        assert!(parse_float::<f32>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_float_saturation_f64() {
        let val = "1e400";
        assert!(parse_float::<f64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_float_underflow_f32() {
        let val = "-3.5e38";
        assert!(parse_float::<f32>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_float_underflow_f64() {
        let val = "-1e400";
        assert!(parse_float::<f64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_float_invalid_input() {
        let val = "abc";
        assert!(parse_float::<f64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_float_empty_input() {
        let val = "   ";
        assert!(parse_float::<f64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_valid_i8() {
        assert_eq!(parse_int::<i8>(&Span::testing("0")), Ok(0));
        assert_eq!(parse_int::<i8>(&Span::testing("-128")), Ok(-128));
        assert_eq!(parse_int::<i8>(&Span::testing("127")), Ok(127));
    }

    #[test]
    fn test_parse_int_saturation_i8() {
        let val = "128";
        assert!(parse_int::<i8>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_underflow_i8() {
        let val = "-129";
        assert!(parse_int::<i8>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_valid_i16() {
        assert_eq!(parse_int::<i16>(&Span::testing("32767")), Ok(32767));
        assert_eq!(parse_int::<i16>(&Span::testing("-32768")), Ok(-32768));
    }

    #[test]
    fn test_parse_int_saturation_i16() {
        let val = "32768";
        assert!(parse_int::<i16>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_underflow_i16() {
        let val = "-32769";
        assert!(parse_int::<i16>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_valid_i32() {
        assert_eq!(parse_int::<i32>(&Span::testing("2147483647")), Ok(2_147_483_647));
        assert_eq!(parse_int::<i32>(&Span::testing("-2147483648")), Ok(-2_147_483_648));
    }

    #[test]
    fn test_parse_int_saturation_i32() {
        let val = "2147483648";
        assert!(parse_int::<i32>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_underflow_i32() {
        let val = "-2147483649";
        assert!(parse_int::<i32>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_valid_i64() {
        assert_eq!(parse_int::<i64>(&Span::testing("9223372036854775807")), Ok(i64::MAX));
        assert_eq!(parse_int::<i64>(&Span::testing("-9223372036854775808")), Ok(i64::MIN));
    }

    #[test]
    fn test_parse_int_saturation_i64() {
        let val = "9223372036854775808";
        assert!(parse_int::<i64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_underflow_i64() {
        let val = "-9223372036854775809";
        assert!(parse_int::<i64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_valid_i128() {
        assert_eq!(parse_int::<i128>(&Span::testing(&i128::MAX.to_string())), Ok(i128::MAX));
        assert_eq!(parse_int::<i128>(&Span::testing(&i128::MIN.to_string())), Ok(i128::MIN));
    }

    #[test]
    fn test_parse_int_saturation_i128() {
        let val = "170141183460469231731687303715884105728";
        assert!(parse_int::<i128>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_underflow_i128() {
        let val = "-170141183460469231731687303715884105729";
        assert!(parse_int::<i128>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_int_invalid_input() {
        let val = "hello";
        assert!(parse_int::<i32>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_valid_u8() {
        assert_eq!(parse_uint::<u8>(&Span::testing("0")), Ok(0));
        assert_eq!(parse_uint::<u8>(&Span::testing("255")), Ok(255));
    }

    #[test]
    fn test_parse_uint_saturation_u8() {
        let val = "256"; // u8::MAX + 1
        assert!(parse_uint::<u8>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_underflow_u8() {
        let val = "-1";
        assert!(parse_uint::<u8>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_valid_u16() {
        assert_eq!(parse_uint::<u16>(&Span::testing("65535")), Ok(65535));
        assert_eq!(parse_uint::<u16>(&Span::testing("12345")), Ok(12345));
    }

    #[test]
    fn test_parse_uint_saturation_u16() {
        let val = "65536";
        assert!(parse_uint::<u16>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_valid_u32() {
        assert_eq!(parse_uint::<u32>(&Span::testing("4294967295")), Ok(4_294_967_295));
    }

    #[test]
    fn test_parse_uint_saturation_u32() {
        let val = "4294967296";
        assert!(parse_uint::<u32>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_valid_u64() {
        assert_eq!(parse_uint::<u64>(&Span::testing("18446744073709551615")), Ok(u64::MAX));
    }

    #[test]
    fn test_parse_uint_saturation_u64() {
        let val = "18446744073709551616";
        assert!(parse_uint::<u64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_valid_u128() {
        assert_eq!(
            parse_uint::<u128>(&Span::testing("340282366920938463463374607431768211455")),
            Ok(u128::MAX)
        );
    }

    #[test]
    fn test_parse_uint_saturation_u128() {
        let val = "340282366920938463463374607431768211456";
        assert!(parse_uint::<u128>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_invalid_input() {
        let val = "abc123";
        assert!(parse_uint::<u16>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_uint_empty_input() {
        let val = "";
        assert!(parse_uint::<u64>(&Span::testing(val)).is_err());
    }

    #[test]
    fn test_parse_f64_valid() {
        assert_eq!(parse_f64(&Span::testing("1.23")), Ok(1.23));
        assert_eq!(parse_f64(&Span::testing("-0.001")), Ok(-0.001));
    }

    #[test]
    fn test_parse_f64_empty() {
        assert!(parse_f64(&Span::testing("   ")).is_err());
        assert!(parse_f64(&Span::testing("")).is_err());
    }

    #[test]
    fn test_parse_f64_invalid() {
        assert!(parse_f64(&Span::testing("abc")).is_err());
        assert!(parse_f64(&Span::testing("1.2.3")).is_err());
    }

    #[test]
    fn test_parse_f64_saturation() {
        assert!(parse_f64(&Span::testing("1e400")).is_err());
    }

    #[test]
    fn test_parse_f64_underflow() {
        assert!(parse_f64(&Span::testing("-1e400")).is_err());
    }

    #[test]
    fn test_parse_i128_valid() {
        assert_eq!(parse_i128(&Span::testing("0")), Ok(0));
        assert_eq!(parse_i128(&Span::testing("-42")), Ok(-42));
        assert_eq!(parse_i128(&Span::testing(&i128::MAX.to_string())), Ok(i128::MAX));
        assert_eq!(parse_i128(&Span::testing(&i128::MIN.to_string())), Ok(i128::MIN));
    }

    #[test]
    fn test_parse_i128_invalid() {
        assert!(parse_i128(&Span::testing("abc")).is_err());
        assert!(parse_i128(&Span::testing("1.23")).is_err());
        assert!(parse_i128(&Span::testing("")).is_err());
    }

    #[test]
    fn test_parse_i128_saturation() {
        let too_large = "170141183460469231731687303715884105728";
        assert!(parse_i128(&Span::testing(too_large)).is_err());
    }

    #[test]
    fn test_parse_i128_underflow() {
        let too_small = "-170141183460469231731687303715884105729";
        assert!(parse_i128(&Span::testing(too_small)).is_err());
    }

    #[test]
    fn test_parse_u128_valid() {
        assert_eq!(parse_u128(&Span::testing("0")), Ok(0));
        assert_eq!(parse_u128(&Span::testing("123456")), Ok(123456));
        assert_eq!(parse_u128(&Span::testing(&u128::MAX.to_string())), Ok(u128::MAX));
    }

    #[test]
    fn test_parse_u128_invalid() {
        assert!(parse_u128(&Span::testing("abc")).is_err());
        assert!(parse_u128(&Span::testing("1.23")).is_err());
        assert!(parse_u128(&Span::testing("")).is_err());
    }

    #[test]
    fn test_parse_u128_saturation() {
        let too_large = "340282366920938463463374607431768211456";
        assert!(parse_u128(&Span::testing(too_large)).is_err());
    }

    #[test]
    fn test_parse_u128_underflow() {
        let negative = "-1";
        assert!(parse_u128(&Span::testing(negative)).is_err());
    }
}
