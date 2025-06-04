// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::num::bound::{max_float, max_int, max_uint, min_float, min_int};
use crate::num::is::{IsFloat, IsInt, IsUint};
use std::any::TypeId;
use std::fmt::{Display, Formatter};
use std::num::IntErrorKind;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    Invalid(String),
    Overflow(String),
    Underflow(String),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Invalid(input) => write!(f, "invalid number: '{}'", input),
            ParseError::Overflow(input) => write!(f, "value overflow: '{}'", input),
            ParseError::Underflow(input) => write!(f, "value underflow: '{}'", input),
        }
    }
}

pub fn parse_int<T>(s: &str) -> Result<T, ParseError>
where
    T: Copy + 'static + IsInt,
{
    let min = min_int::<T>();
    let max = max_int::<T>();

    let value = parse_i128(s)?;

    if value < min {
        return Err(ParseError::Underflow(s.to_string()));
    } else if value > max {
        return Err(ParseError::Overflow(s.to_string()));
    };

    let casted = if TypeId::of::<T>() == TypeId::of::<i8>() {
        cast::<T, i8>(value as i8)
    } else if TypeId::of::<T>() == TypeId::of::<i16>() {
        cast::<T, i16>(value as i16)
    } else if TypeId::of::<T>() == TypeId::of::<i32>() {
        cast::<T, i32>(value as i32)
    } else if TypeId::of::<T>() == TypeId::of::<i64>() {
        cast::<T, i64>(value as i64)
    } else if TypeId::of::<T>() == TypeId::of::<i128>() {
        cast::<T, i128>(value)
    } else {
        unreachable!();
    };

    Ok(casted)
}

pub fn parse_uint<T>(s: &str) -> Result<T, ParseError>
where
    T: Copy + 'static + IsUint,
{
    let max = max_uint::<T>();
    let value = parse_u128(s)?;

    if value > max {
        return Err(ParseError::Overflow(s.to_string()));
    }

    let casted = if TypeId::of::<T>() == TypeId::of::<u8>() {
        cast::<T, u8>(value as u8)
    } else if TypeId::of::<T>() == TypeId::of::<u16>() {
        cast::<T, u16>(value as u16)
    } else if TypeId::of::<T>() == TypeId::of::<u32>() {
        cast::<T, u32>(value as u32)
    } else if TypeId::of::<T>() == TypeId::of::<u64>() {
        cast::<T, u64>(value as u64)
    } else if TypeId::of::<T>() == TypeId::of::<u128>() {
        cast::<T, u128>(value)
    } else {
        unreachable!();
    };

    Ok(casted)
}

pub fn parse_float<T>(s: &str) -> Result<T, ParseError>
where
    T: Copy + 'static + IsFloat,
{
    let min = min_float::<T>();
    let max = max_float::<T>();
    let value = parse_f64(s)?;

    if value < min {
        return Err(ParseError::Underflow(s.to_string()));
    } else if value > max {
        return Err(ParseError::Overflow(s.to_string()));
    };

    let casted = if TypeId::of::<T>() == TypeId::of::<f32>() {
        cast::<T, f32>(value as f32)
    } else if TypeId::of::<T>() == TypeId::of::<f64>() {
        cast::<T, f64>(value)
    } else {
        unreachable!();
    };

    Ok(casted)
}

fn cast<T: 'static, U: 'static>(v: U) -> T {
    // SAFETY: caller guarantees that T and U are the same type
    assert_eq!(TypeId::of::<T>(), TypeId::of::<U>());
    unsafe { std::mem::transmute_copy(&v) }
}

fn parse_f64(s: &str) -> Result<f64, ParseError> {
    if s.trim().is_empty() {
        return Err(ParseError::Invalid(s.to_string()));
    }

    match s.parse::<f64>() {
        Ok(v) => {
            if v == f64::INFINITY {
                Err(ParseError::Overflow(s.to_string()))
            } else if v == f64::NEG_INFINITY {
                Err(ParseError::Underflow(s.to_string()))
            } else {
                Ok(v)
            }
        }
        Err(_) => Err(ParseError::Invalid(s.to_string())),
    }
}

fn parse_i128(s: &str) -> Result<i128, ParseError> {
    match s.parse::<i128>() {
        Ok(v) => Ok(v),
        Err(err) => match err.kind() {
            IntErrorKind::Empty => Err(ParseError::Invalid(s.to_string())),
            IntErrorKind::InvalidDigit => Err(ParseError::Invalid(s.to_string())),
            IntErrorKind::PosOverflow => Err(ParseError::Overflow(s.to_string())),
            IntErrorKind::NegOverflow => Err(ParseError::Underflow(s.to_string())),
            IntErrorKind::Zero => Err(ParseError::Invalid(s.to_string())),
            &_ => unreachable!("{}", err),
        },
    }
}

fn parse_u128(s: &str) -> Result<u128, ParseError> {
    match s.parse::<u128>() {
        Ok(v) => Ok(v),
        Err(err) => {
            if s.contains("-") {
                return Err(ParseError::Underflow(s.to_string()));
            }
            match err.kind() {
                IntErrorKind::Empty => Err(ParseError::Invalid(s.to_string())),
                IntErrorKind::InvalidDigit => Err(ParseError::Invalid(s.to_string())),
                IntErrorKind::PosOverflow => Err(ParseError::Overflow(s.to_string())),
                IntErrorKind::NegOverflow => Err(ParseError::Underflow(s.to_string())),
                IntErrorKind::Zero => Err(ParseError::Invalid(s.to_string())),
                &_ => unreachable!("{}", err),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ParseError::{Invalid, Overflow, Underflow};

    #[test]
    fn test_parse_float_valid_f32() {
        assert_eq!(parse_float::<f32>("0.0"), Ok(0.0));
        assert_eq!(parse_float::<f32>("1.5"), Ok(1.5));
        assert_eq!(parse_float::<f32>("-3.14"), Ok(-3.14));
    }

    #[test]
    fn test_parse_float_valid_f64() {
        assert_eq!(parse_float::<f64>("0.0"), Ok(0.0));
        assert_eq!(parse_float::<f64>("1.5"), Ok(1.5));
        assert_eq!(parse_float::<f64>("-3.14"), Ok(-3.14));
    }

    #[test]
    fn test_parse_float_overflow_f32() {
        let val = "3.5e38";
        assert_eq!(parse_float::<f32>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_float_overflow_f64() {
        let val = "1e400";
        assert_eq!(parse_float::<f64>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_float_underflow_f32() {
        let val = "-3.5e38";
        assert_eq!(parse_float::<f32>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_float_underflow_f64() {
        let val = "-1e400";
        assert_eq!(parse_float::<f64>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_float_invalid_input() {
        let val = "abc";
        assert_eq!(parse_float::<f64>(val), Err(Invalid(val.to_string())));
    }

    #[test]
    fn test_parse_float_empty_input() {
        let val = "   ";
        assert_eq!(parse_float::<f64>(val), Err(Invalid(val.to_string())));
    }

    #[test]
    fn test_parse_int_valid_i8() {
        assert_eq!(parse_int::<i8>("0"), Ok(0));
        assert_eq!(parse_int::<i8>("-128"), Ok(-128));
        assert_eq!(parse_int::<i8>("127"), Ok(127));
    }

    #[test]
    fn test_parse_int_overflow_i8() {
        let val = "128";
        assert_eq!(parse_int::<i8>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_underflow_i8() {
        let val = "-129";
        assert_eq!(parse_int::<i8>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_valid_i16() {
        assert_eq!(parse_int::<i16>("32767"), Ok(32767));
        assert_eq!(parse_int::<i16>("-32768"), Ok(-32768));
    }

    #[test]
    fn test_parse_int_overflow_i16() {
        let val = "32768";
        assert_eq!(parse_int::<i16>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_underflow_i16() {
        let val = "-32769";
        assert_eq!(parse_int::<i16>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_valid_i32() {
        assert_eq!(parse_int::<i32>("2147483647"), Ok(2_147_483_647));
        assert_eq!(parse_int::<i32>("-2147483648"), Ok(-2_147_483_648));
    }

    #[test]
    fn test_parse_int_overflow_i32() {
        let val = "2147483648";
        assert_eq!(parse_int::<i32>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_underflow_i32() {
        let val = "-2147483649";
        assert_eq!(parse_int::<i32>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_valid_i64() {
        assert_eq!(parse_int::<i64>("9223372036854775807"), Ok(i64::MAX));
        assert_eq!(parse_int::<i64>("-9223372036854775808"), Ok(i64::MIN));
    }

    #[test]
    fn test_parse_int_overflow_i64() {
        let val = "9223372036854775808";
        assert_eq!(parse_int::<i64>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_underflow_i64() {
        let val = "-9223372036854775809";
        assert_eq!(parse_int::<i64>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_valid_i128() {
        assert_eq!(parse_int::<i128>(&i128::MAX.to_string()), Ok(i128::MAX));
        assert_eq!(parse_int::<i128>(&i128::MIN.to_string()), Ok(i128::MIN));
    }

    #[test]
    fn test_parse_int_overflow_i128() {
        let val = "170141183460469231731687303715884105728";
        assert_eq!(parse_int::<i128>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_underflow_i128() {
        let val = "-170141183460469231731687303715884105729";
        assert_eq!(parse_int::<i128>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_int_invalid_input() {
        let val = "hello";
        assert_eq!(parse_int::<i32>(val), Err(Invalid(val.to_string())));
    }

    #[test]
    fn test_parse_uint_valid_u8() {
        assert_eq!(parse_uint::<u8>("0"), Ok(0));
        assert_eq!(parse_uint::<u8>("255"), Ok(255));
    }

    #[test]
    fn test_parse_uint_overflow_u8() {
        let val = "256"; // u8::MAX + 1
        assert_eq!(parse_uint::<u8>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_uint_underflow_u8() {
        let val = "-1";
        assert_eq!(parse_uint::<u8>(val), Err(Underflow(val.to_string())));
    }

    #[test]
    fn test_parse_uint_valid_u16() {
        assert_eq!(parse_uint::<u16>("65535"), Ok(65535));
        assert_eq!(parse_uint::<u16>("12345"), Ok(12345));
    }

    #[test]
    fn test_parse_uint_overflow_u16() {
        let val = "65536";
        assert_eq!(parse_uint::<u16>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_uint_valid_u32() {
        assert_eq!(parse_uint::<u32>("4294967295"), Ok(4_294_967_295));
    }

    #[test]
    fn test_parse_uint_overflow_u32() {
        let val = "4294967296";
        assert_eq!(parse_uint::<u32>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_uint_valid_u64() {
        assert_eq!(parse_uint::<u64>("18446744073709551615"), Ok(u64::MAX));
    }

    #[test]
    fn test_parse_uint_overflow_u64() {
        let val = "18446744073709551616";
        assert_eq!(parse_uint::<u64>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_uint_valid_u128() {
        assert_eq!(parse_uint::<u128>("340282366920938463463374607431768211455"), Ok(u128::MAX));
    }

    #[test]
    fn test_parse_uint_overflow_u128() {
        let val = "340282366920938463463374607431768211456";
        assert_eq!(parse_uint::<u128>(val), Err(Overflow(val.to_string())));
    }

    #[test]
    fn test_parse_uint_invalid_input() {
        let val = "abc123";
        assert_eq!(parse_uint::<u16>(val), Err(Invalid(val.to_string())));
    }

    #[test]
    fn test_parse_uint_empty_input() {
        let val = "";
        assert_eq!(parse_uint::<u64>(val), Err(Invalid(val.to_string())));
    }

    #[test]
    fn test_parse_f64_valid() {
        assert_eq!(parse_f64("1.23"), Ok(1.23));
        assert_eq!(parse_f64("-0.001"), Ok(-0.001));
    }

    #[test]
    fn test_parse_f64_empty() {
        assert_eq!(parse_f64("   "), Err(Invalid("   ".to_string())));
        assert_eq!(parse_f64(""), Err(Invalid("".to_string())));
    }

    #[test]
    fn test_parse_f64_invalid() {
        assert_eq!(parse_f64("abc"), Err(Invalid("abc".to_string())));
        assert_eq!(parse_f64("1.2.3"), Err(Invalid("1.2.3".to_string())));
    }

    #[test]
    fn test_parse_f64_overflow() {
        assert_eq!(parse_f64("1e400"), Err(Overflow("1e400".to_string())));
    }

    #[test]
    fn test_parse_f64_underflow() {
        assert_eq!(parse_f64("-1e400"), Err(Underflow("-1e400".to_string())));
    }

    #[test]
    fn test_parse_i128_valid() {
        assert_eq!(parse_i128("0"), Ok(0));
        assert_eq!(parse_i128("-42"), Ok(-42));
        assert_eq!(parse_i128(&i128::MAX.to_string()), Ok(i128::MAX));
        assert_eq!(parse_i128(&i128::MIN.to_string()), Ok(i128::MIN));
    }

    #[test]
    fn test_parse_i128_invalid() {
        assert_eq!(parse_i128("abc"), Err(Invalid("abc".to_string())));
        assert_eq!(parse_i128("1.23"), Err(Invalid("1.23".to_string())));
        assert_eq!(parse_i128(""), Err(Invalid("".to_string())));
    }

    #[test]
    fn test_parse_i128_overflow() {
        let too_large = "170141183460469231731687303715884105728";
        assert_eq!(parse_i128(too_large), Err(Overflow(too_large.to_string())));
    }

    #[test]
    fn test_parse_i128_underflow() {
        let too_small = "-170141183460469231731687303715884105729";
        assert_eq!(parse_i128(too_small), Err(Underflow(too_small.to_string())));
    }

    #[test]
    fn test_parse_u128_valid() {
        assert_eq!(parse_u128("0"), Ok(0));
        assert_eq!(parse_u128("123456"), Ok(123456));
        assert_eq!(parse_u128(&u128::MAX.to_string()), Ok(u128::MAX));
    }

    #[test]
    fn test_parse_u128_invalid() {
        assert_eq!(parse_u128("abc"), Err(Invalid("abc".to_string())));
        assert_eq!(parse_u128("1.23"), Err(Invalid("1.23".to_string())));
        assert_eq!(parse_u128(""), Err(Invalid("".to_string())));
    }

    #[test]
    fn test_parse_u128_overflow() {
        let too_large = "340282366920938463463374607431768211456";
        assert_eq!(parse_u128(too_large), Err(Overflow(too_large.to_string())));
    }

    #[test]
    fn test_parse_u128_underflow() {
        let negative = "-1";
        assert_eq!(parse_u128(negative), Err(Underflow(negative.to_string())));
    }
}
