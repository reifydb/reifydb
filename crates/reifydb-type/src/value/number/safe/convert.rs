// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

pub trait SafeConvert<T>: Sized {
	fn checked_convert(self) -> Option<T>;
	fn saturating_convert(self) -> T;
	fn wrapping_convert(self) -> T;
}

macro_rules! impl_safe_convert {
    ($src:ty => $($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for $src {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(self).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(v) = <$dst>::try_from(self) {
                        v
                    } else if self < 0 {
                        0
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self as $dst
                }
            }
        )*
    };
}

macro_rules! impl_safe_unsigned_convert {
    ($src:ty => $($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for $src {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(self).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if self > <$dst>::MAX as $src {
                        <$dst>::MAX
                    }else{
                        self as $dst
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self as $dst
                }
            }
        )*
    };
}

impl_safe_convert!(i8 => u8, u16, u32, u64, u128);
impl_safe_convert!(i16 => u8, u16, u32, u64, u128);
impl_safe_convert!(i32 => u8, u16, u32, u64, u128);
impl_safe_convert!(i64 => u8, u16, u32, u64, u128);
impl_safe_convert!(i128 => u8, u16, u32, u64, u128);

impl_safe_unsigned_convert!(u8 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u16 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u32 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u64 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u128 => i8, i16, i32, i64, i128);

macro_rules! impl_safe_convert_signed_to_float {
    ($mantissa_bits:expr; $src:ty => $($float:ty),* $(,)?) => {
        $(
            impl SafeConvert<$float> for $src {
                fn checked_convert(self) -> Option<$float> {
                    let val = self as i128;
                    let max_exact = 1i128 << $mantissa_bits;
                    if val >= -max_exact && val <= max_exact {
                        Some(self as $float)
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $float {
                    let max_exact = 1i128 << $mantissa_bits;
                    let min = -max_exact;
                    let max = max_exact;
                    let val = self as i128;
                    if val < min {
                        min as $float
                    } else if val > max {
                        max as $float
                    } else {
                        self as $float
                    }
                }

                fn wrapping_convert(self) -> $float {
                    self as $float
                }
            }
        )*
    };
}

impl_safe_convert_signed_to_float!(24;i8 => f32);
impl_safe_convert_signed_to_float!(24;i16 => f32);
impl_safe_convert_signed_to_float!(24;i32 => f32);
impl_safe_convert_signed_to_float!(24;i64 => f32);
impl_safe_convert_signed_to_float!(24;i128 => f32);

impl_safe_convert_signed_to_float!(53;i8 =>  f64);
impl_safe_convert_signed_to_float!(53;i16 => f64);
impl_safe_convert_signed_to_float!(53;i32 => f64);
impl_safe_convert_signed_to_float!(53;i64 =>  f64);
impl_safe_convert_signed_to_float!(53;i128 => f64);

macro_rules! impl_safe_convert_unsigned_to_float {
    ($mantissa_bits:expr; $src:ty => $($float:ty),* $(,)?) => {
        $(
            impl SafeConvert<$float> for $src {
                fn checked_convert(self) -> Option<$float> {
                    if self as u64 <= (1u64 << $mantissa_bits) {
                        Some(self as $float)
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $float {
                    let max_exact = 1u64 << $mantissa_bits;
                    let max = max_exact as u128;
                    let val = self as u128;
                    if val > max {
                           max as $float
                    } else {
                        self as $float
                    }
                }

                fn wrapping_convert(self) -> $float {
                    self as $float
                }
            }
        )*
    };
}

impl_safe_convert_unsigned_to_float!(24;u8 => f32);
impl_safe_convert_unsigned_to_float!(24;u16 => f32);
impl_safe_convert_unsigned_to_float!(24;u32 => f32);
impl_safe_convert_unsigned_to_float!(24;u64 => f32);
impl_safe_convert_unsigned_to_float!(24;u128 => f32);
impl_safe_convert_unsigned_to_float!(53;u8 =>  f64);
impl_safe_convert_unsigned_to_float!(53;u16 => f64);
impl_safe_convert_unsigned_to_float!(53;u32 => f64);
impl_safe_convert_unsigned_to_float!(53;u64 =>  f64);
impl_safe_convert_unsigned_to_float!(53;u128 => f64);

macro_rules! impl_safe_convert_float_to_signed {
    ($src:ty => $($dst:ty),* $(,)?) => {
        $(
            impl SafeConvert<$dst> for $src {
                fn checked_convert(self) -> Option<$dst> {
                    if self.is_nan() || self.is_infinite() {
                        return None;
                    }

                    let min_val = <$dst>::MIN as $src;
                    let max_val = <$dst>::MAX as $src;

                    if self < min_val || self > max_val {
                        None
                    } else {
                        Some(self as $dst)
                    }
                }

                fn saturating_convert(self) -> $dst {
                    if self.is_nan() {
                        return 0;
                    }

                    if self.is_infinite() {
                        return if self.is_sign_positive() { <$dst>::MAX } else { <$dst>::MIN };
                    }

                    let min_val = <$dst>::MIN as $src;
                    let max_val = <$dst>::MAX as $src;

                    if self < min_val {
                        <$dst>::MIN
                    } else if self > max_val {
                        <$dst>::MAX
                    } else {
                        self as $dst
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if self.is_nan() {
                        return 0;
                    }

                    if self.is_infinite() {
                        return if self.is_sign_positive() { <$dst>::MAX } else { <$dst>::MIN };
                    }

                    self as $dst
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_float_to_unsigned {
    ($src:ty => $($dst:ty),* $(,)?) => {
        $(
            impl SafeConvert<$dst> for $src {
                fn checked_convert(self) -> Option<$dst> {
                    if self.is_nan() || self.is_infinite() || self < 0.0 {
                        return None;
                    }

                    let max_val = <$dst>::MAX as $src;

                    if self > max_val {
                        None
                    } else {
                        Some(self as $dst)
                    }
                }

                fn saturating_convert(self) -> $dst {
                    if self.is_nan() || self < 0.0 {
                        return 0;
                    }

                    if self.is_infinite() {
                        return <$dst>::MAX;
                    }

                    let max_val = <$dst>::MAX as $src;

                    if self > max_val {
                        <$dst>::MAX
                    } else {
                        self as $dst
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if self.is_nan() || self < 0.0 {
                        return 0;
                    }

                    if self.is_infinite() {
                        return <$dst>::MAX;
                    }

                    self as $dst
                }
            }
        )*
    };
}

impl_safe_convert_float_to_signed!(f32 => i8, i16, i32, i64, i128);
impl_safe_convert_float_to_signed!(f64 => i8, i16, i32, i64, i128);

impl_safe_convert_float_to_unsigned!(f32 => u8, u16, u32, u64, u128);
impl_safe_convert_float_to_unsigned!(f64 => u8, u16, u32, u64, u128);

use num_bigint::{BigInt, ToBigInt};
use num_traits::{Signed, ToPrimitive};

use crate::{
	Decimal, VarInt, VarUint,
	value::decimal::{Precision, Scale},
};

macro_rules! impl_safe_convert_varint_to_signed {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarInt {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(&self.0).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else if self.0 < BigInt::from(0) {
                        <$dst>::MIN
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varint_to_unsigned {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarInt {
                fn checked_convert(self) -> Option<$dst> {
                    if self.0 >= BigInt::from(0) {
                        <$dst>::try_from(&self.0).ok()
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $dst {
                    if self.0 < BigInt::from(0) {
                        0
                    } else if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if self.0 < BigInt::from(0) {
                        0
                    } else if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varint_to_float {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarInt {
                fn checked_convert(self) -> Option<$dst> {
                    self.0.to_f64().and_then(|f| {
                        if f.is_finite() {
                            Some(f as $dst)
                        } else {
                            None
                        }
                    })
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(f) = self.0.to_f64() {
                        if f.is_finite() {
                            f as $dst
                        } else if f.is_sign_negative() {
                            <$dst>::MIN
                        } else {
                            <$dst>::MAX
                        }
                    } else if self.0 < BigInt::from(0) {
                        <$dst>::MIN
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self.saturating_convert()
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varuint_to_signed {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarUint {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(&self.0).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Ok(val) = u64::try_from(&self.0) {
                        val as $dst
                    } else {
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varuint_to_unsigned {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarUint {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(&self.0).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Ok(val) = u64::try_from(&self.0) {
                        val as $dst
                    } else {
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varuint_to_float {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarUint {
                fn checked_convert(self) -> Option<$dst> {
                    self.0.to_f64().and_then(|f| {
                        if f.is_finite() {
                            Some(f as $dst)
                        } else {
                            None
                        }
                    })
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(f) = self.0.to_f64() {
                        if f.is_finite() {
                            f as $dst
                        } else {
                            <$dst>::MAX
                        }
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self.saturating_convert()
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_decimal_to_int {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Decimal {
                fn checked_convert(self) -> Option<$dst> {
                    if let Some(int_part) = self.inner().to_bigint() {
                        <$dst>::try_from(int_part).ok()
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(int_part) = self.inner().to_bigint() {
                        if let Ok(val) = <$dst>::try_from(&int_part) {
                            val
                        } else if int_part < BigInt::from(0) {
                            <$dst>::MIN
                        } else {
                            <$dst>::MAX
                        }
                    } else {
                        0
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Some(int_part) = self.inner().to_bigint() {
                        if let Ok(val) = <$dst>::try_from(&int_part) {
                            val
                        } else {
                            self.saturating_convert()
                        }
                    } else {
                        0
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_decimal_to_float {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Decimal {
                fn checked_convert(self) -> Option<$dst> {
                    self.inner().to_f64().and_then(|f| {
                        if f.is_finite() {
                            Some(f as $dst)
                        } else {
                            None
                        }
                    })
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(f) = self.inner().to_f64() {
                        if f.is_finite() {
                            f as $dst
                        } else if f.is_sign_negative() {
                            <$dst>::MIN
                        } else {
                            <$dst>::MAX
                        }
                    } else {
                        0.0
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self.saturating_convert()
                }
            }
        )*
    };
}

// Primitive to VarInt/VarUint conversions
macro_rules! impl_safe_convert_to_varint {
    ($($from:ty),*) => {
        $(
            impl SafeConvert<VarInt> for $from {
                fn checked_convert(self) -> Option<VarInt> {
                    Some(VarInt(BigInt::from(self)))
                }

                fn saturating_convert(self) -> VarInt {
                    VarInt(BigInt::from(self))
                }

                fn wrapping_convert(self) -> VarInt {
                    VarInt(BigInt::from(self))
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_signed_to_varuint {
    ($($from:ty),*) => {
        $(
            impl SafeConvert<VarUint> for $from {
                fn checked_convert(self) -> Option<VarUint> {
                    if self >= 0 {
                        Some(VarUint(BigInt::from(self)))
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> VarUint {
                    if self >= 0 {
                        VarUint(BigInt::from(self))
                    } else {
                        VarUint::zero()
                    }
                }

                fn wrapping_convert(self) -> VarUint {
                    VarUint(BigInt::from(self.wrapping_abs() as u64))
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_unsigned_to_varuint {
    ($($from:ty),*) => {
        $(
            impl SafeConvert<VarUint> for $from {
                fn checked_convert(self) -> Option<VarUint> {
                    Some(VarUint(BigInt::from(self)))
                }

                fn saturating_convert(self) -> VarUint {
                    VarUint(BigInt::from(self))
                }

                fn wrapping_convert(self) -> VarUint {
                    VarUint(BigInt::from(self))
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_float_to_varint {
    ($($from:ty),*) => {
        $(
            impl SafeConvert<VarInt> for $from {
                fn checked_convert(self) -> Option<VarInt> {
                    if self.is_finite() {
                        let truncated = self.trunc();
                        // Use ToBigInt trait for efficient conversion
                        truncated.to_bigint().map(VarInt)
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> VarInt {
                    if self.is_nan() {
                        VarInt::zero()
                    } else if self.is_infinite() {
                        if self.is_sign_positive() {
                            VarInt(BigInt::from(i64::MAX))
                        } else {
                            VarInt(BigInt::from(i64::MIN))
                        }
                    } else {
                        let truncated = self.trunc() as i64;
                        VarInt(BigInt::from(truncated))
                    }
                }

                fn wrapping_convert(self) -> VarInt {
                    if self.is_finite() {
                        VarInt(BigInt::from(self.trunc() as i64))
                    } else {
                        VarInt::zero()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_float_to_varuint {
    ($($from:ty),*) => {
        $(
            impl SafeConvert<VarUint> for $from {
                fn checked_convert(self) -> Option<VarUint> {
                    if self.is_finite() && self >= 0.0 {
                        let truncated = self.trunc();
                        // Use ToBigInt trait for efficient conversion
                        truncated.to_bigint().and_then(|big_int| {
                            if big_int >= BigInt::from(0) {
                                Some(VarUint(big_int))
                            } else {
                                None
                            }
                        })
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> VarUint {
                    if self.is_nan() || self < 0.0 {
                        VarUint::zero()
                    } else if self.is_infinite() {
                        VarUint(BigInt::from(u64::MAX))
                    } else {
                        let truncated = self.trunc() as u64;
                        VarUint(BigInt::from(truncated))
                    }
                }

                fn wrapping_convert(self) -> VarUint {
                    if self.is_finite() {
                        VarUint(BigInt::from((self.trunc() as i64).wrapping_abs() as u64))
                    } else {
                        VarUint::zero()
                    }
                }
            }
        )*
    };
}

// Cross-conversions between VarInt, VarUint, and Decimal
impl SafeConvert<VarUint> for VarInt {
	fn checked_convert(self) -> Option<VarUint> {
		if self.0 >= BigInt::from(0) {
			Some(VarUint(self.0))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarUint {
		if self.0 >= BigInt::from(0) {
			VarUint(self.0)
		} else {
			VarUint::zero()
		}
	}

	fn wrapping_convert(self) -> VarUint {
		VarUint(self.0.abs())
	}
}

impl SafeConvert<VarInt> for VarUint {
	fn checked_convert(self) -> Option<VarInt> {
		Some(VarInt(self.0))
	}

	fn saturating_convert(self) -> VarInt {
		VarInt(self.0)
	}

	fn wrapping_convert(self) -> VarInt {
		VarInt(self.0)
	}
}

impl SafeConvert<Decimal> for VarInt {
	fn checked_convert(self) -> Option<Decimal> {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal::new(big_decimal, Precision::new(38), Scale::new(0))
			.ok()
	}

	fn saturating_convert(self) -> Decimal {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal::new(big_decimal, Precision::new(38), Scale::new(0))
			.unwrap_or_else(|_| {
				Decimal::from_i64(0, 38, 0).unwrap()
			})
	}

	fn wrapping_convert(self) -> Decimal {
		self.saturating_convert()
	}
}

impl SafeConvert<Decimal> for VarUint {
	fn checked_convert(self) -> Option<Decimal> {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal::new(big_decimal, Precision::new(38), Scale::new(0))
			.ok()
	}

	fn saturating_convert(self) -> Decimal {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal::new(big_decimal, Precision::new(38), Scale::new(0))
			.unwrap_or_else(|_| {
				Decimal::from_i64(0, 38, 0).unwrap()
			})
	}

	fn wrapping_convert(self) -> Decimal {
		self.saturating_convert()
	}
}

impl SafeConvert<VarInt> for Decimal {
	fn checked_convert(self) -> Option<VarInt> {
		if let Some(big_int) = self.inner().to_bigint() {
			Some(VarInt(big_int))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarInt {
		self.checked_convert().unwrap_or(VarInt::zero())
	}

	fn wrapping_convert(self) -> VarInt {
		self.saturating_convert()
	}
}

impl SafeConvert<VarUint> for Decimal {
	fn checked_convert(self) -> Option<VarUint> {
		if let Some(big_int) = self.inner().to_bigint() {
			if big_int >= BigInt::from(0) {
				Some(VarUint(big_int))
			} else {
				None
			}
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarUint {
		if let Some(big_int) = self.inner().to_bigint() {
			if big_int >= BigInt::from(0) {
				VarUint(big_int)
			} else {
				VarUint::zero()
			}
		} else {
			VarUint::zero()
		}
	}

	fn wrapping_convert(self) -> VarUint {
		if let Some(big_int) = self.inner().to_bigint() {
			VarUint(big_int.abs())
		} else {
			VarUint::zero()
		}
	}
}

// Apply all the macro implementations
impl_safe_convert_varint_to_signed!(i8, i16, i32, i64, i128);
impl_safe_convert_varint_to_unsigned!(u8, u16, u32, u64, u128);
impl_safe_convert_varint_to_float!(f32, f64);

impl_safe_convert_varuint_to_signed!(i8, i16, i32, i64, i128);
impl_safe_convert_varuint_to_unsigned!(u8, u16, u32, u64, u128);
impl_safe_convert_varuint_to_float!(f32, f64);

impl_safe_convert_decimal_to_int!(
	i8, i16, i32, i64, i128, u8, u16, u32, u64, u128
);
impl_safe_convert_decimal_to_float!(f32, f64);

impl_safe_convert_to_varint!(i8, i16, i32, i64, i128);
impl_safe_convert_signed_to_varuint!(i8, i16, i32, i64, i128);
impl_safe_convert_unsigned_to_varuint!(u8, u16, u32, u64, u128);
impl_safe_convert_float_to_varint!(f32, f64);
impl_safe_convert_float_to_varuint!(f32, f64);

#[cfg(test)]
mod tests {
	mod i8_to_u8 {
		use crate::value::number::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u8 = SafeConvert::saturating_convert(x);
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}
	mod i8_to_u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}
	mod i8_to_u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}
	mod i8_to_u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}
	mod i8_to_u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(
				y,
				340282366920938463463374607431768211455u128
			);
		}
	}
	mod i8_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i8 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}
	}
	mod i8_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i8 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}
	}

	mod i16_to_u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i16 = 42;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i16 = -1;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = -1;
			let y: u8 = SafeConvert::saturating_convert(x);
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}
	mod i16_to_u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i16 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i16 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}
	mod i16_to_u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i16 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i16 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}
	mod i16_to_u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i16 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i16 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}
	mod i16_to_u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i16 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i16 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(
				y,
				340282366920938463463374607431768211455u128
			);
		}
	}
	mod i16_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i16 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}
	}
	mod i16_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i16 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}
	}

	mod i32_to_u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u8 = SafeConvert::saturating_convert(x);
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}
	mod i32_to_u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}
	mod i32_to_u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}
	mod i32_to_u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}
	mod i32_to_u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(
				y,
				340282366920938463463374607431768211455u128
			);
		}
	}
	mod i32_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i32 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}
	}
	mod i32_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i32 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}
	}

	mod i64_to_u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i64 = 42;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i64 = -1;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = -1;
			let y: u8 = SafeConvert::saturating_convert(x);
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}
	mod i64_to_u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i64 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i64 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}
	mod i64_to_u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i64 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i64 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}
	mod i64_to_u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i64 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i64 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}
	mod i64_to_u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i64 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i64 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(
				y,
				340282366920938463463374607431768211455u128
			);
		}
	}
	mod i64_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i64 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: i64 = i64::MAX;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: i64 = i64::MAX;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, (1i128 << 24) as f32);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: i64 = i64::MIN;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, -(1i128 << 24) as f32);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: i64 = i64::MAX;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, i64::MAX as f32);
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: i64 = i64::MIN;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, i64::MIN as f32);
		}
	}
	mod i64_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i64 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: i64 = i64::MAX;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: i64 = i64::MAX;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, (1i128 << 53) as f64);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: i64 = i64::MIN;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, -(1i128 << 53) as f64);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: i64 = i64::MAX;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, i64::MAX as f64);
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: i64 = i64::MIN;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, i64::MIN as f64);
		}
	}

	mod i128_to_u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u8 = SafeConvert::saturating_convert(x);
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}
	mod i128_to_u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}
	mod i128_to_u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}
	mod i128_to_u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}
	mod i128_to_u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(
				y,
				340282366920938463463374607431768211455u128
			);
		}
	}
	mod i128_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i128 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None); // too large for f32's exact range
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, (1i64 << 24) as f32);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, -(1i64 << 24) as f32);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f32 = x.wrapping_convert();
			assert!(y.is_finite());
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f32 = x.wrapping_convert();
			assert!(y.is_finite());
		}
	}
	mod i128_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i128 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, (1i128 << 53) as f64);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, -(1i128 << 53) as f64);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f64 = x.wrapping_convert();
			assert!(y.is_finite());
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f64 = x.wrapping_convert();
			assert!(y.is_finite());
		}
	}

	mod u8_to_i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u8 = 255;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 255;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 127i8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 255;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, -1i8);
		}
	}
	mod u8_to_i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 255;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, 255i16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 255;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, 255i16);
		}
	}
	mod u8_to_i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 255;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, 255i32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 255;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 255i32);
		}
	}
	mod u8_to_i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 255;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, 255i64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 255;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 255i64);
		}
	}
	mod u8_to_i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 255;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, 255i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 255;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 255i128);
		}
	}
	mod u8_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u8 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 1.0f32);
		}
	}
	mod u8_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u8 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 1.0f64);
		}
	}

	mod u16_to_i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u16 = 300;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 300;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 127i8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 300;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 44i8);
		}
	}
	mod u16_to_i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u16 = 40000;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 40000;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, 32767i16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 40000;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -25536i16);
		}
	}
	mod u16_to_i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 65535;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, 65535i32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 65535;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 65535i32);
		}
	}
	mod u16_to_i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 65535;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, 65535i64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 65535;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 65535i64);
		}
	}
	mod u16_to_i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 65535;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, 65535i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 65535;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 65535i128);
		}
	}
	mod u16_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u16 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 1.0f32);
		}
	}
	mod u16_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u16 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 1.0f64);
		}
	}

	mod u32_to_i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u32 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u32 = 300;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = 300;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 127i8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = 300;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 44i8);
		}
	}
	mod u32_to_i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u32 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u32 = 40000;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = 40000;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, 32767i16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = 40000;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -25536i16);
		}
	}
	mod u32_to_i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u32 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u32 = i32::MAX as u32 + 1;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = i32::MAX as u32 + 1;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = i32::MAX as u32 + 1;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, i32::MIN);
		}
	}
	mod u32_to_i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u32 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = u32::MAX;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, u32::MAX as i64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = u32::MAX;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, u32::MAX as i64);
		}
	}
	mod u32_to_i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u32 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = u32::MAX;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, u32::MAX as i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = u32::MAX;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, u32::MAX as i128);
		}
	}
	mod u32_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u32 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = 1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 1.0f32);
		}
	}
	mod u32_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u32 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = 1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 1.0f64);
		}
	}

	mod u64_to_i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 300;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 300;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 127i8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 300;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 44i8);
		}
	}
	mod u64_to_i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 40000;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 40000;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, 32767i16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 40000;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -25536i16);
		}
	}
	mod u64_to_i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = i32::MAX as u64 + 1;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = i32::MAX as u64 + 1;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = i32::MAX as u64 + 1;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, i32::MIN);
		}
	}
	mod u64_to_i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = i64::MAX as u64 + 1;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = i64::MAX as u64 + 1;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = i64::MAX as u64 + 1;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, i64::MIN);
		}
	}
	mod u64_to_i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = u64::MAX;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, u64::MAX as i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = u64::MAX;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, u64::MAX as i128);
		}
	}
	mod u64_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u64 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 1.0f32);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, (1u128 << 24) as f32);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: u64 = u64::MIN;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 0.0f32);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, u64::MAX as f32);
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: u64 = u64::MIN;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, u64::MIN as f32);
		}
	}
	mod u64_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u64 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 0;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 0.0f64);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, (1i128 << 53) as f64);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: u64 = u64::MIN;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 0.0f64);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, u64::MAX as f64);
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: u64 = u64::MIN;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, u64::MIN as f64);
		}
	}

	mod u128_to_i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u128 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u128 = 300;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = 300;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 127i8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = 300;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 44i8);
		}
	}
	mod u128_to_i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u128 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u128 = 40000;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = 40000;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, 32767i16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = 40000;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -25536i16);
		}
	}
	mod u128_to_i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u128 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u128 = i32::MAX as u128 + 1;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = i32::MAX as u128 + 1;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = i32::MAX as u128 + 1;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, i32::MIN);
		}
	}
	mod u128_to_i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u128 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u128 = i64::MAX as u128 + 1;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = i64::MAX as u128 + 1;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = i64::MAX as u128 + 1;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, i64::MIN);
		}
	}
	mod u128_to_i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: u128 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u128 = i128::MAX as u128 + 1;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = i128::MAX as u128 + 1;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = i128::MAX as u128 + 1;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, i128::MIN);
		}
	}
	mod u128_to_f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u128 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = 1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 1.0f32);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: u128 = u128::MAX;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: u128 = u128::MAX;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, (1u128 << 24) as f32);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: u128 = u128::MIN;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 0.0f32);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: u128 = u128::MAX;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, u128::MAX as f32);
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: u128 = u128::MIN;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, u128::MIN as f32);
		}
	}
	mod u128_to_f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: u128 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = 0;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 0.0f64);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: u128 = u128::MAX;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: u128 = u128::MAX;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, (1i128 << 53) as f64);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: u128 = u128::MIN;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 0.0f64);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: u128 = u128::MAX;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, u128::MAX as f64);
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: u128 = u128::MIN;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, u128::MIN as f64);
		}
	}

	mod f32_to_i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 300.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(-42i8));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f32 = f32::NAN;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_neg_infinity() {
			let x: f32 = f32::NEG_INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 42i8);
		}

		#[test]
		fn test_wrapping_convert_nan() {
			let x: f32 = f32::NAN;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod f32_to_i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 40000.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(-42i16));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, 42i16);
		}
	}

	mod f32_to_i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(-42i32));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 42i32);
		}
	}

	mod f32_to_i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(-42i64));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -3e38;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 42i64);
		}
	}

	mod f32_to_i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(-42i128));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -3e38;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 42i128);
		}
	}

	mod f32_to_u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 300.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 300.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f32 = f32::NAN;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 42u8);
		}

		#[test]
		fn test_wrapping_convert_negative() {
			let x: f32 = -42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod f32_to_u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 70000.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 70000.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, u16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 42u16);
		}
	}

	mod f32_to_u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 42u32);
		}
	}

	mod f32_to_u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, u64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 42u64);
		}
	}

	mod f32_to_u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 42u128);
		}
	}

	mod f64_to_i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 300.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(-42i8));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f64 = f64::NAN;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_neg_infinity() {
			let x: f64 = f64::NEG_INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 42i8);
		}

		#[test]
		fn test_wrapping_convert_nan() {
			let x: f64 = f64::NAN;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod f64_to_i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 40000.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(-42i16));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, 42i16);
		}
	}

	mod f64_to_i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 3e38;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(-42i32));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 42i32);
		}
	}

	mod f64_to_i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(-42i64));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -1e300;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 42i64);
		}
	}

	mod f64_to_i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(-42i128));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -1e300;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 42i128);
		}
	}

	mod f64_to_u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 300.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 300.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f64 = f64::NAN;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 42u8);
		}

		#[test]
		fn test_wrapping_convert_negative() {
			let x: f64 = -42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod f64_to_u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 70000.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 70000.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, u16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 42u16);
		}
	}

	mod f64_to_u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 42u32);
		}
	}

	mod f64_to_u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, u64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 42u64);
		}
	}

	mod f64_to_u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, u128::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 42u128);
		}
	}

	mod i32_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_positive() {
			let x: i32 = 42;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i32 = -42;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(-42))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = i32::MAX;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y, VarInt(BigInt::from(i32::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y, VarInt(BigInt::from(-1)));
		}
	}

	mod f64_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_truncated() {
			let x: f64 = 42.7;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.5;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(-42))));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f64 = f64::NAN;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y, VarInt::zero());
		}
	}

	mod varint_to_i32 {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(42));
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarInt(BigInt::from(i64::MAX));
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarInt(BigInt::from(i64::MAX));
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(42));
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 42i32);
		}
	}

	mod u32_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: u32 = 42;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(42))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u32 = u32::MAX;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint(BigInt::from(u32::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u32 = 42;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(42)));
		}
	}

	mod i32_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_positive() {
			let x: i32 = 42;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i32 = -42;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x: i32 = -42;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}

		#[test]
		fn test_wrapping_convert_positive() {
			let x: i32 = 42;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(42)));
		}
	}

	mod f64_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x: f64 = -42.0;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}
	}

	mod varint_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarInt, VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_positive() {
			let x = VarInt(BigInt::from(42));
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x = VarInt(BigInt::from(-42));
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x = VarInt(BigInt::from(-42));
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(42));
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(42)));
		}
	}

	mod varint_to_decimal {
		use num_bigint::BigInt;

		use crate::{Decimal, VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(42));
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt(BigInt::from(-42));
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "-42");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(12345));
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "12345");
		}
	}

	mod varuint_to_decimal {
		use num_bigint::BigInt;

		use crate::{Decimal, VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(42));
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint(BigInt::from(12345));
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "12345");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(67890));
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "67890");
		}
	}

	mod decimal_to_varint {
		use num_bigint::BigInt;

		use crate::{Decimal, VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = Decimal::from_i64(42, 38, 0).unwrap();
			let y: Option<VarInt> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().0, BigInt::from(42));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from_i64(-42, 38, 0).unwrap();
			let y: VarInt = x.saturating_convert();
			assert_eq!(y.0, BigInt::from(-42));
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Decimal::from_i64(12345, 38, 0).unwrap();
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y.0, BigInt::from(12345));
		}
	}

	mod i8_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i8 = -42;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(-42))));
		}

		#[test]
		fn test_saturating_convert_max() {
			let x: i8 = i8::MAX;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y, VarInt(BigInt::from(i8::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = i8::MIN;
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y, VarInt(BigInt::from(i8::MIN)));
		}
	}

	mod i16_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: i16 = 1000;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(1000))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i16 = i16::MAX;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y, VarInt(BigInt::from(i16::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i16 = -1000;
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y, VarInt(BigInt::from(-1000)));
		}
	}

	mod i64_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: i64 = 1000000;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(1000000))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i64 = i64::MAX;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y, VarInt(BigInt::from(i64::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i64 = i64::MIN;
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y, VarInt(BigInt::from(i64::MIN)));
		}
	}

	mod i128_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 1000000000;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(1000000000))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = i128::MAX;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y, VarInt(BigInt::from(i128::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = i128::MIN;
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y, VarInt(BigInt::from(i128::MIN)));
		}
	}

	mod f32_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_truncated() {
			let x: f32 = 42.7;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: VarInt = x.saturating_convert();
			assert_eq!(
				y,
				VarInt::from_i128(9223372036854775807i128)
			);
		}
	}

	mod varint_to_i8 {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(42));
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarInt(BigInt::from(1000));
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarInt(BigInt::from(1000));
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x = VarInt(BigInt::from(-1000));
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}
	}

	mod varint_to_i16 {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(1000));
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(1000i16));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarInt(BigInt::from(100000));
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarInt(BigInt::from(100000));
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(1000));
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, 1000i16);
		}
	}

	mod varint_to_i64 {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(1000000));
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(1000000i64));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarInt(BigInt::from(i128::MAX));
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarInt(BigInt::from(i128::MAX));
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(-1000000));
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, -1000000i64);
		}
	}

	mod varint_to_i128 {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(i128::MAX));
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(i128::MAX));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarInt(BigInt::from(i128::MAX) + 1);
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarInt(BigInt::from(i128::MAX) + 1);
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(i128::MIN));
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, i128::MIN);
		}
	}

	mod varint_to_f32 {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(42));
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt(BigInt::from(-1000));
			let y: f32 = x.saturating_convert();
			assert_eq!(y, -1000.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(123));
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 123.0f32);
		}
	}

	mod varint_to_f64 {
		use num_bigint::BigInt;

		use crate::{VarInt, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarInt(BigInt::from(1000000));
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(1000000.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt(BigInt::from(-1000000));
			let y: f64 = x.saturating_convert();
			assert_eq!(y, -1000000.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt(BigInt::from(12345));
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 12345.0f64);
		}
	}

	// Additional unsigned primitive to VarUint conversions
	mod u8_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(42))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = u8::MAX;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint(BigInt::from(u8::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 123;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(123)));
		}
	}

	mod u16_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 1000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(1000))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint(BigInt::from(u16::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 12345;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(12345)));
		}
	}

	mod u64_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 1000000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(1000000))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = u64::MAX;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint(BigInt::from(u64::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 123456789;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(123456789)));
		}
	}

	mod u128_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: u128 = 1000000000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(1000000000))));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u128 = u128::MAX;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint(BigInt::from(u128::MAX)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u128 = 12345678901234567890;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(
				y,
				VarUint(BigInt::from(12345678901234567890u128))
			);
		}
	}

	mod i8_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_positive() {
			let x: i8 = 42;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i8 = -42;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x: i8 = -42;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}

		#[test]
		fn test_wrapping_convert_positive() {
			let x: i8 = 42;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(42)));
		}
	}

	mod i16_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_positive() {
			let x: i16 = 1000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(1000))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i16 = -1000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x: i16 = -1000;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}

		#[test]
		fn test_wrapping_convert_positive() {
			let x: i16 = 12345;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(12345)));
		}
	}

	mod i64_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_positive() {
			let x: i64 = 1000000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(1000000))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i64 = -1000000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x: i64 = -1000000;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}

		#[test]
		fn test_wrapping_convert_positive() {
			let x: i64 = 123456789;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y, VarUint(BigInt::from(123456789)));
		}
	}

	mod i128_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_positive() {
			let x: i128 = 1000000000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(1000000000))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i128 = -1000000000;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x: i128 = -1000000000;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}

		#[test]
		fn test_wrapping_convert_positive() {
			let x: i128 = 12345678901234567890;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(
				y,
				VarUint(BigInt::from(12345678901234567890i128))
			);
		}
	}

	mod f32_to_varuint {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, Some(VarUint(BigInt::from(42))));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<VarUint> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_negative() {
			let x: f32 = -42.0;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y, VarUint::zero());
		}
	}

	// VarUint to primitive conversions
	mod varuint_to_u8 {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(42));
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarUint(BigInt::from(1000));
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarUint(BigInt::from(1000));
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(123));
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 123u8);
		}
	}

	mod varuint_to_u16 {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(1000));
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(1000u16));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarUint(BigInt::from(100000));
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarUint(BigInt::from(100000));
			let y: u16 = x.saturating_convert();
			assert_eq!(y, u16::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(12345));
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 12345u16);
		}
	}

	mod varuint_to_u64 {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(1000000));
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(1000000u64));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarUint(BigInt::from(u128::MAX));
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarUint(BigInt::from(u128::MAX));
			let y: u64 = x.saturating_convert();
			assert_eq!(y, u64::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(123456789));
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 123456789u64);
		}
	}

	mod varuint_to_u128 {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(u128::MAX));
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(u128::MAX));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarUint(BigInt::from(u128::MAX) + 1);
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarUint(BigInt::from(u128::MAX) + 1);
			let y: u128 = x.saturating_convert();
			assert_eq!(y, u128::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(12345678901234567890u128));
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 12345678901234567890u128);
		}
	}

	mod varuint_to_i8 {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(42));
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarUint(BigInt::from(1000));
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x = VarUint(BigInt::from(1000));
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(100));
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 100i8);
		}
	}

	mod varuint_to_f32 {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(42));
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint(BigInt::from(1000));
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 1000.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(123));
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 123.0f32);
		}
	}

	mod varuint_to_f64 {
		use num_bigint::BigInt;

		use crate::{VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(1000000));
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(1000000.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint(BigInt::from(1000000));
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 1000000.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(12345));
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 12345.0f64);
		}
	}

	// VarUint to VarInt conversion
	mod varuint_to_varint {
		use num_bigint::BigInt;

		use crate::{VarInt, VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = VarUint(BigInt::from(42));
			let y: Option<VarInt> = x.checked_convert();
			assert_eq!(y, Some(VarInt(BigInt::from(42))));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint(BigInt::from(12345));
			let y: VarInt = x.saturating_convert();
			assert_eq!(y, VarInt(BigInt::from(12345)));
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint(BigInt::from(67890));
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y, VarInt(BigInt::from(67890)));
		}
	}

	// Additional Decimal conversion tests
	mod decimal_to_varuint {
		use num_bigint::BigInt;

		use crate::{Decimal, VarUint, value::number::SafeConvert};

		#[test]
		fn test_checked_convert_happy() {
			let x = Decimal::from_i64(42, 38, 0).unwrap();
			let y: Option<VarUint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().0, BigInt::from(42));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from_i64(12345, 38, 0).unwrap();
			let y: VarUint = x.saturating_convert();
			assert_eq!(y.0, BigInt::from(12345));
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Decimal::from_i64(67890, 38, 0).unwrap();
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y.0, BigInt::from(67890));
		}
	}
}
