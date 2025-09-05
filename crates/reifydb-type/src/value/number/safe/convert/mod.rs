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

// Module declarations
mod decimal;
mod f32;
mod f64;
mod i128;
mod i16;
mod i32;
mod i64;
mod i8;
mod u128;
mod u16;
mod u32;
mod u64;
mod u8;
mod varint;
mod varuint;
