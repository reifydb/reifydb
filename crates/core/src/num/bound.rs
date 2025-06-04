// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::num::is::{IsFloat, IsInt, IsUint};
use std::any::TypeId;

pub fn min_float<T>() -> f64
where
    T: 'static + IsFloat,
{
    if TypeId::of::<T>() == TypeId::of::<f32>() {
        f32::MIN as f64
    } else if TypeId::of::<T>() == TypeId::of::<f64>() {
        f64::MIN
    } else {
        unreachable!()
    }
}

pub fn max_float<T>() -> f64
where
    T: 'static,
{
    if TypeId::of::<T>() == TypeId::of::<f32>() {
        f32::MAX as f64
    } else if TypeId::of::<T>() == TypeId::of::<f64>() {
        f64::MAX
    } else {
        unreachable!()
    }
}

pub fn min_int<T>() -> i128
where
    T: 'static + IsInt,
{
    if TypeId::of::<T>() == TypeId::of::<i8>() {
        i8::MIN as i128
    } else if TypeId::of::<T>() == TypeId::of::<i16>() {
        i16::MIN as i128
    } else if TypeId::of::<T>() == TypeId::of::<i32>() {
        i32::MIN as i128
    } else if TypeId::of::<T>() == TypeId::of::<i64>() {
        i64::MIN as i128
    } else if TypeId::of::<T>() == TypeId::of::<i128>() {
        i128::MIN
    } else {
        unreachable!()
    }
}

pub fn max_int<T>() -> i128
where
    T: 'static + IsInt,
{
    if TypeId::of::<T>() == TypeId::of::<i8>() {
        i8::MAX as i128
    } else if TypeId::of::<T>() == TypeId::of::<i16>() {
        i16::MAX as i128
    } else if TypeId::of::<T>() == TypeId::of::<i32>() {
        i32::MAX as i128
    } else if TypeId::of::<T>() == TypeId::of::<i64>() {
        i64::MAX as i128
    } else if TypeId::of::<T>() == TypeId::of::<i128>() {
        i128::MAX
    } else {
        unreachable!()
    }
}

pub fn max_uint<T>() -> u128
where
    T: 'static + IsUint,
{
    if TypeId::of::<T>() == TypeId::of::<u8>() {
        u8::MAX as u128
    } else if TypeId::of::<T>() == TypeId::of::<u16>() {
        u16::MAX as u128
    } else if TypeId::of::<T>() == TypeId::of::<u32>() {
        u32::MAX as u128
    } else if TypeId::of::<T>() == TypeId::of::<u64>() {
        u64::MAX as u128
    } else if TypeId::of::<T>() == TypeId::of::<u128>() {
        u128::MAX
    } else {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_float() {
        assert_eq!(min_float::<f32>(), f32::MIN as f64);
        assert_eq!(min_float::<f64>(), f64::MIN);
    }

    #[test]
    fn test_max_float() {
        assert_eq!(max_float::<f32>(), f32::MAX as f64);
        assert_eq!(max_float::<f64>(), f64::MAX);
    }

    #[test]
    fn test_min_int() {
        assert_eq!(min_int::<i8>(), i8::MIN as i128);
        assert_eq!(min_int::<i16>(), i16::MIN as i128);
        assert_eq!(min_int::<i32>(), i32::MIN as i128);
        assert_eq!(min_int::<i64>(), i64::MIN as i128);
        assert_eq!(min_int::<i128>(), i128::MIN);
    }

    #[test]
    fn test_max_int() {
        assert_eq!(max_int::<i8>(), i8::MAX as i128);
        assert_eq!(max_int::<i16>(), i16::MAX as i128);
        assert_eq!(max_int::<i32>(), i32::MAX as i128);
        assert_eq!(max_int::<i64>(), i64::MAX as i128);
        assert_eq!(max_int::<i128>(), i128::MAX);
    }

    #[test]
    fn test_max_uint() {
        assert_eq!(max_uint::<u8>(), u8::MAX as u128);
        assert_eq!(max_uint::<u16>(), u16::MAX as u128);
        assert_eq!(max_uint::<u32>(), u32::MAX as u128);
        assert_eq!(max_uint::<u64>(), u64::MAX as u128);
        assert_eq!(max_uint::<u128>(), u128::MAX);
    }
}
