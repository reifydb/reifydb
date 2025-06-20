// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(rustfmt, rustfmt_skip)]

use crate::num::is::IsNumber;

pub trait Promote<Rhs> where Self: IsNumber, Rhs: IsNumber {
    type Output: IsNumber;
    fn promote(self, rhs: Rhs) -> (Self::Output, Self::Output);
}

macro_rules! impl_promote {
    ($lhs:ty, $rhs:ty => $common:ty) => {
        impl Promote<$rhs> for $lhs {
            type Output = $common;
            fn promote(self, rhs: $rhs) -> (Self::Output, Self::Output) {
                (self as $common, rhs as $common)
            }
        }
    };
}

impl_promote!(f32, f32 => f32);
impl_promote!(f32, f64 => f64); impl_promote!(f64, f32 => f64);
impl_promote!(f64, f64 => f64);

// float - signed

impl_promote!(f32, i8 => f64); impl_promote!(i8, f32 => f64);
impl_promote!(f32, i16 => f64); impl_promote!(i16, f32 => f64);
impl_promote!(f32, i32 => f64); impl_promote!(i32, f32 => f64);
impl_promote!(f32, i64 => f64); impl_promote!(i64, f32 => f64);
impl_promote!(f32, i128 => f64); impl_promote!(i128, f32 => f64);

impl_promote!(f64, i8 => f64); impl_promote!(i8, f64 => f64);
impl_promote!(f64, i16 => f64); impl_promote!(i16, f64 => f64);
impl_promote!(f64, i32 => f64); impl_promote!(i32, f64 => f64);
impl_promote!(f64, i64 => f64); impl_promote!(i64, f64 => f64);
impl_promote!(f64, i128 => f64); impl_promote!(i128, f64 => f64);

// float - unsigned

impl_promote!(f32, u8 => f64); impl_promote!(u8, f32 => f64);
impl_promote!(f32, u16 => f64); impl_promote!(u16, f32 => f64);
impl_promote!(f32, u32 => f64); impl_promote!(u32, f32 => f64);
impl_promote!(f32, u64 => f64); impl_promote!(u64, f32 => f64);
impl_promote!(f32, u128 => f64); impl_promote!(u128, f32 => f64);

impl_promote!(f64, u8 => f64); impl_promote!(u8, f64 => f64);
impl_promote!(f64, u16 => f64); impl_promote!(u16, f64 => f64);
impl_promote!(f64, u32 => f64); impl_promote!(u32, f64 => f64);
impl_promote!(f64, u64 => f64); impl_promote!(u64, f64 => f64);
impl_promote!(f64, u128 => f64); impl_promote!(u128, f64 => f64);

// signed - signed 
impl_promote!(i8, i8 => i8);
impl_promote!(i8, i16 => i16); impl_promote!(i16, i8 => i16);
impl_promote!(i8, i32 => i32); impl_promote!(i32, i8 => i32);
impl_promote!(i8, i64 => i64); impl_promote!(i64, i8 => i64);
impl_promote!(i8, i128 => i128); impl_promote!(i128, i8 => i128);

impl_promote!(i16, i16 => i16);
impl_promote!(i16, i32 => i32); impl_promote!(i32, i16 => i32);
impl_promote!(i16, i64 => i64); impl_promote!(i64, i16 => i64);
impl_promote!(i16, i128 => i128); impl_promote!(i128, i16 => i128);

impl_promote!(i32, i32 => i32);
impl_promote!(i32, i64 => i64); impl_promote!(i64, i32 => i64);
impl_promote!(i32, i128 => i128); impl_promote!(i128, i32 => i128);

impl_promote!(i64, i64 => i64);
impl_promote!(i64, i128 => i128); impl_promote!(i128, i64 => i128);

impl_promote!(i128, i128 => i128);

// unsigned - unsigned
impl_promote!(u8, u8 => u8);
impl_promote!(u8, u16 => u16); impl_promote!(u16, u8 => u16);
impl_promote!(u8, u32 => u32); impl_promote!(u32, u8 => u32);
impl_promote!(u8, u64 => u64); impl_promote!(u64, u8 => u64);
impl_promote!(u8, u128 => u128); impl_promote!(u128, u8 => u128);

impl_promote!(u16, u16 => u16);
impl_promote!(u16, u32 => u32); impl_promote!(u32, u16 => u32);
impl_promote!(u16, u64 => u64); impl_promote!(u64, u16 => u64);
impl_promote!(u16, u128 => u128); impl_promote!(u128, u16 => u128);

impl_promote!(u32, u32 => u32);
impl_promote!(u32, u64 => u64); impl_promote!(u64, u32 => u64);
impl_promote!(u32, u128 => u128); impl_promote!(u128, u32 => u128);

impl_promote!(u64, u64 => u64);
impl_promote!(u64, u128 => u128); impl_promote!(u128, u64 => u128);

impl_promote!(u128, u128 => u128);


// 8 signed <-> unsinged
impl_promote!(i8, u8 => i128);    impl_promote!(u8, i8 => i128);
impl_promote!(i8, u16 => i128);   impl_promote!(u16, i8 => i128);
impl_promote!(i8, u32 => i128);   impl_promote!(u32, i8 => i128);
impl_promote!(i8, u64 => i128);   impl_promote!(u64, i8 => i128);
impl_promote!(i8, u128 => i128);  impl_promote!(u128, i8 => i128);

// 16 signed <-> unsinged
impl_promote!(i16, u8 => i128);   impl_promote!(u8, i16 => i128);
impl_promote!(i16, u16 => i128);  impl_promote!(u16, i16 => i128);
impl_promote!(i16, u32 => i128);  impl_promote!(u32, i16 => i128);
impl_promote!(i16, u64 => i128);  impl_promote!(u64, i16 => i128);
impl_promote!(i16, u128 => i128); impl_promote!(u128, i16 => i128);

// 32 signed <-> unsinged
impl_promote!(i32, u8 => i128);   impl_promote!(u8, i32 => i128);
impl_promote!(i32, u16 => i128);  impl_promote!(u16, i32 => i128);
impl_promote!(i32, u32 => i128);  impl_promote!(u32, i32 => i128);
impl_promote!(i32, u64 => i128);  impl_promote!(u64, i32 => i128);
impl_promote!(i32, u128 => i128); impl_promote!(u128, i32 => i128);

// 64 signed <-> unsinged
impl_promote!(i64, u8 => i128);   impl_promote!(u8, i64 => i128);
impl_promote!(i64, u16 => i128);  impl_promote!(u16, i64 => i128);
impl_promote!(i64, u32 => i128);  impl_promote!(u32, i64 => i128);
impl_promote!(i64, u64 => i128);  impl_promote!(u64, i64 => i128);
impl_promote!(i64, u128 => i128); impl_promote!(u128, i64 => i128);

// 128 signed <-> unsinged
impl_promote!(i128, u8 => i128);   impl_promote!(u8, i128 => i128);
impl_promote!(i128, u16 => i128);  impl_promote!(u16, i128 => i128);
impl_promote!(i128, u32 => i128);  impl_promote!(u32, i128 => i128);
impl_promote!(i128, u64 => i128);  impl_promote!(u64, i128 => i128);
impl_promote!(i128, u128 => i128); impl_promote!(u128, i128 => i128);