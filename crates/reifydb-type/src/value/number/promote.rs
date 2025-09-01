// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

#![cfg_attr(rustfmt, rustfmt_skip)]

use crate::value::is::IsNumber;

pub trait Promote<R> where Self: IsNumber, R: IsNumber {
    type Output: IsNumber;
    fn checked_promote(self, r: R) -> Option<(Self::Output, Self::Output)>;
    fn saturating_promote(self, r: R) -> (Self::Output, Self::Output);
    fn wrapping_promote(self, r: R) -> (Self::Output, Self::Output);

}

macro_rules! impl_promote_float_float {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(self, r: $r) -> Option<(Self::Output, Self::Output)> {
                if self.is_finite() && r.is_finite() {
                    Some((self as $common, r as $common))
                } else {
                    None
                }
            }

            fn saturating_promote(self, r: $r) -> (Self::Output, Self::Output) {
                let l = if self.is_finite() {
                    self as $common
                } else if self.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };
                let r = if r.is_finite() {
                    r as $common
                } else if r.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };
                (l, r)
            }

            fn wrapping_promote(self, r: $r) -> (Self::Output, Self::Output) {
                (self as $common, r as $common)
            }
        }
    };
}

macro_rules! impl_promote_float_integer {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(self, r: $r) -> Option<(Self::Output, Self::Output)> {
                if self.is_finite() {
                    Some((self as $common, r as $common))
                } else {
                    None
                }
            }

            fn saturating_promote(self, r: $r) -> (Self::Output, Self::Output) {
                let l = if self.is_finite() {
                    self as $common
                } else if self.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };

                let r = r as $common;
                (l, r)
            }

            fn wrapping_promote(self, r: $r) -> (Self::Output, Self::Output) {
                (self as $common, r as $common)
            }
        }
    };
}


macro_rules! impl_promote_integer_float {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(self, r: $r) -> Option<(Self::Output, Self::Output)> {
                if r.is_finite() {
                    Some((self as $common, r as $common))
                } else {
                    None
                }
            }

            fn saturating_promote(self, r: $r) -> (Self::Output, Self::Output) {
                let l = self as $common;
                let r = if r.is_finite() {
                    r as $common
                } else if r.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };
                (l, r)
            }

            fn wrapping_promote(self, r: $r) -> (Self::Output, Self::Output) {
                (self as $common, r as $common)
            }
        }
    };
}



impl_promote_float_float!(f32, f32 => f64);
impl_promote_float_float!(f32, f64 => f64); impl_promote_float_float!(f64, f32 => f64);
impl_promote_float_float!(f64, f64 => f64);

// float - signed

impl_promote_float_integer!(f32, i8 => f64); impl_promote_integer_float!(i8, f32 => f64);
impl_promote_float_integer!(f32, i16 => f64); impl_promote_integer_float!(i16, f32 => f64);
impl_promote_float_integer!(f32, i32 => f64); impl_promote_integer_float!(i32, f32 => f64);
impl_promote_float_integer!(f32, i64 => f64); impl_promote_integer_float!(i64, f32 => f64);
impl_promote_float_integer!(f32, i128 => f64); impl_promote_integer_float!(i128, f32 => f64);

impl_promote_float_integer!(f64, i8 => f64); impl_promote_integer_float!(i8, f64 => f64);
impl_promote_float_integer!(f64, i16 => f64); impl_promote_integer_float!(i16, f64 => f64);
impl_promote_float_integer!(f64, i32 => f64); impl_promote_integer_float!(i32, f64 => f64);
impl_promote_float_integer!(f64, i64 => f64); impl_promote_integer_float!(i64, f64 => f64);
impl_promote_float_integer!(f64, i128 => f64); impl_promote_integer_float!(i128, f64 => f64);

// float - unsigned

impl_promote_float_integer!(f32, u8 => f64); impl_promote_integer_float!(u8, f32 => f64);
impl_promote_float_integer!(f32, u16 => f64); impl_promote_integer_float!(u16, f32 => f64);
impl_promote_float_integer!(f32, u32 => f64); impl_promote_integer_float!(u32, f32 => f64);
impl_promote_float_integer!(f32, u64 => f64); impl_promote_integer_float!(u64, f32 => f64);
impl_promote_float_integer!(f32, u128 => f64); impl_promote_integer_float!(u128, f32 => f64);

impl_promote_float_integer!(f64, u8 => f64); impl_promote_integer_float!(u8, f64 => f64);
impl_promote_float_integer!(f64, u16 => f64); impl_promote_integer_float!(u16, f64 => f64);
impl_promote_float_integer!(f64, u32 => f64); impl_promote_integer_float!(u32, f64 => f64);
impl_promote_float_integer!(f64, u64 => f64); impl_promote_integer_float!(u64, f64 => f64);
impl_promote_float_integer!(f64, u128 => f64); impl_promote_integer_float!(u128, f64 => f64);

// signed - signed 
macro_rules! impl_promote_signed_signed {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(self, r: $r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(self).ok();
                let r: Option<$common> = <$common>::try_from(r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(self, r: $r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(self) {
                    Ok(v) => v,
                    Err(_) => if self < 0 { <$common>::MIN } else { <$common>::MAX }};
                let r = match <$common>::try_from(r) {
                    Ok(v) => v,
                    Err(_) => if r < 0 { <$common>::MIN } else { <$common>::MAX }};
                (l, r)
            }

            fn wrapping_promote(self, r: $r) -> (Self::Output, Self::Output) {
                (self as $common, r as $common)
            }
        }
    };
}
impl_promote_signed_signed!(i8, i8 => i128);
impl_promote_signed_signed!(i8, i16 => i128); impl_promote_signed_signed!(i16, i8 => i128);
impl_promote_signed_signed!(i8, i32 => i128); impl_promote_signed_signed!(i32, i8 => i128);
impl_promote_signed_signed!(i8, i64 => i128); impl_promote_signed_signed!(i64, i8 => i128);
impl_promote_signed_signed!(i8, i128 => i128); impl_promote_signed_signed!(i128, i8 => i128);

impl_promote_signed_signed!(i16, i16 => i128);
impl_promote_signed_signed!(i16, i32 => i128); impl_promote_signed_signed!(i32, i16 => i128);
impl_promote_signed_signed!(i16, i64 => i128); impl_promote_signed_signed!(i64, i16 => i128);
impl_promote_signed_signed!(i16, i128 => i128); impl_promote_signed_signed!(i128, i16 => i128);

impl_promote_signed_signed!(i32, i32 => i128);
impl_promote_signed_signed!(i32, i64 => i128); impl_promote_signed_signed!(i64, i32 => i128);
impl_promote_signed_signed!(i32, i128 => i128); impl_promote_signed_signed!(i128, i32 => i128);

impl_promote_signed_signed!(i64, i64 => i128);
impl_promote_signed_signed!(i64, i128 => i128); impl_promote_signed_signed!(i128, i64 => i128);

impl_promote_signed_signed!(i128, i128 => i128);

macro_rules! impl_promote_unsigned_unsigned {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(self, r: $r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(self).ok();
                let r: Option<$common> = <$common>::try_from(r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(self, r: $r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(self) {
                    Ok(v) => v,
                    Err(_) =>  <$common>::MAX};
                let r = match <$common>::try_from(r) {
                    Ok(v) => v,
                    Err(_) => <$common>::MAX};
                (l, r)
            }

            fn wrapping_promote(self, r: $r) -> (Self::Output, Self::Output) {
                (self as $common, r as $common)
            }
        }
    };
}

impl_promote_unsigned_unsigned!(u8, u8 => u128);
impl_promote_unsigned_unsigned!(u8, u16 => u128); impl_promote_unsigned_unsigned!(u16, u8 => u128);
impl_promote_unsigned_unsigned!(u8, u32 => u128); impl_promote_unsigned_unsigned!(u32, u8 => u128);
impl_promote_unsigned_unsigned!(u8, u64 => u128); impl_promote_unsigned_unsigned!(u64, u8 => u128);
impl_promote_unsigned_unsigned!(u8, u128 => u128); impl_promote_unsigned_unsigned!(u128, u8 => u128);

impl_promote_unsigned_unsigned!(u16, u16 => u128);
impl_promote_unsigned_unsigned!(u16, u32 => u128); impl_promote_unsigned_unsigned!(u32, u16 => u128);
impl_promote_unsigned_unsigned!(u16, u64 => u128); impl_promote_unsigned_unsigned!(u64, u16 => u128);
impl_promote_unsigned_unsigned!(u16, u128 => u128); impl_promote_unsigned_unsigned!(u128, u16 => u128);

impl_promote_unsigned_unsigned!(u32, u32 => u128);
impl_promote_unsigned_unsigned!(u32, u64 => u128); impl_promote_unsigned_unsigned!(u64, u32 => u128);
impl_promote_unsigned_unsigned!(u32, u128 => u128); impl_promote_unsigned_unsigned!(u128, u32 => u128);

impl_promote_unsigned_unsigned!(u64, u64 => u128);
impl_promote_unsigned_unsigned!(u64, u128 => u128); impl_promote_unsigned_unsigned!(u128, u64 => u128);

impl_promote_unsigned_unsigned!(u128, u128 => u128);


macro_rules! impl_promote_signed_unsigned {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(self, r: $r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(self).ok();
                let r: Option<$common> = <$common>::try_from(r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(self, r: $r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(self) {
                    Ok(v) => v,
                    Err(_) => if self < 0 { <$common>::MIN } else { <$common>::MAX }};
                let r = match <$common>::try_from(r) {
                    Ok(v) => v,
                    Err(_) => <$common>::MAX};
                (l, r)
            }

            fn wrapping_promote(self, r: $r) -> (Self::Output, Self::Output) {
                (self as $common, r as $common)
            }
        }
    };
}

macro_rules! impl_promote_unsigned_signed {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(self, r: $r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(self).ok();
                let r: Option<$common> = <$common>::try_from(r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(self, r: $r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(self) {
                    Ok(v) => v,
                    Err(_) => <$common>::MAX};
                let r = match <$common>::try_from(r) {
                    Ok(v) => v,
                    Err(_) => if r < 0 { <$common>::MIN } else { <$common>::MAX }};
                (l, r)
            }

            fn wrapping_promote(self, r: $r) -> (Self::Output, Self::Output) {
                (self as $common, r as $common)
            }
        }
    };
}

impl_promote_signed_unsigned!(i8, u8 => i128);    impl_promote_unsigned_signed!(u8, i8 => i128);
impl_promote_signed_unsigned!(i8, u16 => i128);   impl_promote_unsigned_signed!(u16, i8 => i128);
impl_promote_signed_unsigned!(i8, u32 => i128);   impl_promote_unsigned_signed!(u32, i8 => i128);
impl_promote_signed_unsigned!(i8, u64 => i128);   impl_promote_unsigned_signed!(u64, i8 => i128);
impl_promote_signed_unsigned!(i8, u128 => i128);  impl_promote_unsigned_signed!(u128, i8 => i128);
impl_promote_signed_unsigned!(i16, u8 => i128);   impl_promote_unsigned_signed!(u8, i16 => i128);
impl_promote_signed_unsigned!(i16, u16 => i128);  impl_promote_unsigned_signed!(u16, i16 => i128);
impl_promote_signed_unsigned!(i16, u32 => i128);  impl_promote_unsigned_signed!(u32, i16 => i128);
impl_promote_signed_unsigned!(i16, u64 => i128);  impl_promote_unsigned_signed!(u64, i16 => i128);
impl_promote_signed_unsigned!(i16, u128 => i128); impl_promote_unsigned_signed!(u128, i16 => i128);
impl_promote_signed_unsigned!(i32, u8 => i128);   impl_promote_unsigned_signed!(u8, i32 => i128);
impl_promote_signed_unsigned!(i32, u16 => i128);  impl_promote_unsigned_signed!(u16, i32 => i128);
impl_promote_signed_unsigned!(i32, u32 => i128);  impl_promote_unsigned_signed!(u32, i32 => i128);
impl_promote_signed_unsigned!(i32, u64 => i128);  impl_promote_unsigned_signed!(u64, i32 => i128);
impl_promote_signed_unsigned!(i32, u128 => i128); impl_promote_unsigned_signed!(u128, i32 => i128);
impl_promote_signed_unsigned!(i64, u8 => i128);   impl_promote_unsigned_signed!(u8, i64 => i128);
impl_promote_signed_unsigned!(i64, u16 => i128);  impl_promote_unsigned_signed!(u16, i64 => i128);
impl_promote_signed_unsigned!(i64, u32 => i128);  impl_promote_unsigned_signed!(u32, i64 => i128);
impl_promote_signed_unsigned!(i64, u64 => i128);  impl_promote_unsigned_signed!(u64, i64 => i128);
impl_promote_signed_unsigned!(i64, u128 => i128); impl_promote_unsigned_signed!(u128, i64 => i128);
impl_promote_signed_unsigned!(i128, u8 => i128);   impl_promote_unsigned_signed!(u8, i128 => i128);
impl_promote_signed_unsigned!(i128, u16 => i128);  impl_promote_unsigned_signed!(u16, i128 => i128);
impl_promote_signed_unsigned!(i128, u32 => i128);  impl_promote_unsigned_signed!(u32, i128 => i128);
impl_promote_signed_unsigned!(i128, u64 => i128);  impl_promote_unsigned_signed!(u64, i128 => i128);
impl_promote_signed_unsigned!(i128, u128 => i128); impl_promote_unsigned_signed!(u128, i128 => i128);