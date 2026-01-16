// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB


#![cfg_attr(rustfmt, rustfmt_skip)]

use crate::value::is::IsNumber;

pub trait Promote<R> where Self: IsNumber, R: IsNumber {
    type Output: IsNumber;
    fn checked_promote(&self, r: &R) -> Option<(Self::Output, Self::Output)>;
    fn saturating_promote(&self, r: &R) -> (Self::Output, Self::Output);
    fn wrapping_promote(&self, r: &R) -> (Self::Output, Self::Output);

}

macro_rules! impl_promote_float_float {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(&self, r: &$r) -> Option<(Self::Output, Self::Output)> {
                if self.is_finite() && r.is_finite() {
                    Some((*self as $common, *r as $common))
                } else {
                    None
                }
            }

            fn saturating_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                let l = if self.is_finite() {
                    *self as $common
                } else if self.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };
                let r = if r.is_finite() {
                    *r as $common
                } else if r.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };
                (l, r)
            }

            fn wrapping_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                (*self as $common, *r as $common)
            }
        }
    };
}

macro_rules! impl_promote_float_integer {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(&self, r: &$r) -> Option<(Self::Output, Self::Output)> {
                if self.is_finite() {
                    Some((*self as $common, *r as $common))
                } else {
                    None
                }
            }

            fn saturating_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                let l = if self.is_finite() {
                    *self as $common
                } else if self.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };

                let r = *r as $common;
                (l, r)
            }

            fn wrapping_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                (*self as $common, *r as $common)
            }
        }
    };
}


macro_rules! impl_promote_integer_float {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(&self, r: &$r) -> Option<(Self::Output, Self::Output)> {
                if r.is_finite() {
                    Some((*self as $common, *r as $common))
                } else {
                    None
                }
            }

            fn saturating_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                let l = *self as $common;
                let r = if r.is_finite() {
                    *r as $common
                } else if r.is_sign_negative() {
                    <$common>::MIN
                } else {
                    <$common>::MAX
                };
                (l, r)
            }

            fn wrapping_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                (*self as $common, *r as $common)
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

            fn checked_promote(&self, r: &$r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(*self).ok();
                let r: Option<$common> = <$common>::try_from(*r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(*self) {
                    Ok(v) => v,
                    Err(_) => if *self < 0 { <$common>::MIN } else { <$common>::MAX }};
                let r = match <$common>::try_from(*r) {
                    Ok(v) => v,
                    Err(_) => if *r < 0 { <$common>::MIN } else { <$common>::MAX }};
                (l, r)
            }

            fn wrapping_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                (*self as $common, *r as $common)
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

            fn checked_promote(&self, r: &$r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(*self).ok();
                let r: Option<$common> = <$common>::try_from(*r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(*self) {
                    Ok(v) => v,
                    Err(_) =>  <$common>::MAX};
                let r = match <$common>::try_from(*r) {
                    Ok(v) => v,
                    Err(_) => <$common>::MAX};
                (l, r)
            }

            fn wrapping_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                (*self as $common, *r as $common)
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

            fn checked_promote(&self, r: &$r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(*self).ok();
                let r: Option<$common> = <$common>::try_from(*r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(*self) {
                    Ok(v) => v,
                    Err(_) => if *self < 0 { <$common>::MIN } else { <$common>::MAX }};
                let r = match <$common>::try_from(*r) {
                    Ok(v) => v,
                    Err(_) => <$common>::MAX};
                (l, r)
            }

            fn wrapping_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                (*self as $common, *r as $common)
            }
        }
    };
}

macro_rules! impl_promote_unsigned_signed {
    ($l:ty, $r:ty => $common:ty) => {
        impl Promote<$r> for $l {
            type Output = $common;

            fn checked_promote(&self, r: &$r) -> Option<(Self::Output, Self::Output)>{
                let l: Option<$common> = <$common>::try_from(*self).ok();
                let r: Option<$common> = <$common>::try_from(*r).ok();
                match(l,r){
                    (Some(l),Some(r)) => Some((l,r)),
                    _ => None
                }
            }

            fn saturating_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                let l = match <$common>::try_from(*self) {
                    Ok(v) => v,
                    Err(_) => <$common>::MAX};
                let r = match <$common>::try_from(*r) {
                    Ok(v) => v,
                    Err(_) => if *r < 0 { <$common>::MIN } else { <$common>::MAX }};
                (l, r)
            }

            fn wrapping_promote(&self, r: &$r) -> (Self::Output, Self::Output) {
                (*self as $common, *r as $common)
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

use crate::value::{decimal::Decimal, int::Int, uint::Uint};

impl Promote<Int> for Int {
    type Output = Int;

    fn checked_promote(&self, r: &Int) -> Option<(Self::Output, Self::Output)> {
        Some((self.clone(), r.clone()))
    }

    fn saturating_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        (self.clone(), r.clone())
    }

    fn wrapping_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        (self.clone(), r.clone())
    }
}

impl Promote<Uint> for Uint {
    type Output = Uint;

    fn checked_promote(&self, r: &Uint) -> Option<(Self::Output, Self::Output)> {
        Some((self.clone(), r.clone()))
    }

    fn saturating_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        (self.clone(), r.clone())
    }

    fn wrapping_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        (self.clone(), r.clone())
    }
}

impl Promote<Decimal> for Decimal {
    type Output = Decimal;

    fn checked_promote(&self, r: &Decimal) -> Option<(Self::Output, Self::Output)> {
        Some((self.clone(), r.clone()))
    }

    fn saturating_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        (self.clone(), r.clone())
    }

    fn wrapping_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        (self.clone(), r.clone())
    }
}

impl Promote<Uint> for Int {
    type Output = Int;

    fn checked_promote(&self, r: &Uint) -> Option<(Self::Output, Self::Output)> {
        let r_as_int = Int::from(r.0.clone());
        Some((self.clone(), r_as_int))
    }

    fn saturating_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let r_as_int = Int::from(r.0.clone());
        (self.clone(), r_as_int)
    }

    fn wrapping_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let r_as_int = Int::from(r.0.clone());
        (self.clone(), r_as_int)
    }
}

impl Promote<Int> for Uint {
    type Output = Int;

    fn checked_promote(&self, r: &Int) -> Option<(Self::Output, Self::Output)> {
        let l_as_int = Int::from(self.0.clone());
        Some((l_as_int, r.clone()))
    }

    fn saturating_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let l_as_int = Int::from(self.0.clone());
        (l_as_int, r.clone())
    }

    fn wrapping_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let l_as_int = Int::from(self.0.clone());
        (l_as_int, r.clone())
    }
}

impl Promote<Decimal> for Int {
    type Output = Decimal;

    fn checked_promote(&self, r: &Decimal) -> Option<(Self::Output, Self::Output)> {
        let l_as_decimal = Decimal::from(self.clone());
        Some((l_as_decimal, r.clone()))
    }

    fn saturating_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        (l_as_decimal, r.clone())
    }

    fn wrapping_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        (l_as_decimal, r.clone())
    }
}

impl Promote<Int> for Decimal {
    type Output = Decimal;

    fn checked_promote(&self, r: &Int) -> Option<(Self::Output, Self::Output)> {
        let r_as_decimal = Decimal::from(r.clone());
        Some((self.clone(), r_as_decimal))
    }

    fn saturating_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let r_as_decimal = Decimal::from(r.clone());
        (self.clone(), r_as_decimal)
    }

    fn wrapping_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let r_as_decimal = Decimal::from(r.clone());
        (self.clone(), r_as_decimal)
    }
}

impl Promote<Decimal> for Uint {
    type Output = Decimal;

    fn checked_promote(&self, r: &Decimal) -> Option<(Self::Output, Self::Output)> {
        let l_as_decimal = Decimal::from(self.clone());
        Some((l_as_decimal, r.clone()))
    }

    fn saturating_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        (l_as_decimal, r.clone())
    }

    fn wrapping_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        (l_as_decimal, r.clone())
    }
}

impl Promote<Uint> for Decimal {
    type Output = Decimal;

    fn checked_promote(&self, r: &Uint) -> Option<(Self::Output, Self::Output)> {
        let r_as_decimal = Decimal::from(r.clone());
        Some((self.clone(), r_as_decimal))
    }

    fn saturating_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let r_as_decimal = Decimal::from(r.clone());
        (self.clone(), r_as_decimal)
    }

    fn wrapping_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let r_as_decimal = Decimal::from(r.clone());
        (self.clone(), r_as_decimal)
    }
}
// Float to Int, Uint, Decimal promotions
impl Promote<Int> for f32 {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &Int) -> Option<(Self::Output, Self::Output)> {
        if self.is_finite() {
            Some((Decimal::from(*self), Decimal::from(r.clone())))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, Decimal::from(r.clone()))
    }
    
    fn wrapping_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, Decimal::from(r.clone()))
    }
}

impl Promote<f32> for Int {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &f32) -> Option<(Self::Output, Self::Output)> {
        if r.is_finite() {
            Some((Decimal::from(self.clone()), Decimal::from(*r)))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &f32) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (Decimal::from(self.clone()), r_as_decimal)
    }
    
    fn wrapping_promote(&self, r: &f32) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (Decimal::from(self.clone()), r_as_decimal)
    }
}

impl Promote<Uint> for f32 {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &Uint) -> Option<(Self::Output, Self::Output)> {
        if self.is_finite() {
            Some((Decimal::from(*self), Decimal::from(r.clone())))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, Decimal::from(r.clone()))
    }
    
    fn wrapping_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, Decimal::from(r.clone()))
    }
}

impl Promote<f32> for Uint {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &f32) -> Option<(Self::Output, Self::Output)> {
        if r.is_finite() {
            Some((Decimal::from(self.clone()), Decimal::from(*r)))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &f32) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (Decimal::from(self.clone()), r_as_decimal)
    }
    
    fn wrapping_promote(&self, r: &f32) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (Decimal::from(self.clone()), r_as_decimal)
    }
}

impl Promote<Decimal> for f32 {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &Decimal) -> Option<(Self::Output, Self::Output)> {
        if self.is_finite() {
            Some((Decimal::from(*self), r.clone()))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r.clone())
    }
    
    fn wrapping_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r.clone())
    }
}

impl Promote<f32> for Decimal {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &f32) -> Option<(Self::Output, Self::Output)> {
        if r.is_finite() {
            Some((self.clone(), Decimal::from(*r)))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &f32) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (self.clone(), r_as_decimal)
    }
    
    fn wrapping_promote(&self, r: &f32) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (self.clone(), r_as_decimal)
    }
}

// f64 implementations
impl Promote<Int> for f64 {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &Int) -> Option<(Self::Output, Self::Output)> {
        if self.is_finite() {
            Some((Decimal::from(*self), Decimal::from(r.clone())))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, Decimal::from(r.clone()))
    }
    
    fn wrapping_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, Decimal::from(r.clone()))
    }
}

impl Promote<f64> for Int {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &f64) -> Option<(Self::Output, Self::Output)> {
        if r.is_finite() {
            let l_as_decimal = Decimal::from(self.clone());
            let r_as_decimal = Decimal::from(*r);
            Some((l_as_decimal, r_as_decimal))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &f64) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r_as_decimal)
    }
    
    fn wrapping_promote(&self, r: &f64) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r_as_decimal)
    }
}

impl Promote<Uint> for f64 {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &Uint) -> Option<(Self::Output, Self::Output)> {
        if self.is_finite() {
            let l_as_decimal = Decimal::from(*self);
            let r_as_decimal = Decimal::from(r.clone());
            Some((l_as_decimal, r_as_decimal))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        let r_as_decimal = Decimal::from(r.clone());
        (l_as_decimal, r_as_decimal)
    }
    
    fn wrapping_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        let r_as_decimal = Decimal::from(r.clone());
        (l_as_decimal, r_as_decimal)
    }
}

impl Promote<f64> for Uint {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &f64) -> Option<(Self::Output, Self::Output)> {
        if r.is_finite() {
            let l_as_decimal = Decimal::from(self.clone());
            let r_as_decimal = Decimal::from(*r);
            Some((l_as_decimal, r_as_decimal))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &f64) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r_as_decimal)
    }
    
    fn wrapping_promote(&self, r: &f64) -> (Self::Output, Self::Output) {
        let l_as_decimal = Decimal::from(self.clone());
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r_as_decimal)
    }
}

impl Promote<Decimal> for f64 {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &Decimal) -> Option<(Self::Output, Self::Output)> {
        if self.is_finite() {
            let l_as_decimal = Decimal::from(*self);
            Some((l_as_decimal, r.clone()))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r.clone())
    }
    
    fn wrapping_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
        let l_as_decimal = if self.is_finite() {
            Decimal::from(*self)
        } else {
            Decimal::zero()
        };
        (l_as_decimal, r.clone())
    }
}

impl Promote<f64> for Decimal {
    type Output = Decimal;
    
    fn checked_promote(&self, r: &f64) -> Option<(Self::Output, Self::Output)> {
        if r.is_finite() {
            let r_as_decimal = Decimal::from(*r);
            Some((self.clone(), r_as_decimal))
        } else {
            None
        }
    }
    
    fn saturating_promote(&self, r: &f64) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (self.clone(), r_as_decimal)
    }
    
    fn wrapping_promote(&self, r: &f64) -> (Self::Output, Self::Output) {
        let r_as_decimal = if r.is_finite() {
            Decimal::from(*r)
        } else {
            Decimal::zero()
        };
        (self.clone(), r_as_decimal)
    }
}

// Promote implementations for integer types with Int
macro_rules! impl_promote_int_to_int {
    ($($t:ty),*) => {
        $(
            impl Promote<Int> for $t {
                type Output = Int;
                
                fn checked_promote(&self, r: &Int) -> Option<(Self::Output, Self::Output)> {
                    Some((Int::from(*self), r.clone()))
                }
                
                fn saturating_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
                    (Int::from(*self), r.clone())
                }
                
                fn wrapping_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
                    (Int::from(*self), r.clone())
                }
            }
            
            impl Promote<$t> for Int {
                type Output = Int;
                
                fn checked_promote(&self, r: &$t) -> Option<(Self::Output, Self::Output)> {
                    Some((self.clone(), Int::from(*r)))
                }
                
                fn saturating_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Int::from(*r))
                }
                
                fn wrapping_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Int::from(*r))
                }
            }
        )*
    }
}

impl_promote_int_to_int!(i8, i16, i32, i64, i128);

// Promote implementations for unsigned integer types with Uint
macro_rules! impl_promote_uint_to_uint {
    ($($t:ty),*) => {
        $(
            impl Promote<Uint> for $t {
                type Output = Uint;
                
                fn checked_promote(&self, r: &Uint) -> Option<(Self::Output, Self::Output)> {
                    Some((Uint::from(*self), r.clone()))
                }
                
                fn saturating_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
                    (Uint::from(*self), r.clone())
                }
                
                fn wrapping_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
                    (Uint::from(*self), r.clone())
                }
            }
            
            impl Promote<$t> for Uint {
                type Output = Uint;
                
                fn checked_promote(&self, r: &$t) -> Option<(Self::Output, Self::Output)> {
                    Some((self.clone(), Uint::from(*r)))
                }
                
                fn saturating_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Uint::from(*r))
                }
                
                fn wrapping_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Uint::from(*r))
                }
            }
        )*
    }
}

impl_promote_uint_to_uint!(u8, u16, u32, u64, u128);

// Promote implementations for unsigned integers with Int (promotes to Int)
macro_rules! impl_promote_uint_to_int {
    ($($t:ty),*) => {
        $(
            impl Promote<Int> for $t {
                type Output = Int;
                
                fn checked_promote(&self, r: &Int) -> Option<(Self::Output, Self::Output)> {
                    Some((Int::from(*self), r.clone()))
                }
                
                fn saturating_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
                    (Int::from(*self), r.clone())
                }
                
                fn wrapping_promote(&self, r: &Int) -> (Self::Output, Self::Output) {
                    (Int::from(*self), r.clone())
                }
            }
            
            impl Promote<$t> for Int {
                type Output = Int;
                
                fn checked_promote(&self, r: &$t) -> Option<(Self::Output, Self::Output)> {
                    Some((self.clone(), Int::from(*r)))
                }
                
                fn saturating_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Int::from(*r))
                }
                
                fn wrapping_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Int::from(*r))
                }
            }
        )*
    }
}

impl_promote_uint_to_int!(u8, u16, u32, u64, u128);

// Promote implementations for signed integers with Uint (promotes to Int)
macro_rules! impl_promote_int_to_uint {
    ($($t:ty),*) => {
        $(
            impl Promote<Uint> for $t {
                type Output = Int;
                
                fn checked_promote(&self, r: &Uint) -> Option<(Self::Output, Self::Output)> {
                    Some((Int::from(*self), Int(r.0.clone())))
                }
                
                fn saturating_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
                    (Int::from(*self), Int(r.0.clone()))
                }
                
                fn wrapping_promote(&self, r: &Uint) -> (Self::Output, Self::Output) {
                    (Int::from(*self), Int(r.0.clone()))
                }
            }
            
            impl Promote<$t> for Uint {
                type Output = Int;
                
                fn checked_promote(&self, r: &$t) -> Option<(Self::Output, Self::Output)> {
                    Some((Int(self.0.clone()), Int::from(*r)))
                }
                
                fn saturating_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (Int(self.0.clone()), Int::from(*r))
                }
                
                fn wrapping_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (Int(self.0.clone()), Int::from(*r))
                }
            }
        )*
    }
}

impl_promote_int_to_uint!(i8, i16, i32, i64, i128);

// Promote implementations for all integer types with Decimal
macro_rules! impl_promote_int_to_decimal {
    ($($t:ty),*) => {
        $(
            impl Promote<Decimal> for $t {
                type Output = Decimal;
                
                fn checked_promote(&self, r: &Decimal) -> Option<(Self::Output, Self::Output)> {
                    Some((Decimal::from(*self), r.clone()))
                }
                
                fn saturating_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
                    (Decimal::from(*self), r.clone())
                }
                
                fn wrapping_promote(&self, r: &Decimal) -> (Self::Output, Self::Output) {
                    (Decimal::from(*self), r.clone())
                }
            }
            
            impl Promote<$t> for Decimal {
                type Output = Decimal;
                
                fn checked_promote(&self, r: &$t) -> Option<(Self::Output, Self::Output)> {
                    Some((self.clone(), Decimal::from(*r)))
                }
                
                fn saturating_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Decimal::from(*r))
                }
                
                fn wrapping_promote(&self, r: &$t) -> (Self::Output, Self::Output) {
                    (self.clone(), Decimal::from(*r))
                }
            }
        )*
    }
}

impl_promote_int_to_decimal!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);
