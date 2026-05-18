#![no_std]

use core::ops::{BitAnd, BitOr, BitXor};
use core::{mem, slice};

///Type with a specified byte order.
pub trait Endian<T> {}

macro_rules! impl_Endian{
    ( for $e:ident) => {
        impl<T> BitAnd for $e<T>
        where
            T: BitAnd,
        {
            type Output = $e<<T as BitAnd>::Output>;

            #[inline]
            fn bitand(self, other: Self) -> Self::Output {
                $e(self.0 & other.0)
            }
        }

        impl<T> BitOr for $e<T>
        where
            T: BitOr,
        {
            type Output = $e<<T as BitOr>::Output>;

            #[inline]
            fn bitor(self, other: Self) -> Self::Output {
                $e(self.0 | other.0)
            }
        }

        impl<T> BitXor for $e<T>
        where
            T: BitXor,
        {
            type Output = $e<<T as BitXor>::Output>;

            #[inline]
            fn bitxor(self, other: Self) -> Self::Output {
                $e(self.0 ^ other.0)
            }
        }

        impl<T> $e<T>
        where
            T: Sized + Copy,
        {
            #[inline]
            pub const unsafe fn from_byte_slice(bytes: &[u8]) -> $e<T> {
                debug_assert!(bytes.len() >= mem::size_of::<T>());
                $e({ *(bytes.as_ptr() as *const T) })
            }

            #[inline]
            pub fn as_byte_slice(&self) -> &[u8] {
                unsafe { slice::from_raw_parts(&self.0 as *const T as *const u8, mem::size_of::<T>()) }
            }

            #[inline]
            pub fn as_byte_slice_mut(&mut self) -> &mut [u8] {
                unsafe { slice::from_raw_parts_mut(&mut self.0 as *mut T as *mut u8, mem::size_of::<T>()) }
            }
        }
    }
}

///Big endian byte order.
///
///Most significant byte first.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct BigEndian<T>(T);

impl<T> Endian<T> for BigEndian<T> {}

macro_rules! impl_for_BigEndian {
    ( $t:ident ) => {
        impl From<BigEndian<$t>> for $t {
            #[inline]
            fn from(data: BigEndian<$t>) -> $t {
                $t::from_be(data.0)
            }
        }

        impl From<$t> for BigEndian<$t> {
            #[inline]
            fn from(data: $t) -> Self {
                BigEndian(data.to_be())
            }
        }

        impl From<LittleEndian<$t>> for BigEndian<$t> {
            #[inline]
            fn from(data: LittleEndian<$t>) -> Self {
                BigEndian(data.0.swap_bytes())
            }
        }

        //TODO: Move these to Endian when https://github.com/rust-lang/rust/issues/67792 stabilized in a few years
        impl BigEndian<$t>{
            ///Return the memory representation of this type as a byte array in its endian byte order.
            ///Note: This is just a transmute.
            #[inline]
            pub const fn to_bytes(self) -> [u8; mem::size_of::<BigEndian<$t>>()] {
                unsafe { mem::transmute(self) }
            }

            ///Construct a value from its memory representation as a byte array.
            ///Note: This is just a transmute.
            #[inline]
            pub const fn from_bytes(bytes: [u8; mem::size_of::<BigEndian<$t>>()]) -> BigEndian<$t> {
                unsafe { mem::transmute(bytes) }
            }
        }
    };
}

impl_Endian!(for BigEndian);
impl_for_BigEndian!(u16);
impl_for_BigEndian!(u32);
impl_for_BigEndian!(u64);
impl_for_BigEndian!(u128);
impl_for_BigEndian!(usize);
impl_for_BigEndian!(i16);
impl_for_BigEndian!(i32);
impl_for_BigEndian!(i64);
impl_for_BigEndian!(i128);
impl_for_BigEndian!(isize);

///Little endian byte order.
///
///Least significant byte first.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct LittleEndian<T>(T);

impl<T> Endian<T> for LittleEndian<T> {}

macro_rules! impl_for_LittleEndian {
    ( $t:ident ) => {
        impl From<LittleEndian<$t>> for $t {
            #[inline]
            fn from(data: LittleEndian<$t>) -> $t {
                $t::from_le(data.0)
            }
        }

        impl From<$t> for LittleEndian<$t> {
            #[inline]
            fn from(data: $t) -> Self {
                LittleEndian(data.to_le())
            }
        }

        impl From<BigEndian<$t>> for LittleEndian<$t> {
            #[inline]
            fn from(data: BigEndian<$t>) -> Self {
                LittleEndian(data.0.swap_bytes())
            }
        }

        impl LittleEndian<$t>{
            ///Return the memory representation of this type as a byte array in its endian byte order.
            ///Note: This is just a transmute.
            #[inline]
            pub const fn to_bytes(self) -> [u8; mem::size_of::<LittleEndian<$t>>()] {
                unsafe { mem::transmute(self) }
            }

            ///Construct a value from its memory representation as a byte array.
            ///Note: This is just a transmute.
            #[inline]
            pub const fn from_bytes(bytes: [u8; mem::size_of::<LittleEndian<$t>>()]) -> LittleEndian<$t> {
                unsafe { mem::transmute(bytes) }
            }
        }
    };
}

impl_Endian!(for LittleEndian);
impl_for_LittleEndian!(u16);
impl_for_LittleEndian!(u32);
impl_for_LittleEndian!(u64);
impl_for_LittleEndian!(u128);
impl_for_LittleEndian!(usize);
impl_for_LittleEndian!(i16);
impl_for_LittleEndian!(i32);
impl_for_LittleEndian!(i64);
impl_for_LittleEndian!(i128);
impl_for_LittleEndian!(isize);

///Network byte order as defined by IETF RFC1700 <http://tools.ietf.org/html/rfc1700>.
pub type NetworkOrder<T> = BigEndian<T>;

///Type aliases for primitive types.
pub mod types {
    #![allow(non_camel_case_types)]

    use super::*;

    pub type i16_be = BigEndian<i16>;
    pub type i32_be = BigEndian<i32>;
    pub type i64_be = BigEndian<i64>;
    pub type i128_be = BigEndian<i128>;
    pub type isize_be = BigEndian<isize>;

    pub type u16_be = BigEndian<u16>;
    pub type u32_be = BigEndian<u32>;
    pub type u64_be = BigEndian<u64>;
    pub type u128_be = BigEndian<u128>;
    pub type usize_be = BigEndian<usize>;

    pub type i16_le = LittleEndian<i16>;
    pub type i32_le = LittleEndian<i32>;
    pub type i64_le = LittleEndian<i64>;
    pub type i128_le = LittleEndian<i128>;
    pub type isize_le = LittleEndian<isize>;

    pub type u16_le = LittleEndian<u16>;
    pub type u32_le = LittleEndian<u32>;
    pub type u64_le = LittleEndian<u64>;
    pub type u128_le = LittleEndian<u128>;
    pub type usize_le = LittleEndian<usize>;

    pub type i16_net = NetworkOrder<i16>;
    pub type i32_net = NetworkOrder<i32>;
    pub type i128_net = NetworkOrder<i128>;
    pub type isize_net = NetworkOrder<isize>;

    pub type u16_net = NetworkOrder<u16>;
    pub type u32_net = NetworkOrder<u32>;
    pub type u64_net = NetworkOrder<u64>;
    pub type u128_net = NetworkOrder<u128>;
    pub type usize_net = NetworkOrder<usize>;
}

#[cfg(test)]
mod tests{
    use super::*;
    use super::types::*;

    #[test]
    fn from_to_bytes(){
        macro_rules! test{
            ($e:ident for $t:ident in $r:expr) => {
                for i in $r{
                    let j = $e::<$t>::from(i);
                    assert_eq!($e::<$t>::from_bytes(j.to_bytes()) , j);
                }
            }
        }

        test!(BigEndian for i16  in -10000..10000);
        test!(BigEndian for i32  in -10000..10000);
        test!(BigEndian for i64  in -10000..10000);
        test!(BigEndian for i128 in -10000..10000);

        test!(LittleEndian for i16  in -10000..10000);
        test!(LittleEndian for i32  in -10000..10000);
        test!(LittleEndian for i64  in -10000..10000);
        test!(LittleEndian for i128 in -10000..10000);

        test!(BigEndian for u16  in 0..20000);
        test!(BigEndian for u32  in 0..20000);
        test!(BigEndian for u64  in 0..20000);
        test!(BigEndian for u128 in 0..20000);

        test!(LittleEndian for u16  in 0..20000);
        test!(LittleEndian for u32  in 0..20000);
        test!(LittleEndian for u64  in 0..20000);
        test!(LittleEndian for u128 in 0..20000);
    }

    #[test]
    fn to_bytes_std(){
        macro_rules! test{
            ( for $t:ident in $r:expr) => {
                for i in $r{
                    assert_eq!(   BigEndian::<$t>::from(i).to_bytes() , i.to_be_bytes());
                    assert_eq!(LittleEndian::<$t>::from(i).to_bytes() , i.to_le_bytes());
                }
            }
        }

        test!(for i16  in -10000..10000);
        test!(for i32  in -10000..10000);
        test!(for i64  in -10000..10000);
        test!(for i128 in -10000..10000);

        test!(for i16  in 0..20000);
        test!(for i32  in 0..20000);
        test!(for i64  in 0..20000);
        test!(for i128 in 0..20000);
    }

    #[test]
    fn as_to_byte_slice(){
        macro_rules! test{
            ($e:ident for $t:ident in $r:expr) => {
                for i in $r{
                    let mut j = $e::<$t>::from(i);
                    assert_eq!(unsafe{$e::<$t>::from_byte_slice(j.as_byte_slice())} , j);
                    assert_eq!(unsafe{$e::<$t>::from_byte_slice(j.as_byte_slice_mut())} , j);
                }
            }
        }

        test!(BigEndian for i16  in -10000..10000);
        test!(BigEndian for i32  in -10000..10000);
        test!(BigEndian for i64  in -10000..10000);
        test!(BigEndian for i128 in -10000..10000);

        test!(LittleEndian for i16  in -10000..10000);
        test!(LittleEndian for i32  in -10000..10000);
        test!(LittleEndian for i64  in -10000..10000);
        test!(LittleEndian for i128 in -10000..10000);

        test!(BigEndian for u16  in 0..20000);
        test!(BigEndian for u32  in 0..20000);
        test!(BigEndian for u64  in 0..20000);
        test!(BigEndian for u128 in 0..20000);

        test!(LittleEndian for u16  in 0..20000);
        test!(LittleEndian for u32  in 0..20000);
        test!(LittleEndian for u64  in 0..20000);
        test!(LittleEndian for u128 in 0..20000);
    }

}

#[cfg(doctest)]
mod test_readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            extern {}
        };
    }
    external_doc_test!(include_str!("../README.md"));
}
