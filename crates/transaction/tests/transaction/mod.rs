// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod optimistic;
mod serializable;

use reifydb_core::delta::Bytes;
use reifydb_core::encoding::{bincode, keycode};
use reifydb_core::{AsyncCowVec, Key};

pub trait IntoBytes {
    fn into_bytes(self) -> Bytes;
}

pub trait FromBytes: Sized {
    fn from_bytes(bytes: &Bytes) -> Option<Self>;
}

pub trait FromKey: Sized {
    fn from_key(key: &Key) -> Option<Self>;
}

#[macro_export]
macro_rules! as_key {
    ($key:expr) => {{ AsyncCowVec::new(keycode::serialize(&$key)) }};
}

#[macro_export]
macro_rules! as_bytes {
    ($val:expr) => {{ IntoBytes::into_bytes($val) }};
}

#[macro_export]
macro_rules! from_bytes {
    ($t:ty, $val:expr) => {
        <$t as FromBytes>::from_bytes(&$val).unwrap()
    };
}

#[macro_export]
macro_rules! from_key {
    ($t:ty, $val:expr) => {
        <$t as FromKey>::from_key(&$val).unwrap()
    };
}

macro_rules! impl_kv_for {
    ($t:ty) => {
        impl IntoBytes for $t {
            fn into_bytes(self) -> Bytes {
                AsyncCowVec::new(bincode::serialize(&self))
            }
        }
        impl FromKey for $t {
            fn from_key(key: &Key) -> Option<Self> {
                keycode::deserialize(key).ok()
            }
        }
        impl FromBytes for $t {
            fn from_bytes(bytes: &Bytes) -> Option<Self> {
                bincode::deserialize(bytes).ok()
            }
        }
    };
}

impl_kv_for!(i32);
impl_kv_for!(u64);
impl_kv_for!(String);
