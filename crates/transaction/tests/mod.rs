// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod optimistic;
mod serializable;
mod transaction;

use reifydb_core::AsyncCowVec;
use reifydb_core::encoding::{bincode, keycode};
use reifydb_storage::{Key, Value};

pub trait IntoValue {
    fn into_value(self) -> Value;
}

pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Option<Self>;
}

pub trait FromKey: Sized {
    fn from_key(key: &Key) -> Option<Self>;
}

#[macro_export]
macro_rules! as_key {
    ($key:expr) => {{ AsyncCowVec::new(keycode::serialize(&$key)) }};
}

#[macro_export]
macro_rules! as_value {
    ($val:expr) => {{ IntoValue::into_value($val) }};
}

#[macro_export]
macro_rules! from_value {
    ($t:ty, $val:expr) => {
        <$t as FromValue>::from_value(&$val).unwrap()
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
        impl IntoValue for $t {
            fn into_value(self) -> Value {
                AsyncCowVec::new(bincode::serialize(&self))
            }
        }
        impl FromKey for $t {
            fn from_key(key: &Key) -> Option<Self> {
                keycode::deserialize(key).ok()
            }
        }
        impl FromValue for $t {
            fn from_value(value: &Value) -> Option<Self> {
                bincode::deserialize(value).ok()
            }
        }
    };
}

impl_kv_for!(i32);
impl_kv_for!(u64);
impl_kv_for!(String);
