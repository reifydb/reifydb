// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod optimistic;
mod serializable;
mod transaction;

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::AsyncCowVec;
use reifydb_core::encoding::bincode;
use reifydb_transaction::{Key, Value};

pub trait IntoKey {
    fn into_key(self) -> Key;
}

pub trait IntoValue {
    fn into_value(self) -> Value;
}

pub trait FromKey: Sized {
    fn from_key(key: &Key) -> Option<Self>;
}

pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Option<Self>;
}

#[macro_export]
macro_rules! into_key {
    ($key:expr) => {
        IntoKey::into_key($key.clone())
    };
}

#[macro_export]
macro_rules! into_value {
    ($val:expr) => {
        IntoValue::into_value($val.clone())
    };
}

#[macro_export]
macro_rules! from_value {
    ($val:expr) => {
        FromValue::from_value(&$val).unwrap()
    };
}

macro_rules! impl_kv_for {
    ($t:ty) => {
        impl IntoKey for $t {
            fn into_key(self) -> Key {
                AsyncCowVec::new(bincode::serialize(&self))
            }
        }

        impl IntoValue for $t {
            fn into_value(self) -> Value {
                AsyncCowVec::new(bincode::serialize(&self))
            }
        }

        impl FromKey for $t {
            fn from_key(key: &Key) -> Option<Self> {
                bincode::deserialize(key).ok()
            }
        }

        impl FromValue for $t {
            fn from_value(value: &Value) -> Option<Self> {
                bincode::deserialize(value).ok()
            }
        }
    };
}

impl_kv_for!(u64);
