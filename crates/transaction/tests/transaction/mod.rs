// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod optimistic;
mod serializable;

use reifydb_core::encoding::{bincode, keycode};
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, EncodedKey};

pub trait IntoRow {
    fn into_row(self) -> EncodedRow;
}

pub trait FromRow: Sized {
    fn from_row(row: &EncodedRow) -> Option<Self>;
}

pub trait FromKey: Sized {
    fn from_key(key: &EncodedKey) -> Option<Self>;
}

#[macro_export]
macro_rules! as_key {
    ($key:expr) => {{ EncodedKey::new(keycode::serialize(&$key)) }};
}

#[macro_export]
macro_rules! as_row {
    ($val:expr) => {{ IntoRow::into_row($val) }};
}

#[macro_export]
macro_rules! from_row {
    ($t:ty, $val:expr) => {
        <$t as FromRow>::from_row(&$val).unwrap()
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
        impl IntoRow for $t {
            fn into_row(self) -> EncodedRow {
                EncodedRow(AsyncCowVec::new(bincode::serialize(&self)))
            }
        }
        impl FromKey for $t {
            fn from_key(key: &EncodedKey) -> Option<Self> {
                keycode::deserialize(key).ok()
            }
        }
        impl FromRow for $t {
            fn from_row(row: &EncodedRow) -> Option<Self> {
                bincode::deserialize(row).ok()
            }
        }
    };
}

impl_kv_for!(i32);
impl_kv_for!(u64);
impl_kv_for!(String);
