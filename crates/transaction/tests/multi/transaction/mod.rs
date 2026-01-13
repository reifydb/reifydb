// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod begin;
mod get;
mod iter;
mod range;
mod rollback;
mod version;
mod write;
mod write_skew;

use reifydb_core::{CowVec, value::encoded::EncodedValues};
pub use reifydb_core::{EncodedKey, util::encoding::keycode};
pub use reifydb_transaction::multi::TransactionMulti;

pub fn test_multi() -> TransactionMulti {
	TransactionMulti::testing()
}

pub trait IntoValues {
	fn into_values(self) -> EncodedValues;
}

pub trait FromValues: Sized {
	fn from_values(values: &EncodedValues) -> Option<Self>;
}

pub trait FromKey: Sized {
	fn from_key(key: &EncodedKey) -> Option<Self>;
}

#[macro_export]
macro_rules! as_key {
	($key:expr) => {{ reifydb_core::EncodedKey::new(reifydb_core::util::encoding::keycode::serialize(&$key)) }};
}

#[macro_export]
macro_rules! as_values {
	($val:expr) => {{ <_ as crate::multi::transaction::IntoValues>::into_values($val) }};
}

#[macro_export]
macro_rules! from_values {
	($t:ty, $val:expr) => {
		<$t as FromValues>::from_values(&$val).unwrap()
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
		impl IntoValues for $t {
			fn into_values(self) -> EncodedValues {
				EncodedValues(CowVec::new(keycode::serialize(&self)))
			}
		}
		impl FromKey for $t {
			fn from_key(key: &EncodedKey) -> Option<Self> {
				keycode::deserialize(key).ok()
			}
		}
		impl FromValues for $t {
			fn from_values(values: &EncodedValues) -> Option<Self> {
				keycode::deserialize(&values.0).ok()
			}
		}
	};
}

impl_kv_for!(i32);
impl_kv_for!(u64);
impl_kv_for!(String);
