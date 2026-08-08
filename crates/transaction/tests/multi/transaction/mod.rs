// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod begin;
mod conflict_concurrent;
mod get;
mod iter;
mod lost_update;
mod range;
mod replica;
mod rollback;
mod too_large;
mod version;
mod write;
mod write_skew;

use reifydb_codec::{key as keycode, key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_transaction::multi::transaction::MultiTransaction;
use reifydb_value::util::cowvec::CowVec;

pub fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

pub trait IntoValues {
	fn into_bytes(self) -> EncodedBytes;
}

pub trait FromRow: Sized {
	fn from_bytes(bytes: &EncodedBytes) -> Option<Self>;
}

pub trait FromKey: Sized {
	fn from_key(key: &EncodedKey) -> Option<Self>;
}

#[macro_export]
macro_rules! as_key {
	($key:expr) => {{ reifydb_codec::key::encoded::EncodedKey::new(reifydb_codec::key::serialize(&$key)) }};
}

#[macro_export]
macro_rules! as_values {
	($val:expr) => {{ <_ as crate::multi::transaction::IntoValues>::into_bytes($val) }};
}

#[macro_export]
macro_rules! from_bytes {
	($t:ty, $val:expr) => {
		<$t as FromRow>::from_bytes(&$val).unwrap()
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
			fn into_bytes(self) -> EncodedBytes {
				EncodedBytes(CowVec::new(keycode::serialize(&self)))
			}
		}
		impl FromKey for $t {
			fn from_key(key: &EncodedKey) -> Option<Self> {
				keycode::deserialize(key).ok()
			}
		}
		impl FromRow for $t {
			fn from_bytes(bytes: &EncodedBytes) -> Option<Self> {
				keycode::deserialize(&bytes.0).ok()
			}
		}
	};
}

impl_kv_for!(i32);
impl_kv_for!(u64);
impl_kv_for!(String);
