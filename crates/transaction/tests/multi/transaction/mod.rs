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

use reifydb_codec::{
	key::{deserializer::KeyDeserializer, encoded::EncodedKey, serializer::KeySerializer},
	row::bytes::EncodedBytes,
};
use reifydb_transaction::multi::transaction::MultiTransaction;
use reifydb_value::util::cowvec::CowVec;

pub fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

pub trait IntoKey {
	fn into_key(self) -> EncodedKey;
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
	($key:expr) => {{ <_ as crate::multi::transaction::IntoKey>::into_key($key) }};
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
	($t:ty, $extend:ident, $read:ident) => {
		impl IntoKey for $t {
			fn into_key(self) -> EncodedKey {
				let mut ser = KeySerializer::new();
				ser.$extend(self);
				ser.finish()
			}
		}
		impl IntoValues for $t {
			fn into_bytes(self) -> EncodedBytes {
				let mut ser = KeySerializer::new();
				ser.$extend(self);
				EncodedBytes(CowVec::new(ser.finish().as_slice().to_vec()))
			}
		}
		impl FromKey for $t {
			fn from_key(key: &EncodedKey) -> Option<Self> {
				KeyDeserializer::from_bytes(key.as_slice()).$read().ok()
			}
		}
		impl FromRow for $t {
			fn from_bytes(bytes: &EncodedBytes) -> Option<Self> {
				KeyDeserializer::from_bytes(&bytes.0).$read().ok()
			}
		}
	};
}

impl IntoKey for &str {
	fn into_key(self) -> EncodedKey {
		let mut ser = KeySerializer::new();
		ser.extend_str(self);
		ser.finish()
	}
}

impl_kv_for!(i32, extend_i32, read_i32);
impl_kv_for!(u64, extend_u64, read_u64);
impl_kv_for!(String, extend_str, read_str);
