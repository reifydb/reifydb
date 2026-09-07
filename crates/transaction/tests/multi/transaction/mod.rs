// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod begin;
mod conflict_concurrent;
mod get;
mod iter;
mod lost_update;
mod range;
mod rollback;
mod too_large;
mod version;
mod write;
mod write_skew;

use reifydb_codec::{
	key::{deserializer::KeyDeserializer, encoded::EncodedKey, serializer::KeySerializer},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	interface::catalog::{
		id::{IndexId, TableId},
		object::ObjectId,
	},
	key::{any::TaggedKey, catalog::IndexEntryKey},
	value::index::encoded::EncodedIndexKey,
};
use reifydb_transaction::multi::transaction::MultiTransaction;
use reifydb_value::util::cowvec::CowVec;

pub fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

pub trait IntoKey {
	fn into_key(self) -> TaggedKey;
}

// extend_raw appends the tail verbatim, so encoded order matches the raw key order.
fn synthetic_key(raw: EncodedKey) -> TaggedKey {
	IndexEntryKey::new(ObjectId::Table(TableId(1)), IndexId::primary(1u64), EncodedIndexKey::new(raw.as_slice()))
		.into()
}

fn synthetic_tail(key: &TaggedKey) -> Option<Vec<u8>> {
	match key {
		TaggedKey::IndexEntry(entry) => Some(entry.key.as_ref().to_vec()),
		_ => None,
	}
}

pub trait IntoValues {
	fn into_bytes(self) -> EncodedBytes;
}

pub trait FromRow: Sized {
	fn from_bytes(bytes: &EncodedBytes) -> Option<Self>;
}

pub trait FromKey: Sized {
	fn from_key(key: &TaggedKey) -> Option<Self>;
}

#[macro_export]
macro_rules! as_key {
	($key:expr) => {{ <_ as crate::multi::transaction::IntoKey>::into_key($key) }};
}

#[macro_export]
macro_rules! as_encoded {
	($key:expr) => {{ reifydb_core::key::any::TaggedKey::encode(&$crate::as_key!($key)) }};
}

#[macro_export]
macro_rules! as_bound {
	($key:expr) => {{ reifydb_core::key::bound::TaggedKeyBound::Key($crate::as_key!($key)) }};
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
			fn into_key(self) -> TaggedKey {
				let mut ser = KeySerializer::new();
				ser.$extend(self);
				synthetic_key(ser.finish())
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
			fn from_key(key: &TaggedKey) -> Option<Self> {
				KeyDeserializer::from_bytes(&synthetic_tail(key)?).$read().ok()
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
	fn into_key(self) -> TaggedKey {
		let mut ser = KeySerializer::new();
		ser.extend_str(self);
		synthetic_key(ser.finish())
	}
}

impl_kv_for!(i32, extend_i32, read_i32);
impl_kv_for!(u64, extend_u64, read_u64);
impl_kv_for!(String, extend_str, read_str);
