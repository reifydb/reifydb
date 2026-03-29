// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod begin;
mod get;
mod iter;
mod range;
mod replica;
mod rollback;
mod version;
mod write;
mod write_skew;

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	util::encoding::keycode,
};
use reifydb_transaction::multi::transaction::MultiTransaction;
use reifydb_type::util::cowvec::CowVec;

pub fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

pub trait IntoValues {
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
	($key:expr) => {{ reifydb_core::encoded::key::EncodedKey::new(reifydb_core::util::encoding::keycode::serialize(&$key)) }};
}

#[macro_export]
macro_rules! as_values {
	($val:expr) => {{ <_ as crate::multi::transaction::IntoValues>::into_row($val) }};
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
		impl IntoValues for $t {
			fn into_row(self) -> EncodedRow {
				EncodedRow(CowVec::new(keycode::serialize(&self)))
			}
		}
		impl FromKey for $t {
			fn from_key(key: &EncodedKey) -> Option<Self> {
				keycode::deserialize(key).ok()
			}
		}
		impl FromRow for $t {
			fn from_row(row: &EncodedRow) -> Option<Self> {
				keycode::deserialize(&row.0).ok()
			}
		}
	};
}

impl_kv_for!(i32);
impl_kv_for!(u64);
impl_kv_for!(String);
