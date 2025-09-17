// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod optimistic;
mod serializable;

use reifydb_core::{CowVec, row::EncodedRow};
pub use reifydb_core::{EncodedKey, util::encoding::keycode};

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
	($key:expr) => {{ reifydb_core::EncodedKey::new(reifydb_core::util::encoding::keycode::serialize(&$key)) }};
}

#[macro_export]
macro_rules! as_row {
	($val:expr) => {{ <_ as crate::mvcc::transaction::IntoRow>::into_row($val) }};
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
