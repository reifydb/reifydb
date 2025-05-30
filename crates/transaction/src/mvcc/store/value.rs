// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::sync::atomic::{AtomicU8, Ordering};
use std::fmt::Debug;

use crossbeam_skiplist::{SkipMap, map::Entry as MapEntry};

use reifydb_core::either::Either;
use reifydb_persistence::{Key, Value};
use crate::Version;

const UNINITIALIZED: u8 = 0;
const LOCKED: u8 = 1;
const UNLOCKED: u8 = 2;

#[derive(Debug)]
pub struct VersionedValue<V> {
	pub(crate) op: AtomicU8,
	values: SkipMap<Version, Option<V>>,
}

impl<V> VersionedValue<V> {
	pub(crate) fn new() -> Self {
		Self { op: AtomicU8::new(UNINITIALIZED), values: SkipMap::new() }
	}

	pub(crate) fn lock(&self) {
		let mut current = UNLOCKED;
		// Spin lock is ok here because the lock is expected to be held for a very short time.
		// and it is hardly contended.
		loop {
			match self.op.compare_exchange_weak(
				current,
				LOCKED,
				Ordering::SeqCst,
				Ordering::Acquire,
			) {
				Ok(_) => return,
				Err(old) => {
					// If the current state is uninitialized, we can directly return.
					// as we are based on SkipMap, let it to handle concurrent write is engouth.
					if old == UNINITIALIZED {
						return;
					}

					current = old;
				}
			}
		}
	}

	pub(crate) fn try_lock(&self) -> bool {
		self.op.compare_exchange(UNLOCKED, LOCKED, Ordering::AcqRel, Ordering::Relaxed).is_ok()
	}

	pub(crate) fn unlock(&self) {
		self.op.store(UNLOCKED, Ordering::Release);
	}
}

impl<V> core::ops::Deref for VersionedValue<V> {
	type Target = SkipMap<u64, Option<V>>;

	fn deref(&self) -> &Self::Target {
		&self.values
	}
}

/// A reference to an entry in the write transaction.
pub struct Entry<'a> {
	pub(crate) item: MapEntry<'a, u64, Option<Value>>,
	pub(crate) key: &'a Key,
	pub(crate) version: Version,
}

impl Clone for Entry<'_> {
	fn clone(&self) -> Self {
		Self { item: self.item.clone(), version: self.version, key: self.key }
	}
}

impl Entry<'_> {
	/// Get the value of the entry.
	pub fn value(&self) -> Option<&Value> {
		self.item.value().as_ref()
	}

	/// Get the key of the entry.
	pub fn key(&self) -> &Key {
		self.key
	}

	/// Get the version of the entry.
	pub fn version(&self) -> u64 {
		self.version
	}
}

/// A reference to an entry in the write transaction.
pub struct ValueRef<'a>(pub Either<&'a Value, Entry<'a>>);

impl Debug for ValueRef<'_> {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		core::ops::Deref::deref(self).fmt(f)
	}
}

impl core::fmt::Display for ValueRef<'_> {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		core::ops::Deref::deref(self).fmt(f)
	}
}

impl Clone for ValueRef<'_> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl core::ops::Deref for ValueRef<'_> {
	type Target = Value;

	fn deref(&self) -> &Self::Target {
		match &self.0 {
			Either::Left(v) => v,
			Either::Right(item) => {
				item.value().expect("the value of `Entry` in `ValueRef` cannot be `None`")
			}
		}
	}
}


impl PartialEq<Value> for ValueRef<'_> {
	fn eq(&self, other: &Value) -> bool {
		core::ops::Deref::deref(self).eq(other)
	}
}

impl PartialEq<&Value> for ValueRef<'_> {
	fn eq(&self, other: &&Value) -> bool {
		core::ops::Deref::deref(self).eq(*other)
	}
}
