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

const UNINITIALIZED: u8 = 0;
const LOCKED: u8 = 1;
const UNLOCKED: u8 = 2;

#[derive(Debug)]
#[doc(hidden)]
pub struct Values<V> {
    pub(crate) op: AtomicU8,
    values: SkipMap<u64, Option<V>>,
}

impl<V> Values<V> {
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

impl<V> core::ops::Deref for Values<V> {
    type Target = SkipMap<u64, Option<V>>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

/// A reference to an entry in the write transaction.
pub struct Entry<'a> {
    item: MapEntry<'a, u64, Option<Value>>,
    key: &'a Key,
    version: u64,
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
    pub const fn key(&self) -> &Key {
        self.key
    }

    /// Get the version of the entry.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

/// A reference to an entry in the write transaction.
pub struct ValueRef<'a>(Either<&'a Value, Entry<'a>>);

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

impl ValueRef<'_> {
    /// Returns `true` if the value was commited.

    pub const fn is_committed(&self) -> bool {
        matches!(self.0, Either::Right(_))
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

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::types::TransactionAction;
use reifydb_persistence::{Key, Value};

/// A reference to an entry in the write transaction.
#[derive(Debug)]
pub struct CommittedRef<'a> {
    pub(crate) item: MapEntry<'a, Key, Values<Value>>,
    pub(crate) version: u64,
}

impl Clone for CommittedRef<'_> {
    fn clone(&self) -> Self {
        Self { item: self.item.clone(), version: self.version }
    }
}

impl CommittedRef<'_> {
    /// Get the value of the entry.
    fn entry(&self) -> Entry<'_> {
        let item = self.item.value().get(&self.version).unwrap();

        Entry { item, key: self.item.key(), version: self.version }
    }

    /// Get the key of the ref.
    pub fn value(&self) -> ValueRef<'_> {
        ValueRef(Either::Right(self.entry()))
    }

    /// Get the key of the ref.
    pub fn key(&self) -> &Key {
        self.item.key()
    }

    /// Get the version of the entry.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

enum RefKind<'a> {
    PendingIter { version: u64, key: &'a Key, value: &'a Value },
    Pending(TransactionAction),
    Committed(CommittedRef<'a>),
}

impl core::fmt::Debug for Ref<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Ref")
            .field("key", self.0.key())
            .field("version", &self.0.version())
            .field("value", &self.0.value())
            .finish()
    }
}

impl Clone for RefKind<'_> {
    fn clone(&self) -> Self {
        match self {
            Self::Committed(item) => Self::Committed(item.clone()),
            Self::Pending(action) => Self::Pending(action.clone()),
            Self::PendingIter { version, key, value } => {
                Self::PendingIter { version: *version, key: *key, value: *value }
            }
        }
    }
}

impl RefKind<'_> {
    fn key(&self) -> &Key {
        match self {
            Self::PendingIter { key, .. } => key,
            Self::Pending(item) => item.key(),
            Self::Committed(item) => item.key(),
        }
    }

    fn version(&self) -> u64 {
        match self {
            Self::PendingIter { version, .. } => *version,
            Self::Pending(item) => item.version(),
            Self::Committed(item) => item.version(),
        }
    }

    fn value(&self) -> ValueRef<'_> {
        match self {
            Self::PendingIter { value, .. } => ValueRef(Either::Left(value)),
            Self::Pending(item) => ValueRef(Either::Left(
                item.value().expect("value of pending entry cannot be `None`"),
            )),
            Self::Committed(item) => ValueRef(Either::Right(item.entry())),
        }
    }

    fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }
}

/// A reference to an entry in the write transaction.
pub struct Ref<'a>(RefKind<'a>);

impl Clone for Ref<'_> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<'a> From<(u64, &'a Key, &'a Value)> for Ref<'a> {
    fn from((version, k, v): (u64, &'a Key, &'a Value)) -> Self {
        Self(RefKind::PendingIter { version, key: k, value: v })
    }
}

impl<'a> From<TransactionAction> for Ref<'a> {
    fn from(action: TransactionAction) -> Self {
        Self(RefKind::Pending(action))
    }
}

impl<'a> From<CommittedRef<'a>> for Ref<'a> {
    fn from(item: CommittedRef<'a>) -> Self {
        Self(RefKind::Committed(item))
    }
}

impl Ref<'_> {
    /// Returns the value of the key.

    pub fn key(&self) -> &Key {
        self.0.key()
    }

    /// Returns the read version of the entry.

    pub fn version(&self) -> u64 {
        self.0.version()
    }

    /// Returns the value of the entry.

    pub fn value(&self) -> ValueRef<'_> {
        self.0.value()
    }

    /// Returns `true` if the entry was commited.

    pub fn is_committed(&self) -> bool {
        self.0.is_committed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_send() {
        fn takes_send<T: Send>(_t: T) {}

        let values = Values::<()>::new();
        takes_send(values);
    }
}
