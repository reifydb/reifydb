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

use crossbeam_skiplist::{SkipMap, map::Entry as MapEntry};

use either::Either;

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
pub struct Entry<'a, K, V> {
    ent: MapEntry<'a, u64, Option<V>>,
    key: &'a K,
    version: u64,
}

impl<K, V> Clone for Entry<'_, K, V> {
    fn clone(&self) -> Self {
        Self { ent: self.ent.clone(), version: self.version, key: self.key }
    }
}

impl<K, V> Entry<'_, K, V> {
    /// Get the value of the entry.

    pub fn value(&self) -> Option<&V> {
        self.ent.value().as_ref()
    }

    /// Get the key of the entry.

    pub const fn key(&self) -> &K {
        self.key
    }

    /// Get the version of the entry.

    pub const fn version(&self) -> u64 {
        self.version
    }
}

/// A reference to an entry in the write transaction.
pub struct ValueRef<'a, K, V>(Either<&'a V, Entry<'a, K, V>>);

impl<K, V: core::fmt::Debug> core::fmt::Debug for ValueRef<'_, K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::ops::Deref::deref(self).fmt(f)
    }
}

impl<K, V: core::fmt::Display> core::fmt::Display for ValueRef<'_, K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::ops::Deref::deref(self).fmt(f)
    }
}

impl<K, V> Clone for ValueRef<'_, K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K, V> core::ops::Deref for ValueRef<'_, K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Either::Left(v) => v,
            Either::Right(ent) => {
                ent.value().expect("the value of `Entry` in `ValueRef` cannot be `None`")
            }
        }
    }
}

impl<K, V> ValueRef<'_, K, V> {
    /// Returns `true` if the value was commited.

    pub const fn is_committed(&self) -> bool {
        matches!(self.0, Either::Right(_))
    }
}

impl<K, V> PartialEq<V> for ValueRef<'_, K, V>
where
    V: PartialEq,
{
    fn eq(&self, other: &V) -> bool {
        core::ops::Deref::deref(self).eq(other)
    }
}

impl<K, V> PartialEq<&V> for ValueRef<'_, K, V>
where
    V: PartialEq,
{
    fn eq(&self, other: &&V) -> bool {
        core::ops::Deref::deref(self).eq(other)
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

use super::*;
use crate::mvcc::version::types::EntryRef;

/// A reference to an entry in the write transaction.
#[derive(Debug)]
pub struct CommittedRef<'a, K, V> {
    pub(crate) ent: MapEntry<'a, K, Values<V>>,
    pub(crate) version: u64,
}

impl<K, V> Clone for CommittedRef<'_, K, V> {
    fn clone(&self) -> Self {
        Self { ent: self.ent.clone(), version: self.version }
    }
}

impl<K, V> CommittedRef<'_, K, V> {
    /// Get the value of the entry.

    fn entry(&self) -> Entry<'_, K, V> {
        let ent = self.ent.value().get(&self.version).unwrap();

        Entry { ent, key: self.ent.key(), version: self.version }
    }

    /// Get the key of the ref.

    pub fn value(&self) -> ValueRef<'_, K, V> {
        ValueRef(Either::Right(self.entry()))
    }

    /// Get the key of the ref.

    pub fn key(&self) -> &K {
        self.ent.key()
    }

    /// Get the version of the entry.

    pub const fn version(&self) -> u64 {
        self.version
    }
}

enum RefKind<'a, K, V> {
    PendingIter { version: u64, key: &'a K, value: &'a V },
    Pending(EntryRef<'a, K, V>),
    Committed(CommittedRef<'a, K, V>),
}

impl<K: core::fmt::Debug, V: core::fmt::Debug> core::fmt::Debug for Ref<'_, K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Ref")
            .field("key", self.0.key())
            .field("version", &self.0.version())
            .field("value", &self.0.value())
            .finish()
    }
}

impl<K, V> Clone for RefKind<'_, K, V> {
    fn clone(&self) -> Self {
        match self {
            Self::Committed(ent) => Self::Committed(ent.clone()),
            Self::Pending(ent) => Self::Pending(*ent),
            Self::PendingIter { version, key, value } => {
                Self::PendingIter { version: *version, key: *key, value: *value }
            }
        }
    }
}

impl<K, V> RefKind<'_, K, V> {
    fn key(&self) -> &K {
        match self {
            Self::PendingIter { key, .. } => key,
            Self::Pending(ent) => ent.key(),
            Self::Committed(ent) => ent.key(),
        }
    }

    fn version(&self) -> u64 {
        match self {
            Self::PendingIter { version, .. } => *version,
            Self::Pending(ent) => ent.version(),
            Self::Committed(ent) => ent.version(),
        }
    }

    fn value(&self) -> ValueRef<'_, K, V> {
        match self {
            Self::PendingIter { value, .. } => ValueRef(Either::Left(value)),
            Self::Pending(ent) => ValueRef(Either::Left(
                ent.value().expect("value of pending entry cannot be `None`"),
            )),
            Self::Committed(ent) => ValueRef(Either::Right(ent.entry())),
        }
    }

    fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }
}

/// A reference to an entry in the write transaction.
pub struct Ref<'a, K, V>(RefKind<'a, K, V>);

impl<K, V> Clone for Ref<'_, K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<'a, K, V> From<(u64, &'a K, &'a V)> for Ref<'a, K, V> {
    fn from((version, k, v): (u64, &'a K, &'a V)) -> Self {
        Self(RefKind::PendingIter { version, key: k, value: v })
    }
}

impl<'a, K, V> From<EntryRef<'a, K, V>> for Ref<'a, K, V> {
    fn from(ent: EntryRef<'a, K, V>) -> Self {
        Self(RefKind::Pending(ent))
    }
}

impl<'a, K, V> From<CommittedRef<'a, K, V>> for Ref<'a, K, V> {
    fn from(ent: CommittedRef<'a, K, V>) -> Self {
        Self(RefKind::Committed(ent))
    }
}

impl<K, V> Ref<'_, K, V> {
    /// Returns the value of the key.

    pub fn key(&self) -> &K {
        self.0.key()
    }

    /// Returns the read version of the entry.

    pub fn version(&self) -> u64 {
        self.0.version()
    }

    /// Returns the value of the entry.

    pub fn value(&self) -> ValueRef<'_, K, V> {
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
