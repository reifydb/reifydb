// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::Version;
use crate::mvcc::store::value::{Entry, ValueRef, VersionedValue};
use crate::mvcc::types::TransactionAction;
use crossbeam_skiplist::map::Entry as MapEntry;
use reifydb_core::either::Either;
use reifydb_persistence::{Key, Value};

/// A reference to an entry in the write transaction.
#[derive(Debug)]
pub struct CommittedRef<'a> {
    pub(crate) item: MapEntry<'a, Key, VersionedValue<Value>>,
    pub(crate) version: Version,
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
    PendingIter { version: Version, key: &'a Key, value: &'a Value },
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

        let values = VersionedValue::<()>::new();
        takes_send(values);
    }
}
