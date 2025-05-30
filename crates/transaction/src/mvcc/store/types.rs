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
use crate::mvcc::store::value::ValueRef;
use crate::mvcc::types::Pending;
use reifydb_core::either::Either;
use reifydb_persistence::{Key, Value};

/// Represents a committed key value pair of a specific version
#[derive(Clone, Debug)]
pub struct Committed {
    pub(crate) key: Key,
    pub(crate) value: Value,
    pub(crate) version: Version,
}

impl Committed {
    pub fn value(&self) -> &Value {
        &self.value
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn version(&self) -> Version {
        self.version
    }
}

enum RefKind {
    PendingIter { version: Version, key: Key, value: Value },
    Pending(Pending),
    Committed(Committed),
}

impl core::fmt::Debug for Ref {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Ref")
            .field("key", self.0.key())
            .field("version", &self.0.version())
            .field("value", &self.0.value())
            .finish()
    }
}

impl Clone for RefKind {
    fn clone(&self) -> Self {
        match self {
            Self::Committed(item) => Self::Committed(item.clone()),
            Self::Pending(action) => Self::Pending(action.clone()),
            Self::PendingIter { version, key, value } => {
                Self::PendingIter { version: *version, key: key.clone(), value: value.clone() }
            }
        }
    }
}

impl RefKind {
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
            Self::Committed(item) => ValueRef(Either::Left(&item.value)),
        }
    }

    fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }
}

pub struct Ref(RefKind);

impl Clone for Ref {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl From<(Version, Key, Value)> for Ref {
    fn from((version, k, v): (Version, Key, Value)) -> Self {
        Self(RefKind::PendingIter { version, key: k, value: v })
    }
}

impl From<(Version, &Key, &Value)> for Ref {
    fn from((version, k, v): (Version, &Key, &Value)) -> Self {
        Self(RefKind::PendingIter { version, key: k.clone(), value: v.clone() })
    }
}

impl From<Pending> for Ref {
    fn from(action: Pending) -> Self {
        Self(RefKind::Pending(action))
    }
}

impl From<Committed> for Ref {
    fn from(item: Committed) -> Self {
        Self(RefKind::Committed(item))
    }
}

impl Ref {
    pub fn key(&self) -> &Key {
        self.0.key()
    }

    pub fn version(&self) -> u64 {
        self.0.version()
    }

    pub fn value(&self) -> ValueRef<'_> {
        self.0.value()
    }

    pub fn is_committed(&self) -> bool {
        self.0.is_committed()
    }
}
