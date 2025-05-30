// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_storage::Version;
use reifydb_persistence::{Action, Key, Value};
use std::cmp;
use std::cmp::Reverse;

pub enum TransactionValue {
    PendingIter { version: Version, key: Key, value: Value },
    Pending(Pending),
    Committed(Committed),
}

impl core::fmt::Debug for TransactionValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TransactionValue")
            .field("key", self.key())
            .field("version", &self.version())
            .field("value", &self.value())
            .finish()
    }
}

impl Clone for TransactionValue {
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

impl TransactionValue {
    pub fn key(&self) -> &Key {
        match self {
            Self::PendingIter { key, .. } => key,
            Self::Pending(item) => item.key(),
            Self::Committed(item) => item.key(),
        }
    }

    pub fn version(&self) -> u64 {
        match self {
            Self::PendingIter { version, .. } => *version,
            Self::Pending(item) => item.version(),
            Self::Committed(item) => item.version(),
        }
    }

    pub fn value(&self) -> &Value {
        match self {
            Self::PendingIter { value, .. } => value,
            Self::Pending(item) => item.value().expect("value of pending cannot be `None`"),
            Self::Committed(item) => &item.value,
        }
    }

    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed(_))
    }
}

impl From<(Version, Key, Value)> for TransactionValue {
    fn from((version, k, v): (Version, Key, Value)) -> Self {
        Self::PendingIter { version, key: k, value: v }
    }
}

impl From<(Version, &Key, &Value)> for TransactionValue {
    fn from((version, k, v): (Version, &Key, &Value)) -> Self {
        Self::PendingIter { version, key: k.clone(), value: v.clone() }
    }
}

impl From<Pending> for TransactionValue {
    fn from(action: Pending) -> Self {
        Self::Pending(action)
    }
}

impl From<Committed> for TransactionValue {
    fn from(item: Committed) -> Self {
        Self::Committed(item)
    }
}

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

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Pending {
    pub action: Action,
    pub version: Version,
}

impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.action
            .key()
            .cmp(other.action.key())
            .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
    }
}

impl Clone for Pending {
    fn clone(&self) -> Self {
        Self { version: self.version, action: self.action.clone() }
    }
}

impl Pending {
    pub fn action(&self) -> &Action {
        &self.action
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn into_components(self) -> (u64, Action) {
        (self.version, self.action)
    }

    pub fn key(&self) -> &Key {
        &self.action.key()
    }

    pub fn value(&self) -> Option<&Value> {
        self.action.value()
    }

    pub fn was_removed(&self) -> bool {
        matches!(self.action, Action::Remove { .. })
    }
}
