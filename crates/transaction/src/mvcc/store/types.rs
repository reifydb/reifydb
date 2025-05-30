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
use crate::mvcc::types::Pending;
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

pub enum Ref {
    PendingIter { version: Version, key: Key, value: Value },
    Pending(Pending),
    Committed(Committed),
}

impl core::fmt::Debug for Ref {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Ref")
            .field("key", self.key())
            .field("version", &self.version())
            .field("value", &self.value())
            .finish()
    }
}

impl Clone for Ref {
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

impl Ref {
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

impl From<(Version, Key, Value)> for Ref {
    fn from((version, k, v): (Version, Key, Value)) -> Self {
        Self::PendingIter { version, key: k, value: v }
    }
}

impl From<(Version, &Key, &Value)> for Ref {
    fn from((version, k, v): (Version, &Key, &Value)) -> Self {
        Self::PendingIter { version, key: k.clone(), value: v.clone() }
    }
}

impl From<Pending> for Ref {
    fn from(action: Pending) -> Self {
        Self::Pending(action)
    }
}

impl From<Committed> for Ref {
    fn from(item: Committed) -> Self {
        Self::Committed(item)
    }
}
