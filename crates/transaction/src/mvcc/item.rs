// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::cmp::{self, Reverse};
use reifydb_persistence::{Action, Key, Value};

/// The reference of the [`ToWrite`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ItemRef<'a> {
    /// The data reference of the entry.
    pub data: ItemDataRef<'a>,
    /// The version of the entry.
    pub version: u64,
}

impl Clone for ItemRef<'_> {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for ItemRef<'_> {}

impl ItemRef<'_> {
    /// Get the key of the entry.
    pub const fn key(&self) -> &Key {
        match self.data {
            ItemDataRef::Insert { key, .. } => key,
            ItemDataRef::Remove(key) => key,
        }
    }

    /// Get the value of the entry, if None, it means the entry is removed.
    pub const fn value(&self) -> Option<&Value> {
        match self.data {
            ItemDataRef::Insert { value, .. } => Some(value),
            ItemDataRef::Remove(_) => None,
        }
    }

    /// Returns the version of the entry.
    ///
    /// This version is useful when you want to implement MVCC.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

/// The reference of the [`ItemData`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum ItemDataRef<'a> {
    /// Insert the key and the value.
    Insert {
        /// key of the entry.
        key: &'a Key,
        /// value of the entry.
        value: &'a Value,
    },
    /// Remove the key.
    Remove(&'a Key),
}

impl Clone for ItemDataRef<'_> {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for ItemDataRef<'_> {}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToWrite {
    pub action: Action,
    pub version: u64,
}

impl PartialOrd for ToWrite {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ToWrite {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.action
            .key()
            .cmp(other.action.key())
            .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
    }
}

impl Clone for ToWrite {
    fn clone(&self) -> Self {
        Self { version: self.version, action: self.action.clone() }
    }
}

impl ToWrite {
    pub const fn action(&self) -> &Action {
        &self.action
    }

    pub const fn version(&self) -> u64 {
        self.version
    }

    pub fn into_components(self) -> (u64, Action) {
        (self.version, self.action)
    }

    pub fn key(&self) -> &Key {
        &self.action.key()
    }

    pub fn split(self) -> (Key, EntryValue<Value>) {
        let ToWrite { action: data, version } = self;

        let (key, value) = match data {
            Action::Set { key, value } => (key, Some(value)),
            Action::Remove { key } => (key, None),
        };
        (key, EntryValue { value, version })
    }

    pub fn unsplit(key: Key, value: EntryValue<Value>) -> Self {
        let EntryValue { value, version } = value;
        ToWrite {
            action: match value {
                Some(value) => Action::Set { key, value },
                None => Action::Remove { key },
            },
            version,
        }
    }
}

/// A entry value
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EntryValue<V> {
    /// The version of the entry.
    pub version: u64,
    /// The value of the entry.
    pub value: Option<V>,
}

impl<V> Clone for EntryValue<V>
where
    V: Clone,
{
    fn clone(&self) -> Self {
        Self { version: self.version, value: self.value.clone() }
    }
}
