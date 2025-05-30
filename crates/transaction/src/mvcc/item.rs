// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0


use crate::{Key, Value};
use core::cmp::{self, Reverse};

/// The reference of the [`Item`].
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

/// The data of the [`Item`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum ItemData {
    /// Insert the key and the value.
    Set {
        /// key of the entry.
        key: Key,
        /// value of the entry.
        value: Value,
    },
    /// Remove the key.
    Remove(Key),
}

impl PartialOrd for ItemData {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ItemData {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.key().cmp(other.key())
    }
}

impl ItemData {
    /// Returns the key of the entry.
    pub const fn key(&self) -> &Key {
        match self {
            Self::Set { key, .. } => key,
            Self::Remove(key) => key,
        }
    }

    /// Returns the value of the entry, if None, it means the entry is marked as remove.
    pub const fn value(&self) -> Option<&Value> {
        match self {
            Self::Set { value, .. } => Some(value),
            Self::Remove(_) => None,
        }
    }
}

impl Clone for ItemData {
    fn clone(&self) -> Self {
        match self {
            Self::Set { key, value } => Self::Set { key: key.clone(), value: value.clone() },
            Self::Remove(key) => Self::Remove(key.clone()),
        }
    }
}

/// An entry can be persisted to the database.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Item {
    /// The version of the entry.
    pub version: u64,
    /// The data of the entry.
    pub data: ItemData,
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.data
            .key()
            .cmp(other.data.key())
            .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
    }
}

impl Clone for Item {
    fn clone(&self) -> Self {
        Self { version: self.version, data: self.data.clone() }
    }
}

impl Item {
    /// Returns the data contained by the entry.

    pub const fn data(&self) -> &ItemData {
        &self.data
    }

    /// Returns the version (can also be tought as transaction timestamp) of the entry.

    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Consumes the entry and returns the version and the entry data.

    pub fn into_components(self) -> (u64, ItemData) {
        (self.version, self.data)
    }

    /// Returns the key of the entry.

    pub fn key(&self) -> &Key {
        match &self.data {
            ItemData::Set { key, .. } => key,
            ItemData::Remove(key) => key,
        }
    }

    /// Split the entry into its key and [`EntryValue`].
    pub fn split(self) -> (Key, EntryValue<Value>) {
        let Item { data, version } = self;

        let (key, value) = match data {
            ItemData::Set { key, value } => (key, Some(value)),
            ItemData::Remove(key) => (key, None),
        };
        (key, EntryValue { value, version })
    }

    /// Unsplit the key and [`EntryValue`] into an entry.
    pub fn unsplit(key: Key, value: EntryValue<Value>) -> Self {
        let EntryValue { value, version } = value;
        Item {
            data: match value {
                Some(value) => ItemData::Set { key, value },
                None => ItemData::Remove(key),
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
