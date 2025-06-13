// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::AsyncCowVec;
use crate::key::schema::SchemaKey;
use crate::key::table::TableKey;
pub use range::EncodedKeyRange;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
pub use table_row::TableRowKey;

mod range;
mod schema;
mod table;
mod table_row;

#[derive(Debug, Clone, PartialOrd, Ord, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncodedKey(pub AsyncCowVec<u8>);

impl Deref for EncodedKey {
    type Target = AsyncCowVec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl EncodedKey {
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self(AsyncCowVec::new(key.into()))
    }
}

#[derive(Debug)]
pub enum Key {
    Schema(SchemaKey),
    Table(TableKey),
    TableRow(TableRowKey),
}

impl Key {
    pub fn encode(&self) -> EncodedKey {
        match &self {
            Key::Schema(key) => key.encode(),
            Key::Table(key) => key.encode(),
            Key::TableRow(key) => key.encode(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Schema = 0x01,
    Table = 0x02,
    TableRow = 0x03,
}

impl TryFrom<u8> for KeyKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Schema),
            0x02 => Ok(Self::Table),
            0x03 => Ok(Self::TableRow),
            _ => Err(()),
        }
    }
}

pub trait EncodableKey {
    const KIND: KeyKind;

    fn encode(&self) -> EncodedKey;

    fn decode(version: u8, payload: &[u8]) -> Option<Self>
    where
        Self: Sized;
}

impl Key {
    pub fn decode(key: &EncodedKey) -> Option<Self> {
        let version = *key.get(0)?;
        let kind = *key.get(1)?;
        let payload = &key[2..];

        match KeyKind::try_from(kind).ok()? {
            KeyKind::Schema => SchemaKey::decode(version, payload).map(Self::Schema),
            KeyKind::Table => TableKey::decode(version, payload).map(Self::Table),
            KeyKind::TableRow => TableRowKey::decode(version, payload).map(Self::TableRow),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::key::schema::SchemaKey;
    use crate::key::table::TableKey;
    use crate::key::{Key, TableRowKey};

    #[test]
    fn test_schema() {
        let key = Key::Schema(SchemaKey { schema_id: 42 });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::Schema(decoded_inner) => {
                assert_eq!(decoded_inner.schema_id, 42);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_table() {
        let key = Key::Table(TableKey { table_id: 42 });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::Table(decoded_inner) => {
                assert_eq!(decoded_inner.table_id, 42);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_table_row() {
        let key = Key::TableRow(TableRowKey { table_id: 42, row_id: 999_999 });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::TableRow(decoded_inner) => {
                assert_eq!(decoded_inner.table_id, 42);
                assert_eq!(decoded_inner.row_id, 999_999);
            }
            _ => unreachable!(),
        }
    }
}
