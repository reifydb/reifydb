// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::AsyncCowVec;
pub use range::EncodedKeyRange;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
pub use table::TableRowKey;

mod range;
mod table;

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
    TableRow(TableRowKey),
}

impl Key {
    pub fn encode(&self) -> EncodedKey {
        match &self {
            Key::TableRow(key) => key.encode(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    TableRow = 0x01,
}

impl TryFrom<u8> for KeyKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::TableRow),
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
            KeyKind::TableRow => TableRowKey::decode(version, payload).map(Key::TableRow),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::key::{Key, TableRowKey};

    #[test]
    fn test_encode_decode_key_table_row() {
        let key = Key::TableRow(TableRowKey { table_id: 42, row_id: 999_999 });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::TableRow(decoded_inner) => {
                assert_eq!(decoded_inner.table_id, 42);
                assert_eq!(decoded_inner.row_id, 999_999);
            }
        }
    }
}
