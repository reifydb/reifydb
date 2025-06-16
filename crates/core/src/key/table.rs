// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::catalog::TableId;
use crate::{EncodableKey, EncodedKey, EncodedKeyRange, KeyKind};

#[derive(Debug)]
pub struct TableKey {
    pub table: TableId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableKey {
    const KIND: KeyKind = KeyKind::Table;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(Self::KIND as u8);
        out.extend(&self.table.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 4);
        Some(Self { table: TableId(u32::from_be_bytes(payload[..].try_into().unwrap())) })
    }
}

impl TableKey {
    pub fn full_scan() -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::table_start()), Some(Self::table_end()))
    }

    fn table_start() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.push(VERSION);
        out.push(KeyKind::Table as u8);
        EncodedKey::new(out)
    }

    fn table_end() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.push(VERSION);
        out.push(KeyKind::Table as u8 + 1);
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::TableId;
    use crate::key::table::TableKey;
    use crate::{EncodableKey, KeyKind};

    #[test]
    fn test_encode_decode() {
        let key = TableKey { table: TableId(0xABCD) };
        let encoded = key.encode();
        let expected = vec![1, KeyKind::Table as u8, 0x00, 0x00, 0xAB, 0xCD];
        assert_eq!(encoded.as_slice(), expected);

        let key = TableKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.table, 0xABCD);
    }
}
