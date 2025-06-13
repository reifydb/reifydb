// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::key::{EncodableKey, KeyKind};
use crate::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct TableRowKey {
    pub table_id: u32,
    pub row_id: u64,
}

const VERSION: u8 = 1;

impl EncodableKey for TableRowKey {
    const KIND: KeyKind = KeyKind::TableRow;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(12);
        out.push(VERSION);
        out.push(Self::KIND as u8);
        out.extend(&self.table_id.to_be_bytes());
        out.extend(&self.row_id.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 12);
        Some(Self {
            table_id: u32::from_be_bytes(payload[..4].try_into().unwrap()),
            row_id: u64::from_be_bytes(payload[4..].try_into().unwrap()),
        })
    }
}

impl TableRowKey {
    pub fn full_scan(table_id: u32) -> EncodedKeyRange {
        EncodedKeyRange::start_end(
            Some(Self::table_start(table_id)),
            Some(Self::table_end(table_id)),
        )
    }

    fn table_start(table_id: u32) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(KeyKind::TableRow as u8);
        out.extend(&table_id.to_be_bytes());
        EncodedKey::new(out)
    }

    fn table_end(table_id: u32) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(KeyKind::TableRow as u8);
        out.extend(&(table_id + 1).to_be_bytes());
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::key::{EncodableKey, KeyKind, TableRowKey};

    #[test]
    fn test_encode_decode() {
        let key = TableRowKey { table_id: 0xABCD, row_id: 0x123456789ABCDEF0 };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            1,
            KeyKind::TableRow as u8,
            0x00,
            0x00,
            0xAB,
            0xCD,
            0x12,
            0x34,
            0x56,
            0x78,
            0x9A,
            0xBC,
            0xDE,
            0xF0,
        ];

        assert_eq!(encoded.as_slice(), expected);

        let key = TableRowKey::decode(1, &expected[2..]).unwrap();
        assert_eq!(key.table_id, 0xABCD);
        assert_eq!(key.row_id, 0x123456789ABCDEF0);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = TableRowKey { table_id: 1, row_id: 100 };
        let key2 = TableRowKey { table_id: 1, row_id: 200 };
        let key3 = TableRowKey { table_id: 2, row_id: 0 };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded1 < encoded2, "row_id ordering not preserved");
        assert!(encoded2 < encoded3, "table_id ordering not preserved");
    }
}
