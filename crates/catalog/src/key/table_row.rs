// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::key::{EncodableKey, KeyKind};
use crate::table::TableId;
use reifydb_core::catalog::RowId;
use reifydb_core::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct TableRowKey {
    pub table: TableId,
    pub row: RowId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableRowKey {
    const KIND: KeyKind = KeyKind::TableRow;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(12);
        out.push(VERSION);
        out.push(Self::KIND as u8);
        out.extend(&self.table.to_be_bytes());
        out.extend(&self.row.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 12);
        Some(Self {
            table: TableId(u32::from_be_bytes(payload[..4].try_into().unwrap())),
            row: RowId(u64::from_be_bytes(payload[4..].try_into().unwrap())),
        })
    }
}

impl TableRowKey {
    pub fn full_scan(table_id: TableId) -> EncodedKeyRange {
        EncodedKeyRange::start_end(
            Some(Self::table_start(table_id)),
            Some(Self::table_end(table_id)),
        )
    }

    fn table_start(table_id: TableId) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(KeyKind::TableRow as u8);
        out.extend(&table_id.to_be_bytes());
        EncodedKey::new(out)
    }

    fn table_end(table_id: TableId) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(KeyKind::TableRow as u8);
        out.extend(&(*table_id + 1).to_be_bytes());
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::key::{EncodableKey, KeyKind, TableRowKey};
    use crate::table::TableId;
    use reifydb_core::catalog::RowId;

    #[test]
    fn test_encode_decode() {
        let key = TableRowKey { table: TableId(0xABCD), row: RowId(0x123456789ABCDEF0) };
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
        assert_eq!(key.table, 0xABCD);
        assert_eq!(key.row, 0x123456789ABCDEF0);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = TableRowKey { table: TableId(1), row: RowId(100) };
        let key2 = TableRowKey { table: TableId(1), row: RowId(200) };
        let key3 = TableRowKey { table: TableId(2), row: RowId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded1 < encoded2, "row_id ordering not preserved");
        assert!(encoded2 < encoded3, "table_id ordering not preserved");
    }
}
