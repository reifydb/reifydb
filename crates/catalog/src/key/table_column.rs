// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::column::ColumnId;
use crate::key::{EncodableKey, KeyKind};
use crate::table::TableId;
use reifydb_core::encoding::keycode;
use reifydb_core::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct TableColumnKey {
    pub table: TableId,
    pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableColumnKey {
    const KIND: KeyKind = KeyKind::TableColumn;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(18);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&self.table.to_be_bytes());
        out.extend(&self.column.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 16);
        Some(Self {
            table: TableId(u64::from_be_bytes(payload[..8].try_into().unwrap())),
            column: ColumnId(u64::from_be_bytes(payload[8..].try_into().unwrap())),
        })
    }
}

impl TableColumnKey {
    pub fn full_scan(table: TableId) -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::link_start(table)), Some(Self::link_end(table)))
    }

    fn link_start(table: TableId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&table.to_be_bytes());
        EncodedKey::new(out)
    }

    fn link_end(table: TableId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&(*table + 1).to_be_bytes());
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::column::ColumnId;
    use crate::key::{EncodableKey, KeyKind, TableColumnKey};
    use crate::table::TableId;

    #[test]
    fn test_encode_decode() {
        let key = TableColumnKey { table: TableId(0xABCD), column: ColumnId(0x12345678) };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            1,
            KeyKind::TableColumn as u8,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0xAB,
            0xCD,
            0x00,
            0x00,
            0x00,
            0x00,
            0x12,
            0x34,
            0x56,
            0x78,
        ];

        assert_eq!(encoded.as_slice(), expected);

        let key = TableColumnKey::decode(1, &expected[2..]).unwrap();
        assert_eq!(key.table, 0xABCD);
        assert_eq!(key.column, 0x12345678);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = TableColumnKey { table: TableId(1), column: ColumnId(100) };
        let key2 = TableColumnKey { table: TableId(1), column: ColumnId(200) };
        let key3 = TableColumnKey { table: TableId(2), column: ColumnId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded1 < encoded2, "row_id ordering not preserved");
        assert!(encoded2 < encoded3, "table_id ordering not preserved");
    }
}
