// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::RowId;
use crate::interface::catalog::TableId;
use crate::util::encoding::keycode;
use crate::{EncodedKey, EncodedKeyRange};

#[derive(Debug, Clone,PartialEq)]
pub struct TableRowKey {
    pub table: TableId,
    pub row: RowId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableRowKey {
    const KIND: KeyKind = KeyKind::TableRow;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(18);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.table));
        out.extend(&keycode::serialize(&self.row));

        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        if payload.len() != 16 {
            return None;
        }

        keycode::deserialize(&payload[..8])
            .ok()
            .zip(keycode::deserialize(&payload[8..]).ok())
            .map(|(table, row)| Self { table, row })
    }
}

impl TableRowKey {
    pub fn full_scan(table_id: TableId) -> EncodedKeyRange {
        EncodedKeyRange::start_end(
            Some(Self::table_start(table_id)),
            Some(Self::table_end(table_id)),
        )
    }

    pub fn table_start(table_id: TableId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&table_id));
        EncodedKey::new(out)
    }

    pub fn table_end(table_id: TableId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&(*table_id - 1)));
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::{EncodableKey, TableRowKey};
    use crate::RowId;
    use crate::interface::catalog::TableId;

    #[test]
    fn test_encode_decode() {
        let key = TableRowKey { table: TableId(0xABCD), row: RowId(0x123456789ABCDEF0) };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            0xFE, // version
            0xFC, // kind
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
            0x21, 0x0F,
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

        assert!(encoded3 < encoded2, "ordering not preserved");
        assert!(encoded2 < encoded1, "ordering not preserved");
    }
}
