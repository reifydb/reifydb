// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::{ColumnId, TableId};
use crate::util::encoding::keycode;
use crate::{EncodedKey, EncodedKeyRange};

#[derive(Debug, Clone, PartialEq)]
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
        out.extend(&keycode::serialize(&self.table));
        out.extend(&keycode::serialize(&self.column));
        EncodedKey::new(out)
    }

    fn decode(key: &EncodedKey) -> Option<Self> {
        if key.len() < 2 {
            return None;
        }

        let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
        if version != VERSION {
            return None;
        }

        let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
        if kind != Self::KIND {
            return None;
        }

        let payload = &key[2..];
        if payload.len() != 16 {
            return None;
        }

        keycode::deserialize(&payload[..8])
            .ok()
            .zip(keycode::deserialize(&payload[8..]).ok())
            .map(|(table, column)| Self { table, column })
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
        out.extend(&keycode::serialize(&table));
        EncodedKey::new(out)
    }

    fn link_end(table: TableId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&(*table - 1)));
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::{EncodableKey, TableColumnKey};
    use crate::interface::catalog::ColumnId;
    use crate::interface::catalog::TableId;

    #[test]
    fn test_encode_decode() {
        let key = TableColumnKey { table: TableId(0xABCD), column: ColumnId(0x123456789ABCDEF0) };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            0xFE, // version
            0xF8, // kind
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
            0x21, 0x0F,
        ];

        assert_eq!(encoded.as_slice(), expected);

        let key = TableColumnKey::decode(&encoded).unwrap();
        assert_eq!(key.table, 0xABCD);
        assert_eq!(key.column, 0x123456789ABCDEF0);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = TableColumnKey { table: TableId(1), column: ColumnId(100) };
        let key2 = TableColumnKey { table: TableId(1), column: ColumnId(200) };
        let key3 = TableColumnKey { table: TableId(2), column: ColumnId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded3 < encoded2, "ordering not preserved");
        assert!(encoded2 < encoded1, "ordering not preserved");
    }
}
