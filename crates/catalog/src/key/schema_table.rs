// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::key::{EncodableKey, KeyKind};
use crate::schema::SchemaId;
use crate::table::TableId;
use reifydb_core::encoding::keycode;
use reifydb_core::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct SchemaTableKey {
    pub schema: SchemaId,
    pub table: TableId,
}

const VERSION: u8 = 1;

impl EncodableKey for SchemaTableKey {
    const KIND: KeyKind = KeyKind::SchemaTable;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(18);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&self.schema.to_be_bytes());
        out.extend(&self.table.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 16);
        Some(Self {
            schema: SchemaId(u64::from_be_bytes(payload[..8].try_into().unwrap())),
            table: TableId(u64::from_be_bytes(payload[8..].try_into().unwrap())),
        })
    }
}

impl SchemaTableKey {
    pub fn full_scan(schema_id: SchemaId) -> EncodedKeyRange {
        EncodedKeyRange::start_end(
            Some(Self::link_start(schema_id)),
            Some(Self::link_end(schema_id)),
        )
    }

    fn link_start(schema_id: SchemaId) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&schema_id.to_be_bytes());
        EncodedKey::new(out)
    }

    fn link_end(schema_id: SchemaId) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&(*schema_id + 1).to_be_bytes());
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::key::{EncodableKey, KeyKind, SchemaTableKey};
    use crate::schema::SchemaId;
    use crate::table::TableId;

    #[test]
    fn test_encode_decode() {
        let key = SchemaTableKey { schema: SchemaId(0x12345678), table: TableId(0xABCD) };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            1,
            KeyKind::SchemaTable as u8,
            0x00,
            0x00,
            0x00,
            0x00,
            0x12,
            0x34,
            0x56,
            0x78,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0xAB,
            0xCD,
        ];

        assert_eq!(encoded.as_slice(), expected);

        let key = SchemaTableKey::decode(1, &expected[2..]).unwrap();
        assert_eq!(key.schema, 0x12345678);
        assert_eq!(key.table, 0xABCD);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = SchemaTableKey { schema: SchemaId(1), table: TableId(100) };
        let key2 = SchemaTableKey { schema: SchemaId(1), table: TableId(200) };
        let key3 = SchemaTableKey { schema: SchemaId(2), table: TableId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded1 < encoded2, "row_id ordering not preserved");
        assert!(encoded2 < encoded3, "table_id ordering not preserved");
    }
}
