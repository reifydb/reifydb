// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::key::{EncodableKey, KeyKind};
use crate::schema::SchemaId;
use crate::table::TableId;
use reifydb_core::util::encoding::keycode;
use reifydb_core::{EncodedKey, EncodedKeyRange};

#[derive(Debug, Clone, PartialEq)]
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
        out.extend(&keycode::serialize(&self.schema));
        out.extend(&keycode::serialize(&self.table));
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 16);

        keycode::deserialize(&payload[..8])
            .ok()
            .zip(keycode::deserialize(&payload[8..]).ok())
            .map(|(schema, table)| Self { schema, table })
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
        out.extend(&keycode::serialize(&schema_id));
        EncodedKey::new(out)
    }

    fn link_end(schema_id: SchemaId) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&(*schema_id - 1)));
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::key::{EncodableKey, SchemaTableKey};
    use crate::schema::SchemaId;
    use crate::table::TableId;

    #[test]
    fn test_encode_decode() {
        let key = SchemaTableKey { schema: SchemaId(0xABCD), table: TableId(0x123456789ABCDEF0) };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            0xFE, // version
            0xFB, // kind
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
            0x21, 0x0F,
        ];

        assert_eq!(encoded.as_slice(), expected);

        let key = SchemaTableKey::decode(1, &expected[2..]).unwrap();
        assert_eq!(key.schema, 0xABCD);
        assert_eq!(key.table, 0x123456789ABCDEF0);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = SchemaTableKey { schema: SchemaId(1), table: TableId(100) };
        let key2 = SchemaTableKey { schema: SchemaId(1), table: TableId(200) };
        let key3 = SchemaTableKey { schema: SchemaId(2), table: TableId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded3 < encoded2, "ordering not preserved");
        assert!(encoded2 < encoded1, "ordering not preserved");
    }
}
