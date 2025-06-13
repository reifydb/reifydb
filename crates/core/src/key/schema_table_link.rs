// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::catalog::{SchemaId, TableId};
use crate::key::{EncodableKey, KeyKind};
use crate::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct SchemaTableLinkKey {
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

const VERSION: u8 = 1;

impl EncodableKey for SchemaTableLinkKey {
    const KIND: KeyKind = KeyKind::SchemaTableLink;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(8);
        out.push(VERSION);
        out.push(Self::KIND as u8);
        out.extend(&self.schema_id.to_be_bytes());
        out.extend(&self.table_id.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 8);
        Some(Self {
            schema_id: SchemaId(u32::from_be_bytes(payload[..4].try_into().unwrap())),
            table_id: TableId(u32::from_be_bytes(payload[4..].try_into().unwrap())),
        })
    }
}

impl SchemaTableLinkKey {
    pub fn full_scan(schema_id: SchemaId) -> EncodedKeyRange {
        EncodedKeyRange::start_end(
            Some(Self::link_start(schema_id)),
            Some(Self::link_end(schema_id)),
        )
    }

    fn link_start(schema_id: SchemaId) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(KeyKind::SchemaTableLink as u8);
        out.extend(&schema_id.to_be_bytes());
        EncodedKey::new(out)
    }

    fn link_end(schema_id: SchemaId) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(KeyKind::SchemaTableLink as u8);
        out.extend(&(*schema_id + 1).to_be_bytes());
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{SchemaId, TableId};
    use crate::key::{EncodableKey, KeyKind, SchemaTableLinkKey};

    #[test]
    fn test_encode_decode() {
        let key = SchemaTableLinkKey { schema_id: SchemaId(0x12345678), table_id: TableId(0xABCD) };
        let encoded = key.encode();

        let expected: Vec<u8> =
            vec![1, KeyKind::SchemaTableLink as u8, 0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0xAB, 0xCD];

        assert_eq!(encoded.as_slice(), expected);

        let key = SchemaTableLinkKey::decode(1, &expected[2..]).unwrap();
        assert_eq!(key.schema_id, 0x12345678);
        assert_eq!(key.table_id, 0xABCD);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = SchemaTableLinkKey { schema_id: SchemaId(1), table_id: TableId(100) };
        let key2 = SchemaTableLinkKey { schema_id: SchemaId(1), table_id: TableId(200) };
        let key3 = SchemaTableLinkKey { schema_id: SchemaId(2), table_id: TableId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded1 < encoded2, "row_id ordering not preserved");
        assert!(encoded2 < encoded3, "table_id ordering not preserved");
    }
}
