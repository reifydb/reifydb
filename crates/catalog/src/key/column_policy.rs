// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::column::ColumnId;
use crate::column_policy::ColumnPolicyId;
use crate::key::{EncodableKey, KeyKind};
use reifydb_core::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct ColumnPolicyKey {
    pub column: ColumnId,
    pub policy: ColumnPolicyId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnPolicyKey {
    const KIND: KeyKind = KeyKind::ColumnPolicy;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(18);
        out.push(VERSION);
        out.push(Self::KIND as u8);
        out.extend(&self.column.to_be_bytes());
        out.extend(&self.policy.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 16);
        Some(Self {
            column: ColumnId(u64::from_be_bytes(payload[..8].try_into().unwrap())),
            policy: ColumnPolicyId(u64::from_be_bytes(payload[8..].try_into().unwrap())),
        })
    }
}

impl ColumnPolicyKey {
    pub fn full_scan(column: ColumnId) -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::link_start(column)), Some(Self::link_end(column)))
    }

    fn link_start(column: ColumnId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.push(VERSION);
        out.push(KeyKind::ColumnPolicy as u8);
        out.extend(&column.to_be_bytes());
        EncodedKey::new(out)
    }

    fn link_end(column: ColumnId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.push(VERSION);
        out.push(KeyKind::ColumnPolicy as u8);
        out.extend(&(*column + 1).to_be_bytes());
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::column::ColumnId;
    use crate::column_policy::ColumnPolicyId;
    use crate::key::{ColumnPolicyKey, EncodableKey, KeyKind};

    #[test]
    fn test_encode_decode() {
        let key = ColumnPolicyKey { column: ColumnId(0xABCD), policy: ColumnPolicyId(0x12345678) };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            1,
            KeyKind::ColumnPolicy as u8,
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

        let key = ColumnPolicyKey::decode(1, &expected[2..]).unwrap();
        assert_eq!(key.column, 0xABCD);
        assert_eq!(key.policy, 0x12345678);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = ColumnPolicyKey { column: ColumnId(1), policy: ColumnPolicyId(100) };
        let key2 = ColumnPolicyKey { column: ColumnId(1), policy: ColumnPolicyId(200) };
        let key3 = ColumnPolicyKey { column: ColumnId(2), policy: ColumnPolicyId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded1 < encoded2, "row_id ordering not preserved");
        assert!(encoded2 < encoded3, "table_id ordering not preserved");
    }
}
