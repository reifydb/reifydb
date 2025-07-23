// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::{ColumnId, ColumnPolicyId};
use crate::util::encoding::keycode;
use crate::{EncodedKey, EncodedKeyRange};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnPolicyKey {
    pub column: ColumnId,
    pub policy: ColumnPolicyId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnPolicyKey {
    const KIND: KeyKind = KeyKind::ColumnPolicy;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(18);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.column));
        out.extend(&keycode::serialize(&self.policy));
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 16);

        keycode::deserialize(&payload[..8])
            .ok()
            .zip(keycode::deserialize(&payload[8..]).ok())
            .map(|(column, policy)| Self { column, policy })
    }
}

impl ColumnPolicyKey {
    pub fn full_scan(column: ColumnId) -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::link_start(column)), Some(Self::link_end(column)))
    }

    fn link_start(column: ColumnId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&column));
        EncodedKey::new(out)
    }

    fn link_end(column: ColumnId) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&(*column - 1)));
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnPolicyKey, EncodableKey};
    use crate::interface::catalog::ColumnId;
    use crate::interface::catalog::ColumnPolicyId;

    #[test]
    fn test_encode_decode() {
        let key = ColumnPolicyKey {
            column: ColumnId(0xABCD),
            policy: ColumnPolicyId(0x123456789ABCDEF0),
        };
        let encoded = key.encode();

        let expected: Vec<u8> = vec![
            0xFE, // version
            0xF6, // kind
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
            0x21, 0x0F,
        ];

        assert_eq!(encoded.as_slice(), expected);

        let key = ColumnPolicyKey::decode(1, &expected[2..]).unwrap();
        assert_eq!(key.column, 0xABCD);
        assert_eq!(key.policy, 0x123456789ABCDEF0);
    }

    #[test]
    fn test_order_preserving() {
        let key1 = ColumnPolicyKey { column: ColumnId(1), policy: ColumnPolicyId(100) };
        let key2 = ColumnPolicyKey { column: ColumnId(1), policy: ColumnPolicyId(200) };
        let key3 = ColumnPolicyKey { column: ColumnId(2), policy: ColumnPolicyId(0) };

        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        assert!(encoded3 < encoded2, "ordering not preserved");
        assert!(encoded2 < encoded1, "ordering not preserved");
    }
}
