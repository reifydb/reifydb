// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{EncodedKey, Version};
use crate::util::encoding::keycode;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CdcEventKey {
    pub version: Version,
    pub sequence: u16,
}

const VERSION_BYTE: u8 = 1;

impl EncodableKey for CdcEventKey {
    const KIND: KeyKind = KeyKind::CdcEvent;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(11);
        out.extend(&keycode::serialize(&VERSION_BYTE));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.version));
        out.extend(&keycode::serialize(&self.sequence));
        EncodedKey::new(out)
    }

    fn decode(key: &EncodedKey) -> Option<Self>
    where
        Self: Sized,
    {
        if key.len() < 11 {
            return None;
        }

        let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
        if version != VERSION_BYTE {
            return None;
        }

        let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
        if kind != Self::KIND {
            return None;
        }

        let version: Version = keycode::deserialize(&key[2..10]).ok()?;
        let sequence: u16 = keycode::deserialize(&key[10..12]).ok()?;
        
        Some(Self { version, sequence })
    }
}

#[cfg(test)]
mod tests {
    use super::{CdcEventKey, EncodableKey};

    #[test]
    fn test_encode_decode_cdc_event() {
        let key = CdcEventKey {
            version: 12345678901234567890,
            sequence: 42,
        };
        
        let encoded = key.encode();
        let decoded = CdcEventKey::decode(&encoded).expect("Failed to decode key");
        
        assert_eq!(decoded.version, 12345678901234567890);
        assert_eq!(decoded.sequence, 42);
    }
}