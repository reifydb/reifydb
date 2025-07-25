// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::SystemSequenceId;
use crate::util::encoding::keycode;
use crate::{EncodedKey, EncodedKeyRange};

#[derive(Debug,Clone, PartialEq)]
pub struct SystemSequenceKey {
    pub sequence: SystemSequenceId,
}

const VERSION: u8 = 1;

impl EncodableKey for SystemSequenceKey {
    const KIND: KeyKind = KeyKind::SystemSequence;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.sequence));
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        if payload.len() != 4 {
            return None;
        }
        keycode::deserialize(&payload).ok().map(|sequence| Self { sequence })
    }
}

impl SystemSequenceKey {
    pub fn full_scan() -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::sequence_start()), Some(Self::sequence_end()))
    }

    fn sequence_start() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        EncodedKey::new(out)
    }

    fn sequence_end() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&(Self::KIND as u8 - 1)));
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::{EncodableKey, SystemSequenceKey};
    use crate::interface::catalog::SystemSequenceId;

    #[test]
    fn test_encode_decode() {
        let key = SystemSequenceKey { sequence: SystemSequenceId(0xABCD) };
        let encoded = key.encode();
        let expected = vec![
            0xFE, // version
            0xFA, // kind
            0xFF, 0xFF, 0x54, 0x32,
        ];
        assert_eq!(encoded.as_slice(), expected);

        let key = SystemSequenceKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.sequence, 0xABCD);
    }
}
