// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::catalog::SequenceId;
use crate::{EncodableKey, EncodedKey, EncodedKeyRange, KeyKind};

#[derive(Debug)]
pub struct SequenceKey {
    pub sequence: SequenceId,
}

const VERSION: u8 = 1;

impl EncodableKey for SequenceKey {
    const KIND: KeyKind = KeyKind::Sequence;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(Self::KIND as u8);
        out.extend(&self.sequence.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 4);
        Some(Self { sequence: SequenceId(u32::from_be_bytes(payload[..].try_into().unwrap())) })
    }
}

impl SequenceKey {
    pub fn full_scan() -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::sequence_start()), Some(Self::sequence_end()))
    }

    fn sequence_start() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.push(VERSION);
        out.push(KeyKind::Sequence as u8);
        EncodedKey::new(out)
    }

    fn sequence_end() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.push(VERSION);
        out.push(KeyKind::Sequence as u8 + 1);
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::SequenceId;
    use crate::key::sequence::SequenceKey;
    use crate::{EncodableKey, KeyKind};

    #[test]
    fn test_encode_decode() {
        let key = SequenceKey { sequence: SequenceId(0xABCD) };
        let encoded = key.encode();
        let expected = vec![1, KeyKind::Sequence as u8, 0x00, 0x00, 0xAB, 0xCD];
        assert_eq!(encoded.as_slice(), expected);

        let key = SequenceKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.sequence, 0xABCD);
    }
}
