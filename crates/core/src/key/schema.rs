// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::catalog::SchemaId;
use crate::{EncodableKey, EncodedKey, EncodedKeyRange, KeyKind};

#[derive(Debug)]
pub struct SchemaKey {
    pub schema: SchemaId,
}

const VERSION: u8 = 1;

impl EncodableKey for SchemaKey {
    const KIND: KeyKind = KeyKind::Schema;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(6);
        out.push(VERSION);
        out.push(Self::KIND as u8);
        out.extend(&self.schema.to_be_bytes());
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 4);
        Some(Self { schema: SchemaId(u32::from_be_bytes(payload[..].try_into().unwrap())) })
    }
}

impl SchemaKey {
    pub fn full_scan() -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::schema_start()), Some(Self::schema_end()))
    }

    fn schema_start() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.push(VERSION);
        out.push(KeyKind::Schema as u8);
        EncodedKey::new(out)
    }

    fn schema_end() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.push(VERSION);
        out.push(KeyKind::Schema as u8 + 1);
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::SchemaId;
    use crate::key::schema::SchemaKey;
    use crate::{EncodableKey, KeyKind};

    #[test]
    fn test_encode_decode() {
        let key = SchemaKey { schema: SchemaId(0xABCD) };
        let encoded = key.encode();
        let expected = vec![1, KeyKind::Schema as u8, 0x00, 0x00, 0xAB, 0xCD];
        assert_eq!(encoded.as_slice(), expected);

        let key = SchemaKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.schema, 0xABCD);
    }
}
