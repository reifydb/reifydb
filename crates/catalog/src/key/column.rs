// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnId;
use crate::key::{EncodableKey, KeyKind};
use reifydb_core::encoding::keycode;
use reifydb_core::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct ColumnKey {
    pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnKey {
    const KIND: KeyKind = KeyKind::Column;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.column));
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 8);
        keycode::deserialize(&payload[..8]).ok().map(|column| Self { column })
    }
}

impl ColumnKey {
    pub fn full_scan() -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::column_start()), Some(Self::column_end()))
    }

    fn column_start() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        EncodedKey::new(out)
    }

    fn column_end() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&(Self::KIND as u8 - 1)));
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::column::ColumnId;
    use crate::key::{ColumnKey, EncodableKey};

    #[test]
    fn test_encode_decode() {
        let key = ColumnKey { column: ColumnId(0xABCD) };
        let encoded = key.encode();
        let expected = vec![
            0xFE, // version
            0xF9, // kind
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32,
        ];
        assert_eq!(encoded.as_slice(), expected);

        let key = ColumnKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.column, 0xABCD);
    }
}
