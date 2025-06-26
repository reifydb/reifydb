// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::key::{EncodableKey, KeyKind};
use crate::table::TableId;
use reifydb_core::encoding::keycode;
use reifydb_core::{EncodedKey, EncodedKeyRange};

#[derive(Debug)]
pub struct TableRowSequenceKey {
    pub table: TableId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableRowSequenceKey {
    const KIND: KeyKind = KeyKind::TableRowSequence;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.table));
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 8);
        keycode::deserialize(&payload[..8]).ok().map(|table| Self { table })
    }
}

impl TableRowSequenceKey {
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
    use crate::key::{EncodableKey, TableRowSequenceKey};
    use crate::table::TableId;

    #[test]
    fn test_encode_decode() {
        let key = TableRowSequenceKey { table: TableId(0xABCD) };
        let encoded = key.encode();
        let expected = vec![
            0xFE, // version
            0xF7, // kind
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32,
        ];
        assert_eq!(encoded.as_slice(), expected);

        let key = TableRowSequenceKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.table, 0xABCD);
    }
}
