// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::TableId;
use crate::util::encoding::keycode;
use crate::{EncodedKey, EncodedKeyRange};

#[derive(Debug,Clone, PartialEq)]
pub struct TableKey {
    pub table: TableId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableKey {
    const KIND: KeyKind = KeyKind::Table;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(10);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.table));
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self> {
        assert_eq!(version, VERSION);
        if payload.len() != 8 {
            return None;
        }
        keycode::deserialize(&payload[..8]).ok().map(|table| Self { table })
    }
}

impl TableKey {
    pub fn full_scan() -> EncodedKeyRange {
        EncodedKeyRange::start_end(Some(Self::table_start()), Some(Self::table_end()))
    }

    fn table_start() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        EncodedKey::new(out)
    }

    fn table_end() -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&(Self::KIND as u8 - 1)));
        EncodedKey::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::{EncodableKey, TableKey};
    use crate::interface::catalog::TableId;

    #[test]
    fn test_encode_decode() {
        let key = TableKey { table: TableId(0xABCD) };
        let encoded = key.encode();
        let expected = vec![
            0xFE, // version
            0xFD, // kind
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32,
        ];
        assert_eq!(encoded.as_slice(), expected);

        let key = TableKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.table, 0xABCD);
    }
}
