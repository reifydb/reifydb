// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::{EncodedKey, EncodedKeyRange};
use crate::column::ColumnId;
use crate::key::{EncodableKey, KeyKind};

#[derive(Debug)]
pub struct ColumnKey {
	pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnKey {
	const KIND: KeyKind = KeyKind::Column;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(6);
		out.push(VERSION);
		out.push(Self::KIND as u8);
		out.extend(&self.column.to_be_bytes());
		EncodedKey::new(out)
	}

	fn decode(version: u8, payload: &[u8]) -> Option<Self> {
		assert_eq!(version, VERSION);
		assert_eq!(payload.len(), 4);
		Some(Self { column: ColumnId(u32::from_be_bytes(payload[..].try_into().unwrap())) })
	}
}

impl ColumnKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::column_start()), Some(Self::column_end()))
	}

	fn column_start() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.push(VERSION);
		out.push(KeyKind::Column as u8);
		EncodedKey::new(out)
	}

	fn column_end() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.push(VERSION);
		out.push(KeyKind::Column as u8 + 1);
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use crate::column::ColumnId;
	use crate::key::{ColumnKey, EncodableKey, KeyKind};

	#[test]
	fn test_encode_decode() {
		let key = ColumnKey { column: ColumnId(0xABCD) };
		let encoded = key.encode();
		let expected = vec![1, KeyKind::Column as u8, 0x00, 0x00, 0xAB, 0xCD];
		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnKey::decode(1, &encoded[2..]).unwrap();
		assert_eq!(key.column, 0xABCD);
	}
}
