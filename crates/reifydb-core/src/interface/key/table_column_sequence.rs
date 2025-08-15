// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey,
	interface::{
		TableColumnId, TableId,
		key::{EncodableKey, KeyKind},
	},
	util::encoding::keycode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableColumnSequenceKey {
	pub table: TableId,
	pub column: TableColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableColumnSequenceKey {
	const KIND: KeyKind = KeyKind::TableColumnSequence;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(18);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.table));
		out.extend(&keycode::serialize(&self.column));
		EncodedKey::new(out)
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}

		let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}

		let payload = &key[2..];
		if payload.len() != 16 {
			return None;
		}

		let table = keycode::deserialize(&payload[..8]).ok()?;
		let column = keycode::deserialize(&payload[8..16]).ok()?;
		Some(Self {
			table,
			column,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, TableColumnSequenceKey};
	use crate::{
		EncodedKey,
		interface::{TableColumnId, TableId},
	};

	#[test]
	fn test_encode_decode() {
		let key = TableColumnSequenceKey {
			table: TableId(0x1234),
			column: TableColumnId(0x5678),
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xFE); // version serialized
		assert_eq!(encoded[1], 0xF1); // KeyKind::TableColumnSequence serialized

		// Test decode
		let decoded = TableColumnSequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.table, TableId(0x1234));
		assert_eq!(decoded.column, TableColumnId(0x5678));
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = vec![0xFF]; // wrong version
		encoded.push(0x0E); // correct kind
		encoded.extend(&[0; 16]); // payload

		let decoded = TableColumnSequenceKey::decode(&EncodedKey::new(
			encoded,
		));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = vec![0x01]; // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&[0; 16]); // payload

		let decoded = TableColumnSequenceKey::decode(&EncodedKey::new(
			encoded,
		));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let encoded = vec![0x01, 0x0E]; // version and kind only, missing payload
		let decoded = TableColumnSequenceKey::decode(&EncodedKey::new(
			encoded,
		));
		assert!(decoded.is_none());
	}
}
