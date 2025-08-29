// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey,
	interface::{
		ColumnId, StoreId,
		key::{EncodableKey, KeyKind},
	},
	util::encoding::keycode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnSequenceKey {
	pub store: StoreId,
	pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnSequenceKey {
	const KIND: KeyKind = KeyKind::ColumnSequence;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(19); // 1 + 1 + 9 + 8
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&self.store));
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
		if payload.len() != 17 {
			// 9 bytes for store + 8 bytes for column
			return None;
		}

		let store =
			keycode::deserialize_store_id(&payload[..9]).ok()?;
		let column = keycode::deserialize(&payload[9..17]).ok()?;
		Some(Self {
			store,
			column,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{ColumnSequenceKey, EncodableKey};
	use crate::{
		EncodedKey,
		interface::{ColumnId, StoreId},
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnSequenceKey {
			store: StoreId::table(0x1234),
			column: ColumnId(0x5678),
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xFE); // version serialized
		assert_eq!(encoded[1], 0xF1); // KeyKind::StoreColumnSequence serialized

		// Test decode
		let decoded = ColumnSequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.store, StoreId::table(0x1234));
		assert_eq!(decoded.column, ColumnId(0x5678));
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = vec![0xFF]; // wrong version
		encoded.push(0x0E); // correct kind
		encoded.extend(&[0; 16]); // payload

		let decoded =
			ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = vec![0x01]; // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&[0; 16]); // payload

		let decoded =
			ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let encoded = vec![0x01, 0x0E]; // version and kind only, missing payload
		let decoded =
			ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}
}
