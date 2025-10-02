// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey,
	interface::{ColumnId, SourceId},
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{self, KeySerializer},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnSequenceKey {
	pub source: SourceId,
	pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnSequenceKey {
	const KIND: KeyKind = KeyKind::ColumnSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19); // 1 + 1 + 9 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_source_id(self.source)
			.extend_u64(self.column);
		serializer.to_encoded_key()
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
			// 9 bytes for source + 8 bytes for column
			return None;
		}

		let source = keycode::deserialize_source_id(&payload[..9]).ok()?;
		let column = keycode::deserialize(&payload[9..17]).ok()?;
		Some(Self {
			source,
			column,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{ColumnSequenceKey, EncodableKey};
	use crate::{
		EncodedKey,
		interface::{ColumnId, SourceId},
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnSequenceKey {
			source: SourceId::table(0x1234),
			column: ColumnId(0x5678),
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xFE); // version serialized
		assert_eq!(encoded[1], 0xF1); // KeyKind::StoreColumnSequence serialized

		// Test decode
		let decoded = ColumnSequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.source, SourceId::table(0x1234));
		assert_eq!(decoded.column, ColumnId(0x5678));
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = vec![0xFF]; // wrong version
		encoded.push(0x0E); // correct kind
		encoded.extend(&[0; 16]); // payload

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = vec![0x01]; // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&[0; 16]); // payload

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let encoded = vec![0x01, 0x0E]; // version and kind only, missing payload
		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}
}
