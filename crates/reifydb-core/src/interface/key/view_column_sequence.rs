// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey,
	interface::{
		ViewColumnId, ViewId,
		key::{EncodableKey, KeyKind},
	},
	util::encoding::keycode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewColumnSequenceKey {
	pub view: ViewId,
	pub column: ViewColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ViewColumnSequenceKey {
	const KIND: KeyKind = KeyKind::ViewColumnSequence;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(18);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.view));
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

		let view = keycode::deserialize(&payload[..8]).ok()?;
		let column = keycode::deserialize(&payload[8..16]).ok()?;
		Some(Self {
			view,
			column,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, ViewColumnSequenceKey};
	use crate::{
		EncodedKey,
		interface::{ViewColumnId, ViewId},
	};

	#[test]
	fn test_encode_decode() {
		let key = ViewColumnSequenceKey {
			view: ViewId(0x1234),
			column: ViewColumnId(0x5678),
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xFE); // version serialized
		assert_eq!(encoded[1], 0xEA); // KeyKind::ViewColumnSequence serialized

		// Test decode
		let decoded = ViewColumnSequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.view, ViewId(0x1234));
		assert_eq!(decoded.column, ViewColumnId(0x5678));
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = vec![0xFF]; // wrong version
		encoded.push(0x0E); // correct kind
		encoded.extend(&[0; 16]); // payload

		let decoded = ViewColumnSequenceKey::decode(&EncodedKey::new(
			encoded,
		));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = vec![0x01]; // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&[0; 16]); // payload

		let decoded = ViewColumnSequenceKey::decode(&EncodedKey::new(
			encoded,
		));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let encoded = vec![0x01, 0x0E]; // version and kind only, missing payload
		let decoded = ViewColumnSequenceKey::decode(&EncodedKey::new(
			encoded,
		));
		assert!(decoded.is_none());
	}
}
