// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey,
	util::encoding::keycode::{self, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionVersionKey {}

const VERSION: u8 = 1;

impl EncodableKey for TransactionVersionKey {
	const KIND: KeyKind = KeyKind::TransactionVersion;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
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

		Some(TransactionVersionKey {})
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, TransactionVersionKey};

	#[test]
	fn test_encode_decode() {
		let key = TransactionVersionKey {};
		let encoded = key.encode();
		let expected = vec![
			0xFE, // version
			0xF4, // kind
		];
		assert_eq!(encoded.as_slice(), expected);

		TransactionVersionKey::decode(&encoded).unwrap();
	}
}
