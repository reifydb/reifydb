// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
	value::encoded::key::EncodedKey,
};

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionVersionKey {}

impl TransactionVersionKey {
	pub fn encoded() -> EncodedKey {
		Self {}.encode()
	}
}

const VERSION: u8 = 1;

impl EncodableKey for TransactionVersionKey {
	const KIND: KeyKind = KeyKind::TransactionVersion;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		Some(TransactionVersionKey {})
	}
}

#[cfg(test)]
pub mod tests {
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
