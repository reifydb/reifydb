// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::HandlerId,
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct HandlerKey {
	pub handler: HandlerId,
}

impl HandlerKey {
	pub fn new(handler: HandlerId) -> Self {
		Self {
			handler,
		}
	}

	pub fn encoded(handler: impl Into<HandlerId>) -> EncodedKey {
		Self::new(handler.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for HandlerKey {
	const KIND: KeyKind = KeyKind::Handler;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.handler);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let handler = de.read_u64().ok()?;

		Some(Self {
			handler: HandlerId(handler),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, HandlerKey};
	use crate::interface::catalog::id::HandlerId;

	#[test]
	fn test_encode_decode() {
		let key = HandlerKey {
			handler: HandlerId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![
			0xD1, 
			0x3F, 0x54, 0x32,
		];
		assert_eq!(encoded.as_slice(), expected);

		let decoded = HandlerKey::decode(&encoded).unwrap();
		assert_eq!(decoded.handler, HandlerId(0xABCD));
	}
}
