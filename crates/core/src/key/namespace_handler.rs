// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::{HandlerId, NamespaceId},
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceHandlerKey {
	pub namespace: NamespaceId,
	pub handler: HandlerId,
}

impl NamespaceHandlerKey {
	pub fn new(namespace: NamespaceId, handler: HandlerId) -> Self {
		Self {
			namespace,
			handler,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, handler: impl Into<HandlerId>) -> EncodedKey {
		Self::new(namespace.into(), handler.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for NamespaceHandlerKey {
	const KIND: KeyKind = KeyKind::NamespaceHandler;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.handler);
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

		let namespace = de.read_u64().ok()?;
		let handler = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			handler: HandlerId(handler),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, NamespaceHandlerKey};
	use crate::interface::catalog::id::{HandlerId, NamespaceId};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceHandlerKey {
			namespace: NamespaceId(0xABCD),
			handler: HandlerId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let expected: Vec<u8> = vec![
			0xFE, // version
			0xD0, // kind (NamespaceHandler = 0x2F, encoded as 0xFF ^ 0x2F)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];
		assert_eq!(encoded.as_slice(), expected);

		let decoded = NamespaceHandlerKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.handler, HandlerId(0x123456789ABCDEF0));
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceHandlerKey {
			namespace: NamespaceId(1),
			handler: HandlerId(100),
		};
		let key2 = NamespaceHandlerKey {
			namespace: NamespaceId(1),
			handler: HandlerId(200),
		};
		let key3 = NamespaceHandlerKey {
			namespace: NamespaceId(2),
			handler: HandlerId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
