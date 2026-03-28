// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::{NamespaceId, SourceId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceSourceKey {
	pub namespace: NamespaceId,
	pub source: SourceId,
}

const VERSION: u8 = 1;

impl EncodableKey for NamespaceSourceKey {
	const KIND: KeyKind = KeyKind::NamespaceSource;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.source);
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
		let source = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			source: SourceId(source),
		})
	}
}

impl NamespaceSourceKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, source: impl Into<SourceId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			source: source.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, NamespaceSourceKey};
	use crate::interface::catalog::id::{NamespaceId, SourceId};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceSourceKey {
			namespace: NamespaceId(0xABCD),
			source: SourceId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let decoded = NamespaceSourceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.source, SourceId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceSourceKey {
			namespace: NamespaceId::SYSTEM,
			source: SourceId(100),
		};
		let key2 = NamespaceSourceKey {
			namespace: NamespaceId::SYSTEM,
			source: SourceId(200),
		};
		let key3 = NamespaceSourceKey {
			namespace: NamespaceId::DEFAULT,
			source: SourceId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
