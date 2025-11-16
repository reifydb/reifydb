// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{FlowId, NamespaceId},
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceFlowKey {
	pub namespace: NamespaceId,
	pub flow: FlowId,
}

const VERSION: u8 = 1;

impl EncodableKey for NamespaceFlowKey {
	const KIND: KeyKind = KeyKind::NamespaceFlow;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.flow);
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
		let flow = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			flow: FlowId(flow),
		})
	}
}

impl NamespaceFlowKey {
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
mod tests {
	use super::{EncodableKey, NamespaceFlowKey};
	use crate::interface::catalog::{FlowId, NamespaceId};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceFlowKey {
			namespace: NamespaceId(0xABCD),
			flow: FlowId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let decoded = NamespaceFlowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.flow, FlowId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceFlowKey {
			namespace: NamespaceId(1),
			flow: FlowId(100),
		};
		let key2 = NamespaceFlowKey {
			namespace: NamespaceId(1),
			flow: FlowId(200),
		};
		let key3 = NamespaceFlowKey {
			namespace: NamespaceId(2),
			flow: FlowId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
