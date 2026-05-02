// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::flow::FlowNodeId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

/// Key for storing TTL configuration for a flow operator.
///
/// Sibling to [`crate::key::row_ttl::RowTtlKey`], which stores TTL configuration
/// for data shapes (tables, ringbuffers, series). Operator TTL is keyed by
/// `FlowNodeId` and read by the dedicated operator-state TTL eviction actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorTtlKey {
	pub node: FlowNodeId,
}

impl OperatorTtlKey {
	pub fn encoded(node: impl Into<FlowNodeId>) -> EncodedKey {
		Self {
			node: node.into(),
		}
		.encode()
	}
}

impl EncodableKey for OperatorTtlKey {
	const KIND: KeyKind = KeyKind::OperatorTtl;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.node.0);
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

		let node_id = de.read_u64().ok()?;

		Some(Self {
			node: FlowNodeId(node_id),
		})
	}
}

/// Range for scanning all operator TTL configurations.
pub struct OperatorTtlKeyRange;

impl OperatorTtlKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(OperatorTtlKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(OperatorTtlKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_operator_ttl_key_encoding() {
		let key = OperatorTtlKey {
			node: FlowNodeId(42),
		};

		let encoded = key.encode();
		let decoded = OperatorTtlKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_ttl_key_roundtrip_large_id() {
		let key = OperatorTtlKey {
			node: FlowNodeId(0xDEAD_BEEF_CAFE_BABE),
		};

		let encoded = key.encode();
		let decoded = OperatorTtlKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_ttl_key_decode_invalid_version() {
		let mut bytes = Vec::new();
		bytes.push(0x00); // wrong version
		bytes.push(OperatorTtlKey::KIND as u8);
		bytes.extend(&42u64.to_be_bytes());
		let key = EncodedKey::new(bytes);
		assert!(OperatorTtlKey::decode(&key).is_none());
	}

	#[test]
	fn test_operator_ttl_key_decode_wrong_kind() {
		let mut bytes = Vec::new();
		bytes.push(VERSION);
		bytes.push(0xFF); // wrong kind
		bytes.extend(&42u64.to_be_bytes());
		let key = EncodedKey::new(bytes);
		assert!(OperatorTtlKey::decode(&key).is_none());
	}
}
