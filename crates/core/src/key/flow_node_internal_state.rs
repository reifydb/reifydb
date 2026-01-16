// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::flow::FlowNodeId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNodeInternalStateKey {
	pub node: FlowNodeId,
	pub key: Vec<u8>,
}

const VERSION: u8 = 1;

impl EncodableKey for FlowNodeInternalStateKey {
	const KIND: KeyKind = KeyKind::FlowNodeInternalState;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10 + self.key.len());
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.node.0).extend_raw(&self.key);
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
		let key_bytes = de.read_raw(de.remaining()).ok()?.to_vec();

		Some(Self {
			node: FlowNodeId(node_id),
			key: key_bytes,
		})
	}
}

impl FlowNodeInternalStateKey {
	pub fn new(node: FlowNodeId, key: Vec<u8>) -> Self {
		Self {
			node,
			key,
		}
	}

	pub fn new_empty(node: FlowNodeId) -> Self {
		Self {
			node,
			key: Vec::new(),
		}
	}

	pub fn encoded(node: impl Into<FlowNodeId>, key: impl Into<Vec<u8>>) -> EncodedKey {
		Self::new(node.into(), key.into()).encode()
	}

	/// Create a range for scanning all entries of a specific operator
	pub fn node_range(node: FlowNodeId) -> EncodedKeyRange {
		let range = FlowNodeInternalStateKeyRange::new(node);
		EncodedKeyRange::start_end(range.start(), range.end())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNodeInternalStateKeyRange {
	pub node: FlowNodeId,
}

impl FlowNodeInternalStateKeyRange {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}

	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != FlowNodeInternalStateKey::KIND {
			return None;
		}

		let node_id = de.read_u64().ok()?;

		Some(Self {
			node: FlowNodeId(node_id),
		})
	}
}

impl EncodableKeyRange for FlowNodeInternalStateKeyRange {
	const KIND: KeyKind = KeyKind::FlowNodeInternalState;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.node.0);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.node.0.wrapping_sub(1));
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		use std::ops::Bound;

		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, EncodableKeyRange, FlowNodeInternalStateKey, FlowNodeInternalStateKeyRange};
	use crate::{
		encoded::key::{EncodedKey, EncodedKeyRange},
		interface::catalog::flow::FlowNodeId,
	};

	#[test]
	fn test_encode_decode() {
		let key = FlowNodeInternalStateKey {
			node: FlowNodeId(0xDEADBEEF),
			key: vec![1, 2, 3, 4],
		};
		let encoded = key.encode();

		// Verify the encoded format
		assert_eq!(encoded[0], 0xFE); // version
		assert_eq!(encoded[1], 0xE0); // kind (0x1F encoded)

		let decoded = FlowNodeInternalStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node.0, 0xDEADBEEF);
		assert_eq!(decoded.key, vec![1, 2, 3, 4]);
	}

	#[test]
	fn test_encode_decode_empty_key() {
		let key = FlowNodeInternalStateKey {
			node: FlowNodeId(0xDEADBEEF),
			key: vec![],
		};
		let encoded = key.encode();

		let decoded = FlowNodeInternalStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node.0, 0xDEADBEEF);
		assert_eq!(decoded.key, Vec::<u8>::new());
	}

	#[test]
	fn test_new() {
		let key = FlowNodeInternalStateKey::new(FlowNodeId(42), vec![5, 6, 7]);
		assert_eq!(key.node.0, 42);
		assert_eq!(key.key, vec![5, 6, 7]);
	}

	#[test]
	fn test_new_empty() {
		let key = FlowNodeInternalStateKey::new_empty(FlowNodeId(42));
		assert_eq!(key.node.0, 42);
		assert_eq!(key.key, Vec::<u8>::new());
	}

	#[test]
	fn test_roundtrip() {
		let original = FlowNodeInternalStateKey {
			node: FlowNodeId(999_999_999),
			key: vec![10, 20, 30, 40, 50],
		};
		let encoded = original.encode();
		let decoded = FlowNodeInternalStateKey::decode(&encoded).unwrap();
		assert_eq!(original, decoded);
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = Vec::new();
		encoded.push(0xFF); // wrong version
		encoded.push(0xE5); // correct kind
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeInternalStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = Vec::new();
		encoded.push(0xFE); // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeInternalStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_too_short() {
		let mut encoded = Vec::new();
		encoded.push(0xFE); // correct version
		encoded.push(0xE5); // correct kind
		encoded.extend(&999u32.to_be_bytes()); // only 4 bytes instead of 8 for operator id
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeInternalStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_flow_node_internal_state_key_range() {
		let node = FlowNodeId(42);
		let range = FlowNodeInternalStateKeyRange::new(node);

		// Test start key
		let start = range.start().unwrap();
		let decoded_start = FlowNodeInternalStateKey::decode(&start).unwrap();
		assert_eq!(decoded_start.node, node);
		assert_eq!(decoded_start.key, Vec::<u8>::new());

		// Test end key
		let end = range.end().unwrap();
		let decoded_end = FlowNodeInternalStateKey::decode(&end).unwrap();
		assert_eq!(decoded_end.node.0, 41); // Should be operator - 1
		assert_eq!(decoded_end.key, Vec::<u8>::new());
	}

	#[test]
	fn test_flow_node_internal_state_key_range_decode() {
		let node = FlowNodeId(100);
		let range = FlowNodeInternalStateKeyRange::new(node);

		// Create an EncodedKeyRange
		let encoded_range = EncodedKeyRange::start_end(range.start(), range.end());

		// Decode it back
		let (start_decoded, end_decoded) = FlowNodeInternalStateKeyRange::decode(&encoded_range);

		assert!(start_decoded.is_some());
		assert_eq!(start_decoded.unwrap().node, node);

		assert!(end_decoded.is_some());
		assert_eq!(end_decoded.unwrap().node.0, 99);
	}
}
