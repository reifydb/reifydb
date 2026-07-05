// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::interface::catalog::flow::FlowNodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNodeInternalStateKey {
	pub node: FlowNodeId,
	pub key: Vec<u8>,
}

impl EncodableKey for FlowNodeInternalStateKey {
	const KIND: KeyKind = KeyKind::FlowNodeInternalState;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10 + self.key.len());
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.node.0).extend_raw(&self.key);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

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
	pub const ROW_NUMBER_COUNTER_TAG: u8 = b'C';

	pub const ROW_NUMBER_MAPPING_TAG: u8 = b'M';

	pub const WINDOW_META_TAG: u8 = b'W';

	pub const WINDOW_EXPIRY_TAG: u8 = b'X';

	pub const WINDOW_RUNNING_TAG: u8 = b'R';

	pub const WINDOW_COORD_TAG: u8 = b'S';

	pub const WINDOW_ROW_STATE_TAG: u8 = b'A';

	pub const GATE_VISIBILITY_TAG: u8 = b'G';

	pub fn is_row_number_counter(&self) -> bool {
		self.key.as_slice() == [Self::ROW_NUMBER_COUNTER_TAG]
	}

	pub fn is_row_number_mapping(&self) -> bool {
		self.key.first() == Some(&Self::ROW_NUMBER_MAPPING_TAG)
	}

	pub fn is_window_meta(&self) -> bool {
		self.key.first() == Some(&Self::WINDOW_META_TAG)
	}

	pub fn is_window_expiry(&self) -> bool {
		self.key.first() == Some(&Self::WINDOW_EXPIRY_TAG)
	}

	pub fn is_gate_visibility(&self) -> bool {
		self.key.first() == Some(&Self::GATE_VISIBILITY_TAG)
	}

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
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.node.0);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.node.0.wrapping_sub(1));
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
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
	use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};

	use super::{EncodableKey, EncodableKeyRange, FlowNodeInternalStateKey, FlowNodeInternalStateKeyRange};
	use crate::interface::catalog::flow::FlowNodeId;

	#[test]
	fn test_encode_decode() {
		let key = FlowNodeInternalStateKey {
			node: FlowNodeId(0xDEADBEEF),
			key: vec![1, 2, 3, 4],
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xE0);

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
		encoded.push(0xFF);
		encoded.push(0xE5);
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeInternalStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = Vec::new();
		encoded.push(0xFE);
		encoded.push(0xFF);
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeInternalStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_too_short() {
		let mut encoded = Vec::new();
		encoded.push(0xFE);
		encoded.push(0xE5);
		encoded.extend(&999u32.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeInternalStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_flow_node_internal_state_key_range() {
		let node = FlowNodeId(42);
		let range = FlowNodeInternalStateKeyRange::new(node);

		let start = range.start().unwrap();
		let decoded_start = FlowNodeInternalStateKey::decode(&start).unwrap();
		assert_eq!(decoded_start.node, node);
		assert_eq!(decoded_start.key, Vec::<u8>::new());

		let end = range.end().unwrap();
		let decoded_end = FlowNodeInternalStateKey::decode(&end).unwrap();
		assert_eq!(decoded_end.node.0, 41);
		assert_eq!(decoded_end.key, Vec::<u8>::new());
	}

	#[test]
	fn test_flow_node_internal_state_key_range_decode() {
		let node = FlowNodeId(100);
		let range = FlowNodeInternalStateKeyRange::new(node);

		let encoded_range = EncodedKeyRange::start_end(range.start(), range.end());

		let (start_decoded, end_decoded) = FlowNodeInternalStateKeyRange::decode(&encoded_range);

		assert!(start_decoded.is_some());
		assert_eq!(start_decoded.unwrap().node, node);

		assert!(end_decoded.is_some());
		assert_eq!(end_decoded.unwrap().node.0, 99);
	}
}
