// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange, interface::FlowNodeId,
	util::encoding::keycode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNodeStateKey {
	pub node: FlowNodeId,
	pub key: Vec<u8>,
}

const VERSION: u8 = 1;

impl EncodableKey for FlowNodeStateKey {
	const KIND: KeyKind = KeyKind::FlowNodeState;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(10 + self.key.len());
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.node.0));
		out.extend(&self.key);
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
		if payload.len() < 8 {
			return None;
		}

		let node_id: u64 = keycode::deserialize(&payload[..8]).ok()?;
		let key_bytes = payload[8..].to_vec();

		Some(Self {
			node: FlowNodeId(node_id),
			key: key_bytes,
		})
	}
}

impl FlowNodeStateKey {
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

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::operator_state_start()),
			Some(Self::operator_state_end()),
		)
	}

	/// Create a range for scanning all entries of a specific node
	pub fn node_range(node: FlowNodeId) -> EncodedKeyRange {
		let range = FlowNodeStateKeyRange::new(node);
		EncodedKeyRange::start_end(range.start(), range.end())
	}

	fn operator_state_start() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		EncodedKey::new(out)
	}

	fn operator_state_end() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&(Self::KIND as u8 + 1)));
		EncodedKey::new(out)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNodeStateKeyRange {
	pub node: FlowNodeId,
}

impl FlowNodeStateKeyRange {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}

	fn decode_key(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}

		let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != FlowNodeStateKey::KIND {
			return None;
		}

		let payload = &key[2..];
		if payload.len() < 8 {
			return None;
		}

		let node_id: u64 = keycode::deserialize(&payload[..8]).ok()?;
		Some(Self {
			node: FlowNodeId(node_id),
		})
	}
}

impl EncodableKeyRange for FlowNodeStateKeyRange {
	const KIND: KeyKind = KeyKind::FlowNodeState;

	fn start(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.node.0));
		Some(EncodedKey::new(out))
	}

	fn end(&self) -> Option<EncodedKey> {
		let next_node = FlowNodeId(self.node.0 + 1);
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&next_node.0));
		Some(EncodedKey::new(out))
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		use std::ops::Bound;

		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => {
				Self::decode_key(key)
			}
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => {
				Self::decode_key(key)
			}
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

#[cfg(test)]
mod tests {
	use super::{
		EncodableKey, EncodableKeyRange, FlowNodeStateKey,
		FlowNodeStateKeyRange,
	};
	use crate::{EncodedKey, EncodedKeyRange};

	#[test]
	fn test_encode_decode() {
		let key = FlowNodeStateKey {
			node: crate::interface::FlowNodeId(0xDEADBEEF),
			key: vec![1, 2, 3, 4],
		};
		let encoded = key.encode();

		// Verify the encoded format
		assert_eq!(encoded[0], 0xFE); // version
		assert_eq!(encoded[1], 0xEC); // kind (0x13 encoded)

		let decoded = FlowNodeStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node.0, 0xDEADBEEF);
		assert_eq!(decoded.key, vec![1, 2, 3, 4]);
	}

	#[test]
	fn test_encode_decode_empty_key() {
		let key = FlowNodeStateKey {
			node: crate::interface::FlowNodeId(0xDEADBEEF),
			key: vec![],
		};
		let encoded = key.encode();

		let decoded = FlowNodeStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node.0, 0xDEADBEEF);
		assert_eq!(decoded.key, Vec::<u8>::new());
	}

	#[test]
	fn test_new() {
		let key = FlowNodeStateKey::new(
			crate::interface::FlowNodeId(42),
			vec![5, 6, 7],
		);
		assert_eq!(key.node.0, 42);
		assert_eq!(key.key, vec![5, 6, 7]);
	}

	#[test]
	fn test_new_empty() {
		let key = FlowNodeStateKey::new_empty(
			crate::interface::FlowNodeId(42),
		);
		assert_eq!(key.node.0, 42);
		assert_eq!(key.key, Vec::<u8>::new());
	}

	#[test]
	fn test_roundtrip() {
		let original = FlowNodeStateKey {
			node: crate::interface::FlowNodeId(999_999_999),
			key: vec![10, 20, 30, 40, 50],
		};
		let encoded = original.encode();
		let decoded = FlowNodeStateKey::decode(&encoded).unwrap();
		assert_eq!(original, decoded);
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = Vec::new();
		encoded.push(0xFF); // wrong version
		encoded.push(0xEC); // correct kind
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = Vec::new();
		encoded.push(0xFE); // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_too_short() {
		let mut encoded = Vec::new();
		encoded.push(0xFE); // correct version
		encoded.push(0xEC); // correct kind
		encoded.extend(&999u32.to_be_bytes()); // only 4 bytes instead of 8 for node id
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_flow_node_state_key_range() {
		let node = crate::interface::FlowNodeId(42);
		let range = FlowNodeStateKeyRange::new(node);

		// Test start key
		let start = range.start().unwrap();
		let decoded_start = FlowNodeStateKey::decode(&start).unwrap();
		assert_eq!(decoded_start.node, node);
		assert_eq!(decoded_start.key, Vec::<u8>::new());

		// Test end key
		let end = range.end().unwrap();
		let decoded_end = FlowNodeStateKey::decode(&end).unwrap();
		assert_eq!(decoded_end.node.0, 43); // Should be node + 1
		assert_eq!(decoded_end.key, Vec::<u8>::new());
	}

	#[test]
	fn test_flow_node_state_key_range_decode() {
		let node = crate::interface::FlowNodeId(100);
		let range = FlowNodeStateKeyRange::new(node);

		// Create an EncodedKeyRange
		let encoded_range =
			EncodedKeyRange::start_end(range.start(), range.end());

		// Decode it back
		let (start_decoded, end_decoded) =
			FlowNodeStateKeyRange::decode(&encoded_range);

		assert!(start_decoded.is_some());
		assert_eq!(start_decoded.unwrap().node, node);

		assert!(end_decoded.is_some());
		assert_eq!(end_decoded.unwrap().node.0, 101);
	}

	#[test]
	fn test_node_range_method() {
		let node = crate::interface::FlowNodeId(555);
		let range = FlowNodeStateKey::node_range(node);

		// The range should include all keys for this node
		// Start should be the node with empty key
		// End should be the next node with empty key
		let (start_range, end_range) =
			FlowNodeStateKeyRange::decode(&range);

		assert!(start_range.is_some());
		assert_eq!(start_range.unwrap().node, node);

		assert!(end_range.is_some());
		assert_eq!(end_range.unwrap().node.0, 556);
	}
}
