// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::interface::catalog::flow::OperatorId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateKey {
	pub node: OperatorId,
	pub key: Vec<u8>,
}

impl EncodableKey for OperatorStateKey {
	const KIND: KeyKind = KeyKind::OperatorState;

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
			node: OperatorId(node_id),
			key: key_bytes,
		})
	}
}

impl OperatorStateKey {
	pub fn new(node: OperatorId, key: Vec<u8>) -> Self {
		Self {
			node,
			key,
		}
	}

	pub fn new_empty(node: OperatorId) -> Self {
		Self {
			node,
			key: Vec::new(),
		}
	}

	pub fn encoded(node: impl Into<OperatorId>, key: impl AsRef<[u8]>) -> EncodedKey {
		let key = key.as_ref();
		let mut serializer = KeySerializer::with_capacity(10 + key.len());
		serializer.extend_u8(Self::KIND as u8).extend_u64(node.into().0).extend_raw(key);
		serializer.to_encoded_key()
	}

	pub fn node_range(node: OperatorId) -> EncodedKeyRange {
		let range = OperatorStateKeyRange::new(node);
		EncodedKeyRange::start_end(range.start(), range.end())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateKeyRange {
	pub node: OperatorId,
}

impl OperatorStateKeyRange {
	pub fn new(node: OperatorId) -> Self {
		Self {
			node,
		}
	}

	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != OperatorStateKey::KIND {
			return None;
		}

		let node_id = de.read_u64().ok()?;

		Some(Self {
			node: OperatorId(node_id),
		})
	}
}

impl EncodableKeyRange for OperatorStateKeyRange {
	const KIND: KeyKind = KeyKind::OperatorState;

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

	use super::{EncodableKey, EncodableKeyRange, OperatorStateKey, OperatorStateKeyRange};
	use crate::interface::catalog::flow::OperatorId;

	#[test]
	fn test_encode_decode() {
		let key = OperatorStateKey {
			node: OperatorId(0xDEADBEEF),
			key: vec![1, 2, 3, 4],
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xEC);

		let decoded = OperatorStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node.0, 0xDEADBEEF);
		assert_eq!(decoded.key, vec![1, 2, 3, 4]);
	}

	#[test]
	fn test_encode_decode_empty_key() {
		let key = OperatorStateKey {
			node: OperatorId(0xDEADBEEF),
			key: vec![],
		};
		let encoded = key.encode();

		let decoded = OperatorStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node.0, 0xDEADBEEF);
		assert_eq!(decoded.key, Vec::<u8>::new());
	}

	#[test]
	fn test_new() {
		let key = OperatorStateKey::new(OperatorId(42), vec![5, 6, 7]);
		assert_eq!(key.node.0, 42);
		assert_eq!(key.key, vec![5, 6, 7]);
	}

	#[test]
	fn test_new_empty() {
		let key = OperatorStateKey::new_empty(OperatorId(42));
		assert_eq!(key.node.0, 42);
		assert_eq!(key.key, Vec::<u8>::new());
	}

	#[test]
	fn test_roundtrip() {
		let original = OperatorStateKey {
			node: OperatorId(999_999_999),
			key: vec![10, 20, 30, 40, 50],
		};
		let encoded = original.encode();
		let decoded = OperatorStateKey::decode(&encoded).unwrap();
		assert_eq!(original, decoded);
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = Vec::new();
		encoded.push(0xFF);
		encoded.push(0xEC);
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(OperatorStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = Vec::new();
		encoded.push(0xFE);
		encoded.push(0xFF);
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(OperatorStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_too_short() {
		let mut encoded = Vec::new();
		encoded.push(0xFE);
		encoded.push(0xEC);
		encoded.extend(&999u32.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(OperatorStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_operator_state_key_range() {
		let node = OperatorId(42);
		let range = OperatorStateKeyRange::new(node);

		let start = range.start().unwrap();
		let decoded_start = OperatorStateKey::decode(&start).unwrap();
		assert_eq!(decoded_start.node, node);
		assert_eq!(decoded_start.key, Vec::<u8>::new());

		let end = range.end().unwrap();
		let decoded_end = OperatorStateKey::decode(&end).unwrap();
		assert_eq!(decoded_end.node.0, 41);
		assert_eq!(decoded_end.key, Vec::<u8>::new());
	}

	#[test]
	fn test_operator_state_key_range_decode() {
		let node = OperatorId(100);
		let range = OperatorStateKeyRange::new(node);

		let encoded_range = EncodedKeyRange::start_end(range.start(), range.end());

		let (start_decoded, end_decoded) = OperatorStateKeyRange::decode(&encoded_range);

		assert!(start_decoded.is_some());
		assert_eq!(start_decoded.unwrap().node, node);

		assert!(end_decoded.is_some());
		assert_eq!(end_decoded.unwrap().node.0, 99);
	}

	#[test]
	fn test_node_range_method() {
		let node = OperatorId(555);
		let range = OperatorStateKey::node_range(node);

		let (start_range, end_range) = OperatorStateKeyRange::decode(&range);

		assert!(start_range.is_some());
		assert_eq!(start_range.unwrap().node, node);

		assert!(end_range.is_some());
		assert_eq!(end_range.unwrap().node.0, 554);
	}
}
