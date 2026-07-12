// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::SegmentTreeId;

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentTreeKey {
	pub segment_tree: SegmentTreeId,
}

impl SegmentTreeKey {
	pub fn new(segment_tree: SegmentTreeId) -> Self {
		Self {
			segment_tree,
		}
	}

	pub fn encoded(segment_tree: impl Into<SegmentTreeId>) -> EncodedKey {
		Self::new(segment_tree.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::segment_tree_start()), Some(Self::segment_tree_end()))
	}

	fn segment_tree_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn segment_tree_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for SegmentTreeKey {
	const KIND: KeyKind = KeyKind::SegmentTree;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.segment_tree);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let segment_tree = de.read_u64().ok()?;

		Some(Self {
			segment_tree: SegmentTreeId(segment_tree),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentTreeMetadataKey {
	pub segment_tree: SegmentTreeId,
}

impl SegmentTreeMetadataKey {
	pub fn new(segment_tree: SegmentTreeId) -> Self {
		Self {
			segment_tree,
		}
	}

	pub fn encoded(segment_tree: impl Into<SegmentTreeId>) -> EncodedKey {
		Self::new(segment_tree.into()).encode()
	}
}

impl EncodableKey for SegmentTreeMetadataKey {
	const KIND: KeyKind = KeyKind::SegmentTreeMetadata;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.segment_tree);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let segment_tree = de.read_u64().ok()?;

		Some(Self {
			segment_tree: SegmentTreeId(segment_tree),
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use super::*;

	#[test]
	fn test_segment_tree_key_roundtrip() {
		let key = SegmentTreeKey::new(SegmentTreeId(42));
		let encoded = key.encode();
		let decoded = SegmentTreeKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_segment_tree_metadata_key_roundtrip() {
		let key = SegmentTreeMetadataKey::new(SegmentTreeId(42));
		let encoded = key.encode();
		let decoded = SegmentTreeMetadataKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_segment_tree_key_ordering() {
		let e1 = SegmentTreeKey::encoded(SegmentTreeId(1));
		let e2 = SegmentTreeKey::encoded(SegmentTreeId(2));
		assert!(e2 < e1, "segment tree id 2 should encode smaller than id 1 (descending)");
	}

	#[test]
	fn test_segment_tree_key_full_scan_covers_all() {
		let range = SegmentTreeKey::full_scan();
		assert!(range.contains(&SegmentTreeKey::encoded(SegmentTreeId(1))));
		assert!(range.contains(&SegmentTreeKey::encoded(SegmentTreeId(u64::MAX))));
		assert!(!range.contains(&SegmentTreeMetadataKey::encoded(SegmentTreeId(1))));
	}
}
