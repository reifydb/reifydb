// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::partition::Partition;

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::SegmentTreeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentTreeScope {
	Global,
	Partition(Partition),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentTreeNodeKey {
	pub segment_tree: SegmentTreeId,
	pub scope: SegmentTreeScope,
	pub level: u8,
	pub bucket: u64,
}

impl SegmentTreeNodeKey {
	pub fn new(segment_tree: SegmentTreeId, scope: SegmentTreeScope, level: u8, bucket: u64) -> Self {
		Self {
			segment_tree,
			scope,
			level,
			bucket,
		}
	}

	pub fn encoded(
		segment_tree: impl Into<SegmentTreeId>,
		scope: SegmentTreeScope,
		level: u8,
		bucket: u64,
	) -> EncodedKey {
		Self::new(segment_tree.into(), scope, level, bucket).encode()
	}

	pub fn tree_prefix_range(segment_tree: SegmentTreeId) -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(9);
		start.extend_u8(Self::KIND as u8).extend_u64(segment_tree);
		let mut end = KeySerializer::with_capacity(9);
		end.extend_u8(Self::KIND as u8).extend_u64(segment_tree.0.wrapping_sub(1));
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}
}

impl EncodableKey for SegmentTreeNodeKey {
	const KIND: KeyKind = KeyKind::SegmentTreeNode;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(36);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.segment_tree);
		match &self.scope {
			SegmentTreeScope::Global => {
				serializer.extend_u8(0u8);
			}
			SegmentTreeScope::Partition(partition) => {
				serializer.extend_u8(1u8).extend_u128(partition.0);
			}
		}
		serializer.extend_u8(self.level).extend_u64(self.bucket);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let segment_tree = SegmentTreeId(de.read_u64().ok()?);

		let scope_tag = de.read_u8().ok()?;
		let scope = match scope_tag {
			0 => SegmentTreeScope::Global,
			1 => SegmentTreeScope::Partition(Partition(de.read_u128().ok()?)),
			_ => return None,
		};

		let level = de.read_u8().ok()?;
		let bucket = de.read_u64().ok()?;

		Some(Self {
			segment_tree,
			scope,
			level,
			bucket,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use super::*;

	#[test]
	fn test_roundtrip_global_scope() {
		let key = SegmentTreeNodeKey::new(SegmentTreeId(7), SegmentTreeScope::Global, 3, 100);
		let encoded = key.encode();
		let decoded = SegmentTreeNodeKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_roundtrip_partition_scope() {
		let key = SegmentTreeNodeKey::new(
			SegmentTreeId(7),
			SegmentTreeScope::Partition(Partition(12345)),
			3,
			100,
		);
		let encoded = key.encode();
		let decoded = SegmentTreeNodeKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_ordering_by_bucket_is_descending() {
		let e1 = SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Global, 1, 100);
		let e2 = SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Global, 1, 200);
		assert!(e1 > e2, "bucket descending ordering not preserved");
	}

	#[test]
	fn test_ordering_by_level_is_descending() {
		let e1 = SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Global, 1, 0);
		let e2 = SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Global, 2, 0);
		assert!(e1 > e2, "level descending ordering not preserved");
	}

	#[test]
	fn test_prefix_clustering_by_tree() {
		let mut keys: Vec<EncodedKey> = vec![
			SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Global, 1, 0),
			SegmentTreeNodeKey::encoded(SegmentTreeId(2), SegmentTreeScope::Global, 1, 0),
			SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Global, 2, 5),
			SegmentTreeNodeKey::encoded(SegmentTreeId(2), SegmentTreeScope::Global, 2, 5),
		];
		keys.sort();

		let tree_of = |k: &EncodedKey| SegmentTreeNodeKey::decode(k).unwrap().segment_tree;
		let trees: Vec<_> = keys.iter().map(tree_of).collect();
		let mut seen = std::collections::HashSet::new();
		let mut last = None;
		for t in &trees {
			if last != Some(*t) && !seen.insert(*t) {
				panic!("tree {:?} keys are not contiguous: {:?}", t, trees);
			}
			last = Some(*t);
		}
	}

	#[test]
	fn test_prefix_clustering_within_partition_scope() {
		let mut keys: Vec<EncodedKey> = vec![
			SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Partition(Partition(1)), 1, 0),
			SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Partition(Partition(2)), 1, 0),
			SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Partition(Partition(1)), 2, 5),
			SegmentTreeNodeKey::encoded(SegmentTreeId(1), SegmentTreeScope::Partition(Partition(2)), 2, 5),
		];
		keys.sort();

		let partition_of = |k: &EncodedKey| match SegmentTreeNodeKey::decode(k).unwrap().scope {
			SegmentTreeScope::Partition(p) => p,
			SegmentTreeScope::Global => panic!("expected partition scope"),
		};
		let partitions: Vec<_> = keys.iter().map(partition_of).collect();
		let mut seen = std::collections::HashSet::new();
		let mut last = None;
		for p in &partitions {
			if last != Some(*p) && !seen.insert(*p) {
				panic!("partition {:?} keys are not contiguous: {:?}", p, partitions);
			}
			last = Some(*p);
		}
	}

	#[test]
	fn test_tree_prefix_range_covers_exactly_the_tree() {
		let range = SegmentTreeNodeKey::tree_prefix_range(SegmentTreeId(7));

		let in_tree_1 = SegmentTreeNodeKey::encoded(SegmentTreeId(7), SegmentTreeScope::Global, 1, 0);
		let in_tree_2 = SegmentTreeNodeKey::encoded(
			SegmentTreeId(7),
			SegmentTreeScope::Partition(Partition(99)),
			16,
			u64::MAX,
		);
		let other_tree = SegmentTreeNodeKey::encoded(SegmentTreeId(8), SegmentTreeScope::Global, 1, 0);

		assert!(range.contains(&in_tree_1));
		assert!(range.contains(&in_tree_2));
		assert!(!range.contains(&other_tree));
	}
}
