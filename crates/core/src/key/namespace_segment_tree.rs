// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::{NamespaceId, SegmentTreeId};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceSegmentTreeKey {
	pub namespace: NamespaceId,
	pub segment_tree: SegmentTreeId,
}

impl NamespaceSegmentTreeKey {
	pub fn new(namespace: NamespaceId, segment_tree: SegmentTreeId) -> Self {
		Self {
			namespace,
			segment_tree,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, segment_tree: impl Into<SegmentTreeId>) -> EncodedKey {
		Self::new(namespace.into(), segment_tree.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for NamespaceSegmentTreeKey {
	const KIND: KeyKind = KeyKind::NamespaceSegmentTree;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.namespace).extend_u64(self.segment_tree);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let namespace = de.read_u64().ok()?;
		let segment_tree = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			segment_tree: SegmentTreeId(segment_tree),
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use super::*;

	#[test]
	fn test_roundtrip() {
		let key = NamespaceSegmentTreeKey::new(NamespaceId(3), SegmentTreeId(42));
		let encoded = key.encode();
		let decoded = NamespaceSegmentTreeKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_full_scan_is_per_namespace() {
		let range = NamespaceSegmentTreeKey::full_scan(NamespaceId(3));
		assert!(range.contains(&NamespaceSegmentTreeKey::encoded(NamespaceId(3), SegmentTreeId(1))));
		assert!(range.contains(&NamespaceSegmentTreeKey::encoded(NamespaceId(3), SegmentTreeId(u64::MAX))));
		assert!(!range.contains(&NamespaceSegmentTreeKey::encoded(NamespaceId(4), SegmentTreeId(1))));
	}

	#[test]
	fn test_prefix_clustering_by_namespace() {
		let mut keys: Vec<EncodedKey> = vec![
			NamespaceSegmentTreeKey::encoded(NamespaceId(1), SegmentTreeId(10)),
			NamespaceSegmentTreeKey::encoded(NamespaceId(2), SegmentTreeId(5)),
			NamespaceSegmentTreeKey::encoded(NamespaceId(1), SegmentTreeId(20)),
			NamespaceSegmentTreeKey::encoded(NamespaceId(2), SegmentTreeId(15)),
		];
		keys.sort();

		let ns_of = |k: &EncodedKey| NamespaceSegmentTreeKey::decode(k).unwrap().namespace;
		let namespaces: Vec<_> = keys.iter().map(ns_of).collect();
		let mut seen = std::collections::HashSet::new();
		let mut last = None;
		for ns in &namespaces {
			if last != Some(*ns) && !seen.insert(*ns) {
				panic!("namespace {:?} keys are not contiguous: {:?}", ns, namespaces);
			}
			last = Some(*ns);
		}
	}
}
