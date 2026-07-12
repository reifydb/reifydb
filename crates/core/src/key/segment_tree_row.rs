// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::{id::SegmentTreeId, shape::ShapeId},
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentTreeRowKey {
	pub segment_tree: SegmentTreeId,
	pub key: u64,
	pub sequence: u64,
}

impl EncodableKey for SegmentTreeRowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let object = ShapeId::SegmentTree(self.segment_tree);
		let mut serializer = KeySerializer::with_capacity(27);
		serializer.extend_u8(Self::KIND as u8).extend_shape_id(object);
		serializer.extend_u64(self.key).extend_u64(self.sequence);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_shape_id().ok()?;
		let segment_tree = match object {
			ShapeId::SegmentTree(id) => id,
			_ => return None,
		};

		let key = de.read_u64().ok()?;
		let sequence = de.read_u64().ok()?;

		Some(Self {
			segment_tree,
			key,
			sequence,
		})
	}
}

#[derive(Debug, Clone)]
pub struct SegmentTreeRowKeyRange {
	pub segment_tree: SegmentTreeId,
	pub key_start: Option<u64>,
	pub key_end: Option<u64>,
}

impl SegmentTreeRowKeyRange {
	pub fn full_scan(segment_tree: SegmentTreeId) -> EncodedKeyRange {
		let range = SegmentTreeRowKeyRange {
			segment_tree,
			key_start: None,
			key_end: None,
		};
		EncodedKeyRange::new(Bound::Included(range.start_key()), Bound::Included(range.end_key()))
	}

	pub fn scan_range(
		segment_tree: SegmentTreeId,
		key_start: Option<u64>,
		key_end: Option<u64>,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
		if matches!(key_end, Some(0)) {
			let empty = EncodedKey::new(Vec::<u8>::new());
			return EncodedKeyRange::new(Bound::Excluded(empty.clone()), Bound::Excluded(empty));
		}

		let range = SegmentTreeRowKeyRange {
			segment_tree,
			key_start,
			key_end,
		};

		let start = if let Some(last_key) = last_key {
			Bound::Excluded(last_key.clone())
		} else {
			Bound::Included(range.start_key())
		};

		EncodedKeyRange::new(start, Bound::Included(range.end_key()))
	}

	fn start_key(&self) -> EncodedKey {
		let object = ShapeId::SegmentTree(self.segment_tree);
		let mut serializer = KeySerializer::with_capacity(27);
		serializer.extend_u8(KeyKind::Row as u8).extend_shape_id(object);

		if let Some(key_val) = self.key_end {
			serializer.extend_u64(key_val - 1);
		}
		serializer.to_encoded_key()
	}

	fn end_key(&self) -> EncodedKey {
		if let Some(key_val) = self.key_start {
			let object = ShapeId::SegmentTree(self.segment_tree);
			let mut serializer = KeySerializer::with_capacity(27);
			serializer.extend_u8(KeyKind::Row as u8).extend_shape_id(object);

			serializer.extend_u64(key_val).extend_u64(0u64);
			serializer.to_encoded_key()
		} else {
			let object = ShapeId::SegmentTree(self.segment_tree);
			let mut serializer = KeySerializer::with_capacity(10);
			serializer.extend_u8(KeyKind::Row as u8).extend_shape_id(object.prev());
			serializer.to_encoded_key()
		}
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::HashSet, ops::RangeBounds};

	use super::*;
	use crate::key::series_row::SeriesRowKey;

	#[test]
	fn test_encode_decode_roundtrip() {
		let key = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(42),
			key: 1706745600000,
			sequence: 1,
		};
		let encoded = key.encode();
		let decoded = SegmentTreeRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.segment_tree, SegmentTreeId(42));
		assert_eq!(decoded.key, 1706745600000);
		assert_eq!(decoded.sequence, 1);
	}

	#[test]
	fn test_decode_rejects_wrong_shape() {
		let key = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(42),
			key: 1,
			sequence: 1,
		};
		let encoded = key.encode();
		// SeriesRowKey shares the same KeyKind::Row physical layout but a different
		// ShapeId discriminator byte; decode must reject it as a SegmentTreeRowKey.
		assert!(SeriesRowKey::decode(&encoded).is_none());
	}

	#[test]
	fn test_ordering_by_key() {
		let key1 = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(1),
			key: 100,
			sequence: 0,
		};
		let key2 = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(1),
			key: 200,
			sequence: 0,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();

		assert!(e1 > e2, "key descending ordering not preserved");
	}

	#[test]
	fn test_ordering_by_sequence() {
		let key1 = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(1),
			key: 100,
			sequence: 1,
		};
		let key2 = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(1),
			key: 100,
			sequence: 2,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();

		assert!(e1 > e2, "sequence descending ordering not preserved");
	}

	#[test]
	fn test_prefix_clustering() {
		let mut keys: Vec<EncodedKey> = vec![
			SegmentTreeRowKey {
				segment_tree: SegmentTreeId(1),
				key: 10,
				sequence: 0,
			}
			.encode(),
			SegmentTreeRowKey {
				segment_tree: SegmentTreeId(2),
				key: 5,
				sequence: 0,
			}
			.encode(),
			SegmentTreeRowKey {
				segment_tree: SegmentTreeId(1),
				key: 20,
				sequence: 0,
			}
			.encode(),
			SegmentTreeRowKey {
				segment_tree: SegmentTreeId(2),
				key: 15,
				sequence: 0,
			}
			.encode(),
		];
		keys.sort();

		let tree_of = |k: &EncodedKey| SegmentTreeRowKey::decode(k).unwrap().segment_tree;
		let trees: Vec<_> = keys.iter().map(tree_of).collect();
		// All keys for one tree must be contiguous once sorted.
		let mut seen = HashSet::new();
		let mut last = None;
		for t in &trees {
			if last != Some(*t) && !seen.insert(*t) {
				panic!("tree {:?} keys are not contiguous: {:?}", t, trees);
			}
			last = Some(*t);
		}
	}

	#[test]
	fn test_full_scan_range_covers_tree() {
		let key1 = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(7),
			key: 0,
			sequence: 0,
		}
		.encode();
		let key2 = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(7),
			key: u64::MAX,
			sequence: u64::MAX,
		}
		.encode();
		let other_tree = SegmentTreeRowKey {
			segment_tree: SegmentTreeId(8),
			key: 0,
			sequence: 0,
		}
		.encode();

		let range = SegmentTreeRowKeyRange::full_scan(SegmentTreeId(7));
		assert!(range.contains(&key1));
		assert!(range.contains(&key2));
		assert!(!range.contains(&other_tree));
	}
}
