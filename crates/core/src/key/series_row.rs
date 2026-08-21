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
	interface::catalog::{object::ObjectId, storage::StorageId},
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesRowKey {
	pub storage: StorageId,
	pub variant_tag: Option<u8>,
	pub key: u64,
	pub sequence: u64,
}

impl EncodableKey for SeriesRowKey {
	const KIND: KeyKind = KeyKind::SeriesRow;

	fn encode(&self) -> EncodedKey {
		let object = ObjectId::from(self.storage);
		let capacity = if self.variant_tag.is_some() {
			28
		} else {
			27
		};
		let mut serializer = KeySerializer::with_capacity(capacity);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(object);
		match self.variant_tag {
			Some(tag) => {
				serializer.extend_u8(1u8).extend_u8(tag);
			}
			None => {
				serializer.extend_u8(0u8);
			}
		}
		serializer.extend_u64(self.key).extend_u64(self.sequence);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let storage = StorageId::from_object(de.read_object_id().ok()?)?;

		let variant_tag = match de.read_u8().ok()? {
			1 => Some(de.read_u8().ok()?),
			0 => None,
			_ => return None,
		};

		let key = de.read_u64().ok()?;
		let sequence = de.read_u64().ok()?;

		Some(Self {
			storage,
			variant_tag,
			key,
			sequence,
		})
	}
}

#[derive(Debug, Clone)]
pub struct SeriesRowKeyRange {
	pub storage: StorageId,
	pub variant_tag: Option<u8>,
	pub key_start: Option<u64>,
	pub key_end: Option<u64>,
}

impl SeriesRowKeyRange {
	pub fn full_scan(storage: StorageId, variant_tag: Option<u8>) -> EncodedKeyRange {
		let range = SeriesRowKeyRange {
			storage,
			variant_tag,
			key_start: None,
			key_end: None,
		};
		EncodedKeyRange::new(Bound::Included(range.start_key()), Bound::Included(range.end_key()))
	}

	pub fn scan_range(
		storage: StorageId,
		variant_tag: Option<u8>,
		key_start: Option<u64>,
		key_end: Option<u64>,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
		if matches!(key_end, Some(0)) {
			let empty = EncodedKey::new(Vec::<u8>::new());
			return EncodedKeyRange::new(Bound::Excluded(empty.clone()), Bound::Excluded(empty));
		}

		let range = SeriesRowKeyRange {
			storage,
			variant_tag,
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

	pub fn decode_storage(key: &EncodedKey) -> Option<StorageId> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != SeriesRowKey::KIND {
			return None;
		}

		StorageId::from_object(de.read_object_id().ok()?)
	}

	pub fn decode(range: &EncodedKeyRange) -> (Option<StorageId>, Option<StorageId>) {
		let start = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_storage(key),
			Bound::Unbounded => None,
		};

		let end = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_storage(key),
			Bound::Unbounded => None,
		};

		(start, end)
	}

	fn start_key(&self) -> EncodedKey {
		let object = ObjectId::from(self.storage);
		let mut serializer = KeySerializer::with_capacity(27);
		serializer.extend_u8(SeriesRowKey::KIND as u8).extend_object_id(object);
		match self.variant_tag {
			Some(tag) => {
				serializer.extend_u8(1u8).extend_u8(tag);
			}
			None if self.key_start.is_some() || self.key_end.is_some() => {
				serializer.extend_u8(0u8);
			}
			None => {}
		}

		if let Some(key_val) = self.key_end {
			serializer.extend_u64(key_val - 1);
		}
		serializer.to_encoded_key()
	}

	fn end_key(&self) -> EncodedKey {
		if let Some(key_val) = self.key_start {
			let object = ObjectId::from(self.storage);
			let mut serializer = KeySerializer::with_capacity(27);
			serializer.extend_u8(SeriesRowKey::KIND as u8).extend_object_id(object);
			match self.variant_tag {
				Some(tag) => {
					serializer.extend_u8(1u8).extend_u8(tag);
				}
				None => {
					serializer.extend_u8(0u8);
				}
			}

			serializer.extend_u64(key_val).extend_u64(0u64);
			serializer.to_encoded_key()
		} else {
			let object = ObjectId::from(self.storage);
			let mut serializer = KeySerializer::with_capacity(10);
			serializer.extend_u8(SeriesRowKey::KIND as u8).extend_object_id(object.prev());
			serializer.to_encoded_key()
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use super::*;
	use crate::{
		interface::catalog::id::{SeriesId, ViewId},
		key::{EncodableKeyRange, row::RowKeyRange},
	};

	#[test]
	fn test_encode_decode_without_tag() {
		// Without the flag byte a missing tag shifts every later field by one and the key reads back wrong.
		let key = SeriesRowKey {
			storage: StorageId::Series(SeriesId(42)),
			variant_tag: None,
			key: 1706745600000,
			sequence: 1,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.storage, StorageId::Series(SeriesId(42)));
		assert_eq!(decoded.variant_tag, None);
		assert_eq!(decoded.key, 1706745600000);
		assert_eq!(decoded.sequence, 1);
	}

	#[test]
	fn test_encode_decode_with_tag() {
		// A tagged key must report the exact tag it was written with, never a byte borrowed from the key.
		let key = SeriesRowKey {
			storage: StorageId::Series(SeriesId(42)),
			variant_tag: Some(3),
			key: 1706745600000,
			sequence: 5,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.storage, StorageId::Series(SeriesId(42)));
		assert_eq!(decoded.variant_tag, Some(3));
		assert_eq!(decoded.key, 1706745600000);
		assert_eq!(decoded.sequence, 5);
	}

	#[test]
	fn test_a_view_storage_round_trips() {
		// Widening off SeriesId is pointless unless a non-series storage keeps its own tag through decode.
		let key = SeriesRowKey {
			storage: StorageId::View(ViewId(42)),
			variant_tag: Some(7),
			key: 900,
			sequence: 2,
		};
		let decoded = SeriesRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_full_scan_range_still_names_its_storage() {
		// classify_range routes a series scan to its own physical entry; an undecodable range falls to Multi.
		let range = SeriesRowKeyRange::full_scan(StorageId::Series(SeriesId(42)), None);
		let (start, end) = SeriesRowKeyRange::decode(&range);
		assert_eq!(start, Some(StorageId::Series(SeriesId(42))));
		assert_eq!(
			end,
			Some(StorageId::Series(SeriesId(41))),
			"the exclusive end brackets the next series id down"
		);
	}

	#[test]
	fn test_untagged_full_scan_covers_tagged_rows() {
		// A flag byte in the range bounds would pin the scan to flag=0 and silently drop every tagged row.
		let storage = StorageId::Series(SeriesId(9));
		let range = SeriesRowKeyRange::full_scan(storage, None);
		let untagged = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 500,
			sequence: 0,
		}
		.encode();
		let tagged = SeriesRowKey {
			storage,
			variant_tag: Some(4),
			key: 500,
			sequence: 0,
		}
		.encode();

		assert!(range.contains(&untagged));
		assert!(range.contains(&tagged), "an untagged full scan must still see tagged rows");
	}

	#[test]
	fn test_scan_range_brackets_the_rows_it_selects() {
		// The range must contain a key inside the window and exclude one outside, or eviction skips live rows.
		let storage = StorageId::Series(SeriesId(1));
		let range = SeriesRowKeyRange::scan_range(storage, None, Some(100), Some(200), None);
		let inside = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 150,
			sequence: 1,
		}
		.encode();
		let below = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 99,
			sequence: 1,
		}
		.encode();
		let above = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 201,
			sequence: 1,
		}
		.encode();

		assert!(range.contains(&inside));
		assert!(!range.contains(&below));
		assert!(!range.contains(&above));
	}

	#[test]
	fn test_row_key_range_never_claims_a_series_range() {
		// A shared kind byte made a series scan classify as a plain row scan of the same object id.
		let range = SeriesRowKeyRange::full_scan(StorageId::Series(SeriesId(42)), None);
		assert_eq!(RowKeyRange::decode(&range), (None, None));
	}

	#[test]
	fn test_ordering_by_key() {
		// Reads walk newest first; an ascending key encoding would hand back the oldest rows instead.
		let key1 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 100,
			sequence: 0,
		};
		let key2 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 200,
			sequence: 0,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();

		assert!(e1 > e2, "key descending ordering not preserved");
	}

	#[test]
	fn test_ordering_by_sequence() {
		// Two rows at the same key must still come back newest sequence first.
		let key1 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 100,
			sequence: 1,
		};
		let key2 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 100,
			sequence: 2,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();

		assert!(e1 > e2, "sequence descending ordering not preserved");
	}

	#[test]
	fn test_half_bounded_untagged_range_excludes_tagged_rows() {
		// A range bounded on one side only must still pin its tag class: the untagged flag encodes 0xFF and
		// the tagged flag 0xFE, so omitting the flag from the start bound lets every tagged row of the series
		// sort into the window regardless of its key.
		let range = SeriesRowKeyRange::scan_range(StorageId::Series(SeriesId(7)), None, Some(100), None, None);

		let untagged = SeriesRowKey {
			storage: StorageId::Series(SeriesId(7)),
			variant_tag: None,
			key: 500,
			sequence: 1,
		}
		.encode();
		let tagged = SeriesRowKey {
			storage: StorageId::Series(SeriesId(7)),
			variant_tag: Some(3),
			key: 500,
			sequence: 1,
		}
		.encode();

		assert!(range.contains(&untagged), "an untagged row above the lower bound must stay in range");
		assert!(!range.contains(&tagged), "a tagged row must never leak into an untagged key-bounded range");
	}
}
