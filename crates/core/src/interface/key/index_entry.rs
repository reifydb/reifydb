// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{IndexId, SourceId},
	util::{
		CowVec,
		encoding::keycode::{self, KeySerializer},
	},
	value::index::{EncodedIndexKey, EncodedIndexKeyRange},
};

const VERSION: u8 = 1;

/// Key for storing actual index entries with the encoded index key data
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntryKey {
	pub source: SourceId,
	pub index: IndexId,
	pub key: EncodedIndexKey,
}

impl IndexEntryKey {
	pub fn new(source: impl Into<SourceId>, index: IndexId, key: EncodedIndexKey) -> Self {
		let source = source.into();
		Self {
			source,
			index,
			key,
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntryKeyRange {
	pub source: SourceId,
	pub index: IndexId,
}

impl IndexEntryKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		if key.len() < 20 {
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
		if payload.len() < 18 {
			// 9 bytes for source + 9 bytes for index
			return None;
		}

		let source = keycode::deserialize_source_id(&payload[..9]).ok()?;
		let index = keycode::deserialize_index_id(&payload[9..18]).ok()?;

		Some(IndexEntryKeyRange {
			source,
			index,
		})
	}
}

impl EncodableKeyRange for IndexEntryKeyRange {
	const KIND: KeyKind = KeyKind::IndexEntry;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(20); // 1 + 1 + 9 + 9
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_source_id(self.source)
			.extend_index_id(self.index);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(20);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_source_id(self.source)
			.extend_index_id(self.index.prev());
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

impl EncodableKey for IndexEntryKey {
	const KIND: KeyKind = KeyKind::IndexEntry;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(20 + self.key.len());
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_source_id(self.source)
			.extend_index_id(self.index)
			// Append the raw index key bytes
			.extend_raw(self.key.as_slice());
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 20 {
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
		if payload.len() < 18 {
			// 9 bytes for source + 9 bytes for index
			return None;
		}

		let source = keycode::deserialize_source_id(&payload[..9]).ok()?;
		let index = keycode::deserialize_index_id(&payload[9..18]).ok()?;

		// The remaining bytes are the index key
		if payload.len() > 18 {
			let index_key_bytes = &payload[18..];
			let index_key = EncodedIndexKey(CowVec::new(index_key_bytes.to_vec()));
			Some(Self {
				source,
				index,
				key: index_key,
			})
		} else {
			None
		}
	}
}

impl IndexEntryKey {
	/// Create a range for scanning all entries of a specific index
	pub fn index_range(source: impl Into<SourceId>, index: IndexId) -> EncodedKeyRange {
		let range = IndexEntryKeyRange {
			source: source.into(),
			index,
		};
		EncodedKeyRange::new(Bound::Included(range.start().unwrap()), Bound::Excluded(range.end().unwrap()))
	}

	/// Create a range for scanning all entries of a source (all indexes)
	pub fn source_range(source: impl Into<SourceId>) -> EncodedKeyRange {
		let source = source.into();
		let mut start_serializer = KeySerializer::with_capacity(11);
		start_serializer.extend_u8(VERSION).extend_u8(KeyKind::IndexEntry as u8).extend_source_id(source);

		let next_source = source.next();
		let mut end_serializer = KeySerializer::with_capacity(11);
		end_serializer.extend_u8(VERSION).extend_u8(KeyKind::IndexEntry as u8).extend_source_id(next_source);

		EncodedKeyRange {
			start: Bound::Included(start_serializer.to_encoded_key()),
			end: Bound::Excluded(end_serializer.to_encoded_key()),
		}
	}

	/// Create a range for scanning entries within an index with a specific
	/// key prefix
	pub fn key_prefix_range(source: impl Into<SourceId>, index: IndexId, key_prefix: &[u8]) -> EncodedKeyRange {
		let source = source.into();
		let mut serializer = KeySerializer::with_capacity(20 + key_prefix.len());
		serializer
			.extend_u8(VERSION)
			.extend_u8(KeyKind::IndexEntry as u8)
			.extend_source_id(source)
			.extend_index_id(index)
			.extend_raw(key_prefix);
		let start = serializer.to_encoded_key();

		// For the end key, append 0xFF to get all keys with this prefix
		let mut end = start.as_slice().to_vec();
		end.push(0xFF);

		EncodedKeyRange {
			start: Bound::Included(start),
			end: Bound::Excluded(EncodedKey::new(end)),
		}
	}

	/// Create a range for entries from an EncodedIndexKeyRange
	/// This method leverages the EncodedIndexKeyRange type for cleaner
	/// range handling.
	pub fn key_range(
		source: impl Into<SourceId>,
		index: IndexId,
		index_range: EncodedIndexKeyRange,
	) -> EncodedKeyRange {
		let source = source.into();
		// Build the prefix for this source and index
		let mut prefix_serializer = KeySerializer::with_capacity(20);
		prefix_serializer
			.extend_u8(VERSION)
			.extend_u8(KeyKind::IndexEntry as u8)
			.extend_source_id(source)
			.extend_index_id(index);
		let prefix = prefix_serializer.to_encoded_key().to_vec();

		// Convert bounds to include the prefix
		let start = match index_range.start {
			Bound::Included(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Included(EncodedKey::new(bytes))
			}
			Bound::Excluded(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Excluded(EncodedKey::new(bytes))
			}
			Bound::Unbounded => {
				// Start from the beginning of this index
				Bound::Included(EncodedKey::new(prefix.clone()))
			}
		};

		let end = match index_range.end {
			Bound::Included(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Included(EncodedKey::new(bytes))
			}
			Bound::Excluded(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Excluded(EncodedKey::new(bytes))
			}
			Bound::Unbounded => {
				// End at the beginning of the next index
				let mut serializer = KeySerializer::with_capacity(20);
				serializer
					.extend_u8(VERSION)
					.extend_u8(KeyKind::IndexEntry as u8)
					.extend_source_id(source)
					// Use prev() for end bound in descending order
					.extend_index_id(index.prev());
				Bound::Excluded(serializer.to_encoded_key())
			}
		};

		EncodedKeyRange {
			start,
			end,
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use super::*;
	use crate::{SortDirection, value::index::EncodedIndexLayout};

	#[test]
	fn test_encode_decode() {
		// Create a simple index key
		let layout = EncodedIndexLayout::new(
			&[Type::Uint8, Type::RowNumber],
			&[SortDirection::Asc, SortDirection::Asc],
		)
		.unwrap();

		let mut index_key = layout.allocate_key();
		layout.set_u64(&mut index_key, 0, 100u64);
		layout.set_row_number(&mut index_key, 1, 1u64);

		let entry = IndexEntryKey {
			source: SourceId::table(42),
			index: IndexId::primary(7),
			key: index_key.clone(),
		};

		let encoded = entry.encode();
		let decoded = IndexEntryKey::decode(&encoded).unwrap();

		assert_eq!(decoded.source, SourceId::table(42));
		assert_eq!(decoded.index, IndexId::primary(7));
		assert_eq!(decoded.key.as_slice(), index_key.as_slice());
	}

	#[test]
	fn test_ordering() {
		let layout = EncodedIndexLayout::new(&[Type::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key1 = layout.allocate_key();
		layout.set_u64(&mut key1, 0, 100u64);

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);

		// Same source and index, different keys
		let entry1 = IndexEntryKey {
			source: SourceId::table(1),
			index: IndexId::primary(1),
			key: key1,
		};

		let entry2 = IndexEntryKey {
			source: SourceId::table(1),
			index: IndexId::primary(1),
			key: key2,
		};

		let encoded1 = entry1.encode();
		let encoded2 = entry2.encode();

		// entry1 should come before entry2 because 100 < 200
		assert!(encoded1.as_slice() < encoded2.as_slice());
	}

	#[test]
	fn test_index_range() {
		let range = IndexEntryKey::index_range(SourceId::table(10), IndexId::primary(5));

		// Create entries that should be included
		let layout = EncodedIndexLayout::new(&[Type::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 50u64);

		let entry = IndexEntryKey {
			source: SourceId::table(10),
			index: IndexId::primary(5),
			key,
		};

		let encoded = entry.encode();

		// Check that the entry falls within the range
		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		} else {
			panic!("Expected Included/Excluded bounds");
		}

		// Entry with different index should not be in range
		// Note: Due to keycode encoding, IndexId(6) will have a smaller
		// encoded value than IndexId(5) since keycode inverts bits
		// (larger numbers become smaller byte sequences)
		let entry2 = IndexEntryKey {
			source: SourceId::table(10),
			index: IndexId::primary(6),
			key: layout.allocate_key(),
		};

		let encoded2 = entry2.encode();
		// The entry with IndexId(6) should not be within the range for
		// IndexId(5)
		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			// encoded2 should either be < start or >= end
			assert!(encoded2.as_slice() < start.as_slice() || encoded2.as_slice() >= end.as_slice());
		}
	}

	#[test]
	fn test_key_prefix_range() {
		let layout = EncodedIndexLayout::new(
			&[Type::Uint8, Type::RowNumber],
			&[SortDirection::Asc, SortDirection::Asc],
		)
		.unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 100u64);
		layout.set_row_number(&mut key, 1, 0u64); // Set to 0 to get the minimal key with this prefix

		// Use the full encoded key up to the first field as the prefix
		let prefix = &key.as_slice()[..layout.fields[1].offset]; // Include bitvec and first field
		let range = IndexEntryKey::key_prefix_range(SourceId::table(1), IndexId::primary(1), prefix);

		// Now create a full key with the same prefix
		layout.set_row_number(&mut key, 1, 999u64);
		let entry = IndexEntryKey {
			source: SourceId::table(1),
			index: IndexId::primary(1),
			key: key.clone(),
		};

		let encoded = entry.encode();

		// Should be within range
		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		}

		// Create a key with different prefix
		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64); // Different first field
		layout.set_row_number(&mut key2, 1, 1u64);

		let entry2 = IndexEntryKey {
			source: SourceId::table(1),
			index: IndexId::primary(1),
			key: key2,
		};

		let encoded2 = entry2.encode();

		// Should not be in range
		if let Bound::Excluded(end) = &range.end {
			assert!(encoded2.as_slice() >= end.as_slice());
		}
	}
}
