// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	index::{EncodedIndexKey, EncodedIndexKeyRange},
	interface::catalog::{IndexId, ViewId},
	util::{CowVec, encoding::keycode},
};

const VERSION: u8 = 1;

/// Key for storing actual index entries with the encoded index key data
#[derive(Debug, Clone, PartialEq)]
pub struct ViewIndexEntryKey {
	pub view: ViewId,
	pub index: IndexId,
	pub key: EncodedIndexKey,
}

impl ViewIndexEntryKey {
	pub fn new(view: ViewId, index: IndexId, key: EncodedIndexKey) -> Self {
		Self {
			view,
			index,
			key,
		}
	}
}

impl EncodableKey for ViewIndexEntryKey {
	const KIND: KeyKind = KeyKind::ViewIndexEntry;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(18 + self.key.len());
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.view));
		out.extend(&keycode::serialize(&self.index));
		// Append the raw index key bytes
		out.extend_from_slice(self.key.as_slice());

		EncodedKey::new(out)
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 18 {
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
		if payload.len() < 16 {
			return None;
		}

		let view: ViewId = keycode::deserialize(&payload[..8]).ok()?;
		let index: IndexId =
			keycode::deserialize(&payload[8..16]).ok()?;

		// The remaining bytes are the index key
		if payload.len() > 16 {
			let index_key_bytes = &payload[16..];
			let index_key = EncodedIndexKey(CowVec::new(
				index_key_bytes.to_vec(),
			));
			Some(Self {
				view,
				index,
				key: index_key,
			})
		} else {
			None
		}
	}
}

impl ViewIndexEntryKey {
	/// Create a range for scanning all entries of a specific index
	pub fn index_range(view: ViewId, index: IndexId) -> EncodedKeyRange {
		let mut start = Vec::with_capacity(18);
		start.extend(&keycode::serialize(&VERSION));
		start.extend(&keycode::serialize(&KeyKind::ViewIndexEntry));
		start.extend(&keycode::serialize(&view));
		start.extend(&keycode::serialize(&index));

		// For the end key, we append 0xFF to ensure we get all keys for
		// this index This works because any actual index key data
		// will make the key longer
		let mut end = start.clone();
		end.push(0xFF);

		EncodedKeyRange {
			start: Bound::Included(EncodedKey::new(start)),
			end: Bound::Excluded(EncodedKey::new(end)),
		}
	}

	/// Create a range for scanning all entries of a view (all indexes)
	pub fn view_range(view: ViewId) -> EncodedKeyRange {
		let mut start = Vec::with_capacity(10);
		start.extend(&keycode::serialize(&VERSION));
		start.extend(&keycode::serialize(&KeyKind::ViewIndexEntry));
		start.extend(&keycode::serialize(&view));

		let mut end = Vec::with_capacity(10);
		end.extend(&keycode::serialize(&VERSION));
		end.extend(&keycode::serialize(&KeyKind::ViewIndexEntry));
		let next_view = ViewId(*view + 1);
		end.extend(&keycode::serialize(&next_view));

		EncodedKeyRange {
			start: Bound::Included(EncodedKey::new(start)),
			end: Bound::Excluded(EncodedKey::new(end)),
		}
	}

	/// Create a range for scanning entries within an index with a specific
	/// key prefix
	pub fn key_prefix_range(
		view: ViewId,
		index: IndexId,
		key_prefix: &[u8],
	) -> EncodedKeyRange {
		let mut start = Vec::with_capacity(18 + key_prefix.len());
		start.extend(&keycode::serialize(&VERSION));
		start.extend(&keycode::serialize(&KeyKind::ViewIndexEntry));
		start.extend(&keycode::serialize(&view));
		start.extend(&keycode::serialize(&index));
		start.extend_from_slice(key_prefix);

		// For the end key, append 0xFF to get all keys with this prefix
		let mut end = start.clone();
		end.push(0xFF);

		EncodedKeyRange {
			start: Bound::Included(EncodedKey::new(start)),
			end: Bound::Excluded(EncodedKey::new(end)),
		}
	}

	/// Create a range for entries from an EncodedIndexKeyRange
	/// This method leverages the EncodedIndexKeyRange type for cleaner
	/// range handling.
	pub fn key_range(
		view: ViewId,
		index: IndexId,
		index_range: EncodedIndexKeyRange,
	) -> EncodedKeyRange {
		// Build the prefix for this view and index
		let mut prefix = Vec::with_capacity(18);
		prefix.extend(&keycode::serialize(&VERSION));
		prefix.extend(&keycode::serialize(&KeyKind::ViewIndexEntry));
		prefix.extend(&keycode::serialize(&view));
		prefix.extend(&keycode::serialize(&index));

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
				let mut bytes = Vec::with_capacity(18);
				bytes.extend(&keycode::serialize(&VERSION));
				bytes.extend(&keycode::serialize(
					&KeyKind::ViewIndexEntry,
				));
				bytes.extend(&keycode::serialize(&view));
				let next_index = IndexId(*index + 1);
				bytes.extend(&keycode::serialize(&next_index));
				Bound::Excluded(EncodedKey::new(bytes))
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
	use super::*;
	use crate::{SortDirection, Type, index::EncodedIndexLayout};

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

		let entry = ViewIndexEntryKey {
			view: ViewId(42),
			index: IndexId(7),
			key: index_key.clone(),
		};

		let encoded = entry.encode();
		let decoded = ViewIndexEntryKey::decode(&encoded).unwrap();

		assert_eq!(decoded.view, ViewId(42));
		assert_eq!(decoded.index, IndexId(7));
		assert_eq!(decoded.key.as_slice(), index_key.as_slice());
	}

	#[test]
	fn test_ordering() {
		let layout = EncodedIndexLayout::new(
			&[Type::Uint8],
			&[SortDirection::Asc],
		)
		.unwrap();

		let mut key1 = layout.allocate_key();
		layout.set_u64(&mut key1, 0, 100u64);

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);

		// Same view and index, different keys
		let entry1 = ViewIndexEntryKey {
			view: ViewId(1),
			index: IndexId(1),
			key: key1,
		};

		let entry2 = ViewIndexEntryKey {
			view: ViewId(1),
			index: IndexId(1),
			key: key2,
		};

		let encoded1 = entry1.encode();
		let encoded2 = entry2.encode();

		// entry1 should come before entry2 because 100 < 200
		assert!(encoded1.as_slice() < encoded2.as_slice());
	}

	#[test]
	fn test_index_range() {
		let range =
			ViewIndexEntryKey::index_range(ViewId(10), IndexId(5));

		// Create entries that should be included
		let layout = EncodedIndexLayout::new(
			&[Type::Uint8],
			&[SortDirection::Asc],
		)
		.unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 50u64);

		let entry = ViewIndexEntryKey {
			view: ViewId(10),
			index: IndexId(5),
			key,
		};

		let encoded = entry.encode();

		// Check that the entry falls within the range
		if let (Bound::Included(start), Bound::Excluded(end)) =
			(&range.start, &range.end)
		{
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		} else {
			panic!("Expected Included/Excluded bounds");
		}

		// Entry with different index should not be in range
		// Note: Due to keycode encoding, IndexId(6) will have a smaller
		// encoded value than IndexId(5) since keycode inverts bits
		// (larger numbers become smaller byte sequences)
		let entry2 = ViewIndexEntryKey {
			view: ViewId(10),
			index: IndexId(6),
			key: layout.allocate_key(),
		};

		let encoded2 = entry2.encode();
		// The entry with IndexId(6) should not be within the range for
		// IndexId(5)
		if let (Bound::Included(start), Bound::Excluded(end)) =
			(&range.start, &range.end)
		{
			// encoded2 should either be < start or >= end
			assert!(encoded2.as_slice() < start.as_slice()
				|| encoded2.as_slice() >= end.as_slice());
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
		let range = ViewIndexEntryKey::key_prefix_range(
			ViewId(1),
			IndexId(1),
			prefix,
		);

		// Now create a full key with the same prefix
		layout.set_row_number(&mut key, 1, 999u64);
		let entry = ViewIndexEntryKey {
			view: ViewId(1),
			index: IndexId(1),
			key: key.clone(),
		};

		let encoded = entry.encode();

		// Should be within range
		if let (Bound::Included(start), Bound::Excluded(end)) =
			(&range.start, &range.end)
		{
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		}

		// Create a key with different prefix
		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64); // Different first field
		layout.set_row_number(&mut key2, 1, 1u64);

		let entry2 = ViewIndexEntryKey {
			view: ViewId(1),
			index: IndexId(1),
			key: key2,
		};

		let encoded2 = entry2.encode();

		// Should not be in range
		if let Bound::Excluded(end) = &range.end {
			assert!(encoded2.as_slice() >= end.as_slice());
		}
	}
}
