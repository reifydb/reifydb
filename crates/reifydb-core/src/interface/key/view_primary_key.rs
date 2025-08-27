// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	index::EncodedIndexKey,
	interface::ViewId,
	util::{CowVec, encoding::keycode},
};

const VERSION: u8 = 1;

/// Key for storing view primary key entries with the encoded index key data
#[derive(Debug, Clone, PartialEq)]
pub struct ViewPrimaryKeyEntry {
	pub view: ViewId,
	pub key: EncodedIndexKey,
}

impl ViewPrimaryKeyEntry {
	pub fn new(view: ViewId, key: EncodedIndexKey) -> Self {
		Self {
			view,
			key,
		}
	}
}

impl EncodableKey for ViewPrimaryKeyEntry {
	const KIND: KeyKind = KeyKind::ViewPrimaryKey;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(10 + self.key.len());
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.view));
		// Append the raw index key bytes
		out.extend_from_slice(self.key.as_slice());

		EncodedKey::new(out)
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 10 {
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

		let view: ViewId = keycode::deserialize(&payload[..8]).ok()?;

		// The remaining bytes are the index key
		if payload.len() > 8 {
			let index_key_bytes = &payload[8..];
			let index_key = EncodedIndexKey(CowVec::new(
				index_key_bytes.to_vec(),
			));
			Some(Self {
				view,
				key: index_key,
			})
		} else {
			None
		}
	}
}

/// Range for scanning view primary key entries
#[derive(Debug, Clone, PartialEq)]
pub struct ViewPrimaryKeyRange {
	pub view: ViewId,
}

impl EncodableKeyRange for ViewPrimaryKeyRange {
	const KIND: KeyKind = KeyKind::ViewPrimaryKey;

	fn start(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.view));
		Some(EncodedKey::new(out))
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&(*self.view - 1)));
		Some(EncodedKey::new(out))
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
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

impl ViewPrimaryKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
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

		let view: ViewId = keycode::deserialize(&payload[..8]).ok()?;
		Some(ViewPrimaryKeyRange {
			view,
		})
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

		let entry = ViewPrimaryKeyEntry {
			view: ViewId(42),
			key: index_key.clone(),
		};

		let encoded = entry.encode();
		let decoded = ViewPrimaryKeyEntry::decode(&encoded).unwrap();

		assert_eq!(decoded.view, ViewId(42));
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

		// Same view, different keys
		let entry1 = ViewPrimaryKeyEntry {
			view: ViewId(1),
			key: key1,
		};

		let entry2 = ViewPrimaryKeyEntry {
			view: ViewId(1),
			key: key2,
		};

		let encoded1 = entry1.encode();
		let encoded2 = entry2.encode();

		// entry1 should come before entry2 because 100 < 200
		assert!(encoded1.as_slice() < encoded2.as_slice());
	}

	#[test]
	fn test_view_range() {
		let range_key = ViewPrimaryKeyRange {
			view: ViewId(10),
		};

		// Get the start and end keys
		let start = range_key.start().unwrap();
		let end = range_key.end().unwrap();

		// Create entries that should be included
		let layout = EncodedIndexLayout::new(
			&[Type::Uint8],
			&[SortDirection::Asc],
		)
		.unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 50u64);

		let entry = ViewPrimaryKeyEntry {
			view: ViewId(10),
			key,
		};

		let encoded = entry.encode();

		// Check that the entry falls within the range
		assert!(encoded.as_slice() >= start.as_slice());
		assert!(encoded.as_slice() < end.as_slice());

		// Entry with different view should not be in range
		let entry2 = ViewPrimaryKeyEntry {
			view: ViewId(11),
			key: layout.allocate_key(),
		};

		let encoded2 = entry2.encode();
		assert!(encoded2.as_slice() < start.as_slice());
	}

	#[test]
	fn test_range_decode() {
		let range = ViewPrimaryKeyRange {
			view: ViewId(42),
		};

		let start = range.start().unwrap();
		let end = range.end().unwrap();

		let encoded_range = EncodedKeyRange {
			start: Bound::Included(start),
			end: Bound::Excluded(end),
		};

		let (decoded_start, decoded_end) =
			ViewPrimaryKeyRange::decode(&encoded_range);

		assert!(decoded_start.is_some());
		assert_eq!(decoded_start.unwrap().view, ViewId(42));

		assert!(decoded_end.is_some());
		assert_eq!(decoded_end.unwrap().view, ViewId(41));
	}
}
