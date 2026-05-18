// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::Bound, iter};

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	value::index::encoded::EncodedIndexKey,
};

#[derive(Clone, Debug)]
pub struct EncodedIndexKeyRange {
	pub start: Bound<EncodedIndexKey>,
	pub end: Bound<EncodedIndexKey>,
}

impl EncodedIndexKeyRange {
	pub fn new(start: Bound<EncodedIndexKey>, end: Bound<EncodedIndexKey>) -> Self {
		Self {
			start,
			end,
		}
	}

	pub fn start_end(start: Option<EncodedIndexKey>, end: Option<EncodedIndexKey>) -> Self {
		let start = match start {
			Some(s) => Bound::Included(s),
			None => Bound::Unbounded,
		};

		let end = match end {
			Some(e) => Bound::Excluded(e),
			None => Bound::Unbounded,
		};

		Self {
			start,
			end,
		}
	}

	pub fn start_end_inclusive(start: Option<EncodedIndexKey>, end: Option<EncodedIndexKey>) -> Self {
		let start = match start {
			Some(s) => Bound::Included(s),
			None => Bound::Unbounded,
		};

		let end = match end {
			Some(e) => Bound::Included(e),
			None => Bound::Unbounded,
		};

		Self {
			start,
			end,
		}
	}

	pub fn prefix(prefix: &[u8]) -> Self {
		let start = Bound::Included(EncodedIndexKey::from_bytes(prefix));
		let end = match prefix.iter().rposition(|&b| b != 0xff) {
			Some(i) => Bound::Excluded(EncodedIndexKey::from_bytes(
				&prefix.iter().take(i).copied().chain(iter::once(prefix[i] + 1)).collect::<Vec<_>>(),
			)),
			None => Bound::Unbounded,
		};
		Self {
			start,
			end,
		}
	}

	pub fn all() -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}

	pub fn to_encoded_key_range(&self) -> EncodedKeyRange {
		let start = match &self.start {
			Bound::Included(key) => Bound::Included(EncodedKey::new(key.as_slice())),
			Bound::Excluded(key) => Bound::Excluded(EncodedKey::new(key.as_slice())),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match &self.end {
			Bound::Included(key) => Bound::Included(EncodedKey::new(key.as_slice())),
			Bound::Excluded(key) => Bound::Excluded(EncodedKey::new(key.as_slice())),
			Bound::Unbounded => Bound::Unbounded,
		};

		EncodedKeyRange::new(start, end)
	}

	pub fn from_prefix(key: &EncodedIndexKey) -> Self {
		Self::prefix(key.as_slice())
	}
}

impl From<EncodedIndexKeyRange> for EncodedKeyRange {
	fn from(range: EncodedIndexKeyRange) -> Self {
		range.to_encoded_key_range()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::{sort::SortDirection, value::index::shape::IndexShape};

	#[test]
	fn test_start_end() {
		let layout = IndexShape::new(&[Type::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key1 = layout.allocate_key();
		layout.set_u64(&mut key1, 0, 100u64);

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);

		let range = EncodedIndexKeyRange::start_end(Some(key1.clone()), Some(key2.clone()));

		match &range.start {
			Bound::Included(k) => {
				assert_eq!(k.as_slice(), key1.as_slice())
			}
			_ => panic!("Expected Included start bound"),
		}

		match &range.end {
			Bound::Excluded(k) => {
				assert_eq!(k.as_slice(), key2.as_slice())
			}
			_ => panic!("Expected Excluded end bound"),
		}
	}

	#[test]
	fn test_start_end_inclusive() {
		let layout = IndexShape::new(&[Type::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key1 = layout.allocate_key();
		layout.set_u64(&mut key1, 0, 100u64);

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);

		let range = EncodedIndexKeyRange::start_end_inclusive(Some(key1.clone()), Some(key2.clone()));

		match &range.start {
			Bound::Included(k) => {
				assert_eq!(k.as_slice(), key1.as_slice())
			}
			_ => panic!("Expected Included start bound"),
		}

		match &range.end {
			Bound::Included(k) => {
				assert_eq!(k.as_slice(), key2.as_slice())
			}
			_ => panic!("Expected Included end bound"),
		}
	}

	#[test]
	fn test_unbounded() {
		let range = EncodedIndexKeyRange::start_end(None, None);
		assert!(matches!(range.start, Bound::Unbounded));
		assert!(matches!(range.end, Bound::Unbounded));
	}

	#[test]
	fn test_prefix() {
		let prefix = &[0x12, 0x34];
		let range = EncodedIndexKeyRange::prefix(prefix);

		match &range.start {
			Bound::Included(k) => assert_eq!(k.as_slice(), prefix),
			_ => panic!("Expected Included start bound"),
		}

		match &range.end {
			Bound::Excluded(k) => {
				assert_eq!(k.as_slice(), &[0x12, 0x35])
			}
			_ => panic!("Expected Excluded end bound"),
		}
	}

	#[test]
	fn test_prefix_with_ff() {
		let prefix = &[0x12, 0xff];
		let range = EncodedIndexKeyRange::prefix(prefix);

		match &range.start {
			Bound::Included(k) => assert_eq!(k.as_slice(), prefix),
			_ => panic!("Expected Included start bound"),
		}

		match &range.end {
			Bound::Excluded(k) => assert_eq!(k.as_slice(), &[0x13]),
			_ => panic!("Expected Excluded end bound"),
		}
	}

	#[test]
	fn test_prefix_all_ff() {
		let prefix = &[0xff, 0xff];
		let range = EncodedIndexKeyRange::prefix(prefix);

		match &range.start {
			Bound::Included(k) => assert_eq!(k.as_slice(), prefix),
			_ => panic!("Expected Included start bound"),
		}

		assert!(matches!(range.end, Bound::Unbounded));
	}

	#[test]
	fn test_to_encoded_key_range() {
		let layout = IndexShape::new(&[Type::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 100u64);

		let index_range = EncodedIndexKeyRange::start_end(Some(key.clone()), None);
		let key_range = index_range.to_encoded_key_range();

		match &key_range.start {
			Bound::Included(k) => {
				assert_eq!(k.as_slice(), key.as_slice())
			}
			_ => panic!("Expected Included start bound"),
		}

		assert!(matches!(key_range.end, Bound::Unbounded));
	}

	#[test]
	fn test_all() {
		let range = EncodedIndexKeyRange::all();
		assert!(matches!(range.start, Bound::Unbounded));
		assert!(matches!(range.end, Bound::Unbounded));
	}
}
