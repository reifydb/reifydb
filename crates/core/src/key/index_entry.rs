// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{id::IndexId, shape::ShapeId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
	value::index::{encoded::EncodedIndexKey, range::EncodedIndexKeyRange},
};

#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntryKey {
	pub shape: ShapeId,
	pub index: IndexId,
	pub key: EncodedIndexKey,
}

impl IndexEntryKey {
	pub fn new(shape: impl Into<ShapeId>, index: IndexId, key: EncodedIndexKey) -> Self {
		Self {
			shape: shape.into(),
			index,
			key,
		}
	}

	pub fn encoded(shape: impl Into<ShapeId>, index: IndexId, key: EncodedIndexKey) -> EncodedKey {
		Self::new(shape, index, key).encode()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntryKeyRange {
	pub shape: ShapeId,
	pub index: IndexId,
}

impl IndexEntryKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let shape = de.read_shape_id().ok()?;
		let index = de.read_index_id().ok()?;

		Some(IndexEntryKeyRange {
			shape,
			index,
		})
	}
}

impl EncodableKeyRange for IndexEntryKeyRange {
	const KIND: KeyKind = KeyKind::IndexEntry;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer.extend_u8(Self::KIND as u8).extend_shape_id(self.shape).extend_index_id(self.index);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer.extend_u8(Self::KIND as u8).extend_shape_id(self.shape).extend_index_id(self.index.prev());
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
			.extend_u8(Self::KIND as u8)
			.extend_shape_id(self.shape)
			.extend_index_id(self.index)
			.extend_raw(self.key.as_slice());
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let shape = de.read_shape_id().ok()?;
		let index = de.read_index_id().ok()?;

		let remaining = de.remaining();
		if remaining > 0 {
			let remaining_bytes = de.read_raw(remaining).ok()?;
			let index_key = EncodedIndexKey::new(remaining_bytes.to_vec());
			Some(Self {
				shape,
				index,
				key: index_key,
			})
		} else {
			None
		}
	}
}

impl IndexEntryKey {
	pub fn index_range(shape: impl Into<ShapeId>, index: IndexId) -> EncodedKeyRange {
		let range = IndexEntryKeyRange {
			shape: shape.into(),
			index,
		};
		EncodedKeyRange::new(Bound::Included(range.start().unwrap()), Bound::Excluded(range.end().unwrap()))
	}

	pub fn shape_range(shape: impl Into<ShapeId>) -> EncodedKeyRange {
		let shape = shape.into();
		let mut start_serializer = KeySerializer::with_capacity(10);
		start_serializer.extend_u8(KeyKind::IndexEntry as u8).extend_shape_id(shape);

		let next_primitive = shape.next();
		let mut end_serializer = KeySerializer::with_capacity(10);
		end_serializer.extend_u8(KeyKind::IndexEntry as u8).extend_shape_id(next_primitive);

		EncodedKeyRange {
			start: Bound::Included(start_serializer.to_encoded_key()),
			end: Bound::Excluded(end_serializer.to_encoded_key()),
		}
	}

	pub fn key_prefix_range(shape: impl Into<ShapeId>, index: IndexId, key_prefix: &[u8]) -> EncodedKeyRange {
		let shape = shape.into();
		let mut serializer = KeySerializer::with_capacity(20 + key_prefix.len());
		serializer
			.extend_u8(KeyKind::IndexEntry as u8)
			.extend_shape_id(shape)
			.extend_index_id(index)
			.extend_raw(key_prefix);
		let start = serializer.to_encoded_key();

		let mut end = start.as_slice().to_vec();
		end.push(0xFF);

		EncodedKeyRange {
			start: Bound::Included(start),
			end: Bound::Excluded(EncodedKey::new(end)),
		}
	}

	pub fn key_range(
		shape: impl Into<ShapeId>,
		index: IndexId,
		index_range: EncodedIndexKeyRange,
	) -> EncodedKeyRange {
		let shape = shape.into();

		let mut prefix_serializer = KeySerializer::with_capacity(19);
		prefix_serializer.extend_u8(KeyKind::IndexEntry as u8).extend_shape_id(shape).extend_index_id(index);
		let prefix = prefix_serializer.to_encoded_key().to_vec();

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
			Bound::Unbounded => Bound::Included(EncodedKey::new(prefix.clone())),
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
				let mut serializer = KeySerializer::with_capacity(19);
				serializer
					.extend_u8(KeyKind::IndexEntry as u8)
					.extend_shape_id(shape)
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
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::{sort::SortDirection, value::index::shape::IndexShape};

	#[test]
	fn test_encode_decode() {
		let layout = IndexShape::new(&[Type::Uint8, Type::Uint8], &[SortDirection::Asc, SortDirection::Asc])
			.unwrap();

		let mut index_key = layout.allocate_key();
		layout.set_u64(&mut index_key, 0, 100u64);
		layout.set_row_number(&mut index_key, 1, 1u64);

		let entry = IndexEntryKey {
			shape: ShapeId::table(42),
			index: IndexId::primary(7),
			key: index_key.clone(),
		};

		let encoded = entry.encode();
		let decoded = IndexEntryKey::decode(&encoded).unwrap();

		assert_eq!(decoded.shape, ShapeId::table(42));
		assert_eq!(decoded.index, IndexId::primary(7));
		assert_eq!(decoded.key.as_slice(), index_key.as_slice());
	}

	#[test]
	fn test_ordering() {
		let layout = IndexShape::new(&[Type::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key1 = layout.allocate_key();
		layout.set_u64(&mut key1, 0, 100u64);

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);

		let entry1 = IndexEntryKey {
			shape: ShapeId::table(1),
			index: IndexId::primary(1),
			key: key1,
		};

		let entry2 = IndexEntryKey {
			shape: ShapeId::table(1),
			index: IndexId::primary(1),
			key: key2,
		};

		let encoded1 = entry1.encode();
		let encoded2 = entry2.encode();

		assert!(encoded1.as_slice() < encoded2.as_slice());
	}

	#[test]
	fn test_index_range() {
		let range = IndexEntryKey::index_range(ShapeId::table(10), IndexId::primary(5));

		let layout = IndexShape::new(&[Type::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 50u64);

		let entry = IndexEntryKey {
			shape: ShapeId::table(10),
			index: IndexId::primary(5),
			key,
		};

		let encoded = entry.encode();

		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		} else {
			panic!("Expected Included/Excluded bounds");
		}

		let entry2 = IndexEntryKey {
			shape: ShapeId::table(10),
			index: IndexId::primary(6),
			key: layout.allocate_key(),
		};

		let encoded2 = entry2.encode();

		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded2.as_slice() < start.as_slice() || encoded2.as_slice() >= end.as_slice());
		}
	}

	#[test]
	fn test_key_prefix_range() {
		let layout = IndexShape::new(&[Type::Uint8, Type::Uint8], &[SortDirection::Asc, SortDirection::Asc])
			.unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 100u64);
		layout.set_row_number(&mut key, 1, 0u64);

		let prefix = &key.as_slice()[..layout.fields[1].offset];
		let range = IndexEntryKey::key_prefix_range(ShapeId::table(1), IndexId::primary(1), prefix);

		layout.set_row_number(&mut key, 1, 999u64);
		let entry = IndexEntryKey {
			shape: ShapeId::table(1),
			index: IndexId::primary(1),
			key: key.clone(),
		};

		let encoded = entry.encode();

		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		}

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);
		layout.set_row_number(&mut key2, 1, 1u64);

		let entry2 = IndexEntryKey {
			shape: ShapeId::table(1),
			index: IndexId::primary(1),
			key: key2,
		};

		let encoded2 = entry2.encode();

		if let Bound::Excluded(end) = &range.end {
			assert!(encoded2.as_slice() >= end.as_slice());
		}
	}
}
