// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{id::SeriesId, shape::ShapeId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesRowKey {
	pub series: SeriesId,
	pub variant_tag: Option<u8>,
	pub key: u64,
	pub sequence: u64,
}

impl EncodableKey for SeriesRowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let object = ShapeId::Series(self.series);
		let capacity = if self.variant_tag.is_some() {
			28
		} else {
			27
		};
		let mut serializer = KeySerializer::with_capacity(capacity);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_shape_id(object);
		if let Some(tag) = self.variant_tag {
			serializer.extend_u8(tag);
		}
		serializer.extend_u64(self.key).extend_u64(self.sequence);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_shape_id().ok()?;
		let series = match object {
			ShapeId::Series(id) => id,
			_ => return None,
		};

		let remaining = de.remaining();
		let variant_tag = if remaining > 16 {
			Some(de.read_u8().ok()?)
		} else {
			None
		};

		let key = de.read_u64().ok()?;
		let sequence = de.read_u64().ok()?;

		Some(Self {
			series,
			variant_tag,
			key,
			sequence,
		})
	}
}

#[derive(Debug, Clone)]
pub struct SeriesRowKeyRange {
	pub series: SeriesId,
	pub variant_tag: Option<u8>,
	pub key_start: Option<u64>,
	pub key_end: Option<u64>,
}

impl SeriesRowKeyRange {
	pub fn full_scan(series: SeriesId, variant_tag: Option<u8>) -> EncodedKeyRange {
		let range = SeriesRowKeyRange {
			series,
			variant_tag,
			key_start: None,
			key_end: None,
		};
		EncodedKeyRange::new(Bound::Included(range.start_key()), Bound::Included(range.end_key()))
	}

	pub fn scan_range(
		series: SeriesId,
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
			series,
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

	fn start_key(&self) -> EncodedKey {
		let object = ShapeId::Series(self.series);
		let mut serializer = KeySerializer::with_capacity(28);
		serializer.extend_u8(VERSION).extend_u8(KeyKind::Row as u8).extend_shape_id(object);
		if let Some(tag) = self.variant_tag {
			serializer.extend_u8(tag);
		}

		if let Some(key_val) = self.key_end {
			serializer.extend_u64(key_val - 1);
		}
		serializer.to_encoded_key()
	}

	fn end_key(&self) -> EncodedKey {
		if let Some(key_val) = self.key_start {
			let object = ShapeId::Series(self.series);
			let mut serializer = KeySerializer::with_capacity(28);
			serializer.extend_u8(VERSION).extend_u8(KeyKind::Row as u8).extend_shape_id(object);
			if let Some(tag) = self.variant_tag {
				serializer.extend_u8(tag);
			}

			serializer.extend_u64(key_val).extend_u64(0u64);
			serializer.to_encoded_key()
		} else {
			let object = ShapeId::Series(self.series);
			let mut serializer = KeySerializer::with_capacity(11);
			serializer.extend_u8(VERSION).extend_u8(KeyKind::Row as u8).extend_shape_id(object.prev());
			serializer.to_encoded_key()
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_without_tag() {
		let key = SeriesRowKey {
			series: SeriesId(42),
			variant_tag: None,
			key: 1706745600000,
			sequence: 1,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.series, SeriesId(42));
		assert_eq!(decoded.variant_tag, None);
		assert_eq!(decoded.key, 1706745600000);
		assert_eq!(decoded.sequence, 1);
	}

	#[test]
	fn test_encode_decode_with_tag() {
		let key = SeriesRowKey {
			series: SeriesId(42),
			variant_tag: Some(3),
			key: 1706745600000,
			sequence: 5,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.series, SeriesId(42));
		assert_eq!(decoded.variant_tag, Some(3));
		assert_eq!(decoded.key, 1706745600000);
		assert_eq!(decoded.sequence, 5);
	}

	#[test]
	fn test_ordering_by_key() {
		let key1 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			key: 100,
			sequence: 0,
		};
		let key2 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			key: 200,
			sequence: 0,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();
		// Keycode encoding uses NOT of big-endian, producing descending order
		// Smaller key values sort AFTER larger key values
		assert!(e1 > e2, "key descending ordering not preserved");
	}

	#[test]
	fn test_ordering_by_sequence() {
		let key1 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			key: 100,
			sequence: 1,
		};
		let key2 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			key: 100,
			sequence: 2,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();
		// Keycode encoding uses NOT of big-endian, producing descending order
		assert!(e1 > e2, "sequence descending ordering not preserved");
	}
}
