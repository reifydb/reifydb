// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{id::SeriesId, primitive::PrimitiveId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

/// Key for series data rows.
///
/// Layout without tag: `[Version | Row(0x03) | PrimitiveId::Series(id) | ordering_value(i64) | sequence(u64)]`
/// Layout with tag:    `[Version | Row(0x03) | PrimitiveId::Series(id) | variant_tag(u8) | ordering_value(i64) |
/// sequence(u64)]`
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesRowKey {
	pub series: SeriesId,
	pub variant_tag: Option<u8>,
	pub key: i64,
	pub sequence: u64,
}

impl EncodableKey for SeriesRowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let primitive = PrimitiveId::Series(self.series);
		let capacity = if self.variant_tag.is_some() {
			28
		} else {
			27
		};
		let mut serializer = KeySerializer::with_capacity(capacity);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_primitive_id(primitive);
		if let Some(tag) = self.variant_tag {
			serializer.extend_u8(tag);
		}
		serializer.extend_i64(self.key).extend_u64(self.sequence);
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

		let primitive = de.read_primitive_id().ok()?;
		let series = match primitive {
			PrimitiveId::Series(id) => id,
			_ => return None,
		};

		// We need to know if there's a variant tag. We can tell by the remaining bytes:
		// Without tag: i64(8) + u64(8) = 16 bytes remain
		// With tag: u8(1) + i64(8) + u64(8) = 17 bytes remain
		let remaining = de.remaining();
		let variant_tag = if remaining > 16 {
			Some(de.read_u8().ok()?)
		} else {
			None
		};

		let key = de.read_i64().ok()?;
		let sequence = de.read_u64().ok()?;

		Some(Self {
			series,
			variant_tag,
			key,
			sequence,
		})
	}
}

/// Range key for scanning series data rows.
#[derive(Debug, Clone)]
pub struct SeriesRowKeyRange {
	pub series: SeriesId,
	pub variant_tag: Option<u8>,
	pub key_start: Option<i64>,
	pub key_end: Option<i64>,
}

impl SeriesRowKeyRange {
	/// Create a range covering all rows for a series (optionally filtered by tag).
	pub fn full_scan(series: SeriesId, variant_tag: Option<u8>) -> EncodedKeyRange {
		let range = SeriesRowKeyRange {
			series,
			variant_tag,
			key_start: None,
			key_end: None,
		};
		EncodedKeyRange::new(Bound::Included(range.start_key()), Bound::Included(range.end_key()))
	}

	/// Create a range scan with optional key bounds.
	pub fn scan_range(
		series: SeriesId,
		variant_tag: Option<u8>,
		key_start: Option<i64>,
		key_end: Option<i64>,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
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
		let primitive = PrimitiveId::Series(self.series);
		let mut serializer = KeySerializer::with_capacity(28);
		serializer.extend_u8(VERSION).extend_u8(KeyKind::Row as u8).extend_primitive_id(primitive);
		if let Some(tag) = self.variant_tag {
			serializer.extend_u8(tag);
		}
		// Descending key encoding: higher key values have lower encoded values.
		// The start key (lower bound) uses key_end (the highest key value in
		// the desired range) to begin scanning from the newest matching row.
		if let Some(key_val) = self.key_end {
			serializer.extend_i64(key_val);
		}
		serializer.to_encoded_key()
	}

	fn end_key(&self) -> EncodedKey {
		// Descending key encoding: lower key values have higher encoded values.
		// The end key (upper bound) uses key_start (the lowest key value in
		// the desired range) to stop scanning after the oldest matching row.
		if let Some(key_val) = self.key_start {
			let primitive = PrimitiveId::Series(self.series);
			let mut serializer = KeySerializer::with_capacity(28);
			serializer.extend_u8(VERSION).extend_u8(KeyKind::Row as u8).extend_primitive_id(primitive);
			if let Some(tag) = self.variant_tag {
				serializer.extend_u8(tag);
			}
			// Use sequence 0 which encodes to max bytes in descending encoding,
			// ensuring all rows at this key value are included.
			serializer.extend_i64(key_val).extend_u64(0u64);
			serializer.to_encoded_key()
		} else {
			// Use PrimitiveId ordering trick to get end of range
			let primitive = PrimitiveId::Series(self.series);
			let mut serializer = KeySerializer::with_capacity(11);
			serializer
				.extend_u8(VERSION)
				.extend_u8(KeyKind::Row as u8)
				.extend_primitive_id(primitive.prev());
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
