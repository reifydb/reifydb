// SPDX-License-Identifier: AGPL-3.0-or-later
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
/// Layout without tag: `[Version | Row(0x03) | PrimitiveId::Series(id) | timestamp(i64) | sequence(u64)]`
/// Layout with tag:    `[Version | Row(0x03) | PrimitiveId::Series(id) | variant_tag(u8) | timestamp(i64) |
/// sequence(u64)]`
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesRowKey {
	pub series: SeriesId,
	pub variant_tag: Option<u8>,
	pub timestamp: i64,
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
		serializer.extend_i64(self.timestamp).extend_u64(self.sequence);
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

		let timestamp = de.read_i64().ok()?;
		let sequence = de.read_u64().ok()?;

		Some(Self {
			series,
			variant_tag,
			timestamp,
			sequence,
		})
	}
}

/// Range key for scanning series data rows.
#[derive(Debug, Clone)]
pub struct SeriesRowKeyRange {
	pub series: SeriesId,
	pub variant_tag: Option<u8>,
	pub time_start: Option<i64>,
	pub time_end: Option<i64>,
}

impl SeriesRowKeyRange {
	/// Create a range covering all rows for a series (optionally filtered by tag).
	pub fn full_scan(series: SeriesId, variant_tag: Option<u8>) -> EncodedKeyRange {
		let range = SeriesRowKeyRange {
			series,
			variant_tag,
			time_start: None,
			time_end: None,
		};
		EncodedKeyRange::new(Bound::Included(range.start_key()), Bound::Included(range.end_key()))
	}

	/// Create a range scan with optional time bounds.
	pub fn scan_range(
		series: SeriesId,
		variant_tag: Option<u8>,
		time_start: Option<i64>,
		time_end: Option<i64>,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
		let range = SeriesRowKeyRange {
			series,
			variant_tag,
			time_start,
			time_end,
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
		// Descending key encoding: higher timestamps have lower key values.
		// The start key (lower bound) uses time_end (the highest timestamp in
		// the desired range) to begin scanning from the newest matching row.
		if let Some(ts) = self.time_end {
			serializer.extend_i64(ts);
		}
		serializer.to_encoded_key()
	}

	fn end_key(&self) -> EncodedKey {
		// Descending key encoding: lower timestamps have higher key values.
		// The end key (upper bound) uses time_start (the lowest timestamp in
		// the desired range) to stop scanning after the oldest matching row.
		if let Some(ts) = self.time_start {
			let primitive = PrimitiveId::Series(self.series);
			let mut serializer = KeySerializer::with_capacity(28);
			serializer.extend_u8(VERSION).extend_u8(KeyKind::Row as u8).extend_primitive_id(primitive);
			if let Some(tag) = self.variant_tag {
				serializer.extend_u8(tag);
			}
			// Use sequence 0 which encodes to max bytes in descending encoding,
			// ensuring all rows at this timestamp are included.
			serializer.extend_i64(ts).extend_u64(0u64);
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
			timestamp: 1706745600000,
			sequence: 1,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.series, SeriesId(42));
		assert_eq!(decoded.variant_tag, None);
		assert_eq!(decoded.timestamp, 1706745600000);
		assert_eq!(decoded.sequence, 1);
	}

	#[test]
	fn test_encode_decode_with_tag() {
		let key = SeriesRowKey {
			series: SeriesId(42),
			variant_tag: Some(3),
			timestamp: 1706745600000,
			sequence: 5,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.series, SeriesId(42));
		assert_eq!(decoded.variant_tag, Some(3));
		assert_eq!(decoded.timestamp, 1706745600000);
		assert_eq!(decoded.sequence, 5);
	}

	#[test]
	fn test_ordering_by_timestamp() {
		let key1 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			timestamp: 100,
			sequence: 0,
		};
		let key2 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			timestamp: 200,
			sequence: 0,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();
		// Keycode encoding uses NOT of big-endian, producing descending order
		// Earlier timestamps (smaller values) sort AFTER later timestamps
		assert!(e1 > e2, "timestamp descending ordering not preserved");
	}

	#[test]
	fn test_ordering_by_sequence() {
		let key1 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			timestamp: 100,
			sequence: 1,
		};
		let key2 = SeriesRowKey {
			series: SeriesId(1),
			variant_tag: None,
			timestamp: 100,
			sequence: 2,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();
		// Keycode encoding uses NOT of big-endian, producing descending order
		assert!(e1 > e2, "sequence descending ordering not preserved");
	}
}
