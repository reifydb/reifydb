// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use reifydb_type::value::row_number::RowNumber;

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::shape::ShapeId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct RowKey {
	pub shape: ShapeId,
	pub row: RowNumber,
}

impl EncodableKey for RowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19); // 1 + 1 + 9 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_shape_id(self.shape)
			.extend_u64(self.row.0);
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

		let shape = de.read_shape_id().ok()?;
		let row = de.read_row_number().ok()?;

		Some(Self {
			shape,
			row,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowKeyRange {
	pub shape: ShapeId,
}

impl RowKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let shape = de.read_shape_id().ok()?;

		Some(RowKeyRange {
			shape,
		})
	}

	/// Create a range for scanning rows from a shape
	///
	/// If `last_key` is provided, creates a range that continues from after that key.
	/// Otherwise, creates a range that includes all rows for the shape.
	///
	/// The caller is responsible for limiting the number of results returned.
	pub fn scan_range(shape: ShapeId, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let range = RowKeyRange {
			shape,
		};

		if let Some(last_key) = last_key {
			EncodedKeyRange::new(Bound::Excluded(last_key.clone()), Bound::Included(range.end().unwrap()))
		} else {
			EncodedKeyRange::new(
				Bound::Included(range.start().unwrap()),
				Bound::Included(range.end().unwrap()),
			)
		}
	}
}

impl EncodableKeyRange for RowKeyRange {
	const KIND: KeyKind = KeyKind::Row;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(11); // 1 + 1 + 9
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_shape_id(self.shape);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_shape_id(self.shape.prev());
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

impl RowKey {
	pub fn encoded(shape: impl Into<ShapeId>, row: impl Into<RowNumber>) -> EncodedKey {
		Self {
			shape: shape.into(),
			row: row.into(),
		}
		.encode()
	}

	pub fn full_scan(shape: impl Into<ShapeId>) -> EncodedKeyRange {
		let shape = shape.into();
		EncodedKeyRange::start_end(Some(Self::shape_start(shape)), Some(Self::shape_end(shape)))
	}

	pub fn shape_start(shape: impl Into<ShapeId>) -> EncodedKey {
		let shape = shape.into();
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_shape_id(shape);
		serializer.to_encoded_key()
	}

	pub fn shape_end(shape: impl Into<ShapeId>) -> EncodedKey {
		let shape = shape.into();
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_shape_id(shape.prev());
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::row_number::RowNumber;

	use super::{EncodableKey, RowKey};
	use crate::interface::catalog::shape::ShapeId;

	#[test]
	fn test_encode_decode() {
		let key = RowKey {
			shape: ShapeId::table(0xABCD),
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xFC, // kind
			0x01, // ShapeId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = RowKey::decode(&encoded).unwrap();
		assert_eq!(key.shape, ShapeId::table(0xABCD));
		assert_eq!(key.row, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = RowKey {
			shape: ShapeId::table(1),
			row: RowNumber(100),
		};
		let key2 = RowKey {
			shape: ShapeId::table(1),
			row: RowNumber(200),
		};
		let key3 = RowKey {
			shape: ShapeId::table(2),
			row: RowNumber(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
