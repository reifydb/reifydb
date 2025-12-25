// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use reifydb_type::RowNumber;

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{EncodableKeyRange, catalog::PrimitiveId},
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct RowKey {
	pub primitive: PrimitiveId,
	pub row: RowNumber,
}

impl EncodableKey for RowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19); // 1 + 1 + 9 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_primitive_id(self.primitive)
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

		let primitive = de.read_primitive_id().ok()?;
		let row = de.read_row_number().ok()?;

		Some(Self {
			primitive,
			row,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowKeyRange {
	pub primitive: PrimitiveId,
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

		let primitive = de.read_primitive_id().ok()?;

		Some(RowKeyRange {
			primitive,
		})
	}

	/// Create a range for scanning rows from a primitive
	///
	/// If `last_key` is provided, creates a range that continues from after that key.
	/// Otherwise, creates a range that includes all rows for the primitive.
	///
	/// The caller is responsible for limiting the number of results returned.
	pub fn scan_range(primitive: PrimitiveId, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let range = RowKeyRange {
			primitive,
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
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_primitive_id(self.primitive);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_primitive_id(self.primitive.prev());
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
	pub fn encoded(primitive: impl Into<PrimitiveId>, row: impl Into<RowNumber>) -> EncodedKey {
		Self {
			primitive: primitive.into(),
			row: row.into(),
		}
		.encode()
	}

	pub fn full_scan(primitive: impl Into<PrimitiveId>) -> EncodedKeyRange {
		let primitive = primitive.into();
		EncodedKeyRange::start_end(Some(Self::primitive_start(primitive)), Some(Self::primitive_end(primitive)))
	}

	pub fn primitive_start(primitive: impl Into<PrimitiveId>) -> EncodedKey {
		let primitive = primitive.into();
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_primitive_id(primitive);
		serializer.to_encoded_key()
	}

	pub fn primitive_end(primitive: impl Into<PrimitiveId>) -> EncodedKey {
		let primitive = primitive.into();
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_primitive_id(primitive.prev());
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::RowNumber;

	use super::{EncodableKey, RowKey};
	use crate::interface::catalog::PrimitiveId;

	#[test]
	fn test_encode_decode() {
		let key = RowKey {
			primitive: PrimitiveId::table(0xABCD),
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xFC, // kind
			0x01, // PrimitiveId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = RowKey::decode(&encoded).unwrap();
		assert_eq!(key.primitive, PrimitiveId::table(0xABCD));
		assert_eq!(key.row, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = RowKey {
			primitive: PrimitiveId::table(1),
			row: RowNumber(100),
		};
		let key2 = RowKey {
			primitive: PrimitiveId::table(1),
			row: RowNumber(200),
		};
		let key3 = RowKey {
			primitive: PrimitiveId::table(2),
			row: RowNumber(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
