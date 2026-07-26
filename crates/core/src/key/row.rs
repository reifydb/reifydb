// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::row_number::RowNumber;

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	interface::catalog::object::ObjectId,
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct RowKey {
	pub object: ObjectId,
	pub row: RowNumber,
}

impl EncodableKey for RowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object).extend_u64(self.row.0);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_object_id().ok()?;
		let row = de.read_row_number().ok()?;

		Some(Self {
			object,
			row,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowKeyRange {
	pub object: ObjectId,
}

impl RowKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_object_id().ok()?;

		Some(RowKeyRange {
			object,
		})
	}

	pub fn scan_range(object: ObjectId, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let range = RowKeyRange {
			object,
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
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object.prev());
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
	pub fn encoded(object: impl Into<ObjectId>, row: impl Into<RowNumber>) -> EncodedKey {
		Self {
			object: object.into(),
			row: row.into(),
		}
		.encode()
	}

	pub fn full_scan(object: impl Into<ObjectId>) -> EncodedKeyRange {
		let object = object.into();
		EncodedKeyRange::start_end(Some(Self::object_start(object)), Some(Self::object_end(object)))
	}

	pub fn object_start(object: impl Into<ObjectId>) -> EncodedKey {
		let object = object.into();
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(object);
		serializer.to_encoded_key()
	}

	pub fn object_end(object: impl Into<ObjectId>) -> EncodedKey {
		let object = object.into();
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(object.prev());
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::row_number::RowNumber;

	use super::{EncodableKey, RowKey};
	use crate::interface::catalog::object::ObjectId;

	#[test]
	fn test_encode_decode() {
		let key = RowKey {
			object: ObjectId::table(0xABCD),
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> =
			vec![0xFC, 0x01, 0x3F, 0x54, 0x32, 0x00, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F];

		assert_eq!(encoded.as_slice(), expected);

		let key = RowKey::decode(&encoded).unwrap();
		assert_eq!(key.object, ObjectId::table(0xABCD));
		assert_eq!(key.row, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = RowKey {
			object: ObjectId::table(1),
			row: RowNumber(100),
		};
		let key2 = RowKey {
			object: ObjectId::table(1),
			row: RowNumber(200),
		};
		let key3 = RowKey {
			object: ObjectId::table(2),
			row: RowNumber(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
