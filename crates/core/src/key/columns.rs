// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::ColumnId;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnsKey {
	pub column: ColumnId,
}

impl EncodableKey for ColumnsKey {
	const KIND: KeyKind = KeyKind::Columns;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.column);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let column = de.read_u64().ok()?;

		Some(Self {
			column: ColumnId(column),
		})
	}
}

impl ColumnsKey {
	pub fn encoded(column: impl Into<ColumnId>) -> EncodedKey {
		Self {
			column: column.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::column_start()), Some(Self::column_end()))
	}

	fn column_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn column_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{ColumnsKey, EncodableKey};
	use crate::interface::catalog::id::ColumnId;

	#[test]
	fn test_encode_decode() {
		let key = ColumnsKey {
			column: ColumnId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF9, 0x3F, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnsKey::decode(&encoded).unwrap();
		assert_eq!(key.column, 0xABCD);
	}
}
