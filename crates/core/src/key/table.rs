// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::TableId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct TableKey {
	pub table: TableId,
}

const VERSION: u8 = 1;

impl EncodableKey for TableKey {
	const KIND: KeyKind = KeyKind::Table;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.table);
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

		let table = de.read_u64().ok()?;

		Some(Self {
			table: TableId(table),
		})
	}
}

impl TableKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::table_start()), Some(Self::table_end()))
	}

	fn table_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn table_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, TableKey};
	use crate::interface::catalog::TableId;

	#[test]
	fn test_encode_decode() {
		let key = TableKey {
			table: TableId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![
			0xFE, // version
			0xFD, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32,
		];
		assert_eq!(encoded.as_slice(), expected);

		let key = TableKey::decode(&encoded).unwrap();
		assert_eq!(key.table, 0xABCD);
	}
}
