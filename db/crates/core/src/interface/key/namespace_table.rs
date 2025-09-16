// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{NamespaceId, TableId},
	util::encoding::keycode::{self, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceTableKey {
	pub namespace: NamespaceId,
	pub table: TableId,
}

const VERSION: u8 = 1;

impl EncodableKey for NamespaceTableKey {
	const KIND: KeyKind = KeyKind::NamespaceTable;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.table);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}

		let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}

		let payload = &key[2..];
		if payload.len() != 16 {
			return None;
		}

		keycode::deserialize(&payload[..8]).ok().zip(keycode::deserialize(&payload[8..]).ok()).map(
			|(namespace, table)| Self {
				namespace,
				table,
			},
		)
	}
}

impl NamespaceTableKey {
	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, NamespaceTableKey};
	use crate::interface::catalog::{NamespaceId, TableId};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceTableKey {
			namespace: NamespaceId(0xABCD),
			table: TableId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xFB, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = NamespaceTableKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
		assert_eq!(key.table, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceTableKey {
			namespace: NamespaceId(1),
			table: TableId(100),
		};
		let key2 = NamespaceTableKey {
			namespace: NamespaceId(1),
			table: TableId(200),
		};
		let key3 = NamespaceTableKey {
			namespace: NamespaceId(2),
			table: TableId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
