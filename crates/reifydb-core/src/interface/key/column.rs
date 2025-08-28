// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{ColumnId, EncodableKey, KeyKind, StoreId},
	util::encoding::keycode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnKey {
	pub store: StoreId,
	pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnKey {
	const KIND: KeyKind = KeyKind::Column;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(19); // 1 + 1 + 9 + 8
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&self.store));
		out.extend(&keycode::serialize(&self.column));
		EncodedKey::new(out)
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
		if payload.len() != 17 {
			// 9 bytes for store + 8 bytes for column
			return None;
		}

		let store =
			keycode::deserialize_store_id(&payload[..9]).ok()?;
		let column: ColumnId =
			keycode::deserialize(&payload[9..]).ok()?;

		Some(Self {
			store,
			column,
		})
	}
}

impl ColumnKey {
	pub fn full_scan(store: impl Into<StoreId>) -> EncodedKeyRange {
		let store = store.into();
		EncodedKeyRange::start_end(
			Some(Self::start(store)),
			Some(Self::end(store)),
		)
	}

	fn start(store: StoreId) -> EncodedKey {
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&store));
		EncodedKey::new(out)
	}

	fn end(store: StoreId) -> EncodedKey {
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&store.prev()));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use super::EncodableKey;
	use crate::interface::{
		ColumnKey,
		catalog::{ColumnId, StoreId},
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnKey {
			store: StoreId::table(0xABCD),
			column: ColumnId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xF8, // kind
			0x01, // StoreId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54,
			0x32, // store id bytes
			0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21,
			0x0F, // column id bytes
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnKey::decode(&encoded).unwrap();
		assert_eq!(key.store, 0xABCD);
		assert_eq!(key.column, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ColumnKey {
			store: StoreId::table(1),
			column: ColumnId(100),
		};
		let key2 = ColumnKey {
			store: StoreId::table(1),
			column: ColumnId(200),
		};
		let key3 = ColumnKey {
			store: StoreId::table(2),
			column: ColumnId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
