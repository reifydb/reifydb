// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange, RowNumber,
	interface::{EncodableKeyRange, catalog::StoreId},
	util::encoding::keycode,
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct RowKey {
	pub store: StoreId,
	pub row: RowNumber,
}

impl EncodableKey for RowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(19); // 1 + 1 + 9 + 8
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&self.store));
		out.extend(&keycode::serialize(&self.row));

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
			// 9 bytes for store + 8 bytes for row
			return None;
		}

		let store =
			keycode::deserialize_store_id(&payload[..9]).ok()?;
		let row: RowNumber =
			keycode::deserialize(&payload[9..]).ok()?;

		Some(Self {
			store,
			row,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoreRowKeyRange {
	pub store: StoreId,
}

impl StoreRowKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
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
		if payload.len() < 9 {
			return None;
		}

		let store =
			keycode::deserialize_store_id(&payload[..9]).ok()?;
		Some(StoreRowKeyRange {
			store,
		})
	}
}

impl EncodableKeyRange for StoreRowKeyRange {
	const KIND: KeyKind = KeyKind::Row;

	fn start(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(11); // 1 + 1 + 9
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&self.store));
		Some(EncodedKey::new(out))
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&self.store.prev()));
		Some(EncodedKey::new(out))
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => {
				Self::decode_key(key)
			}
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => {
				Self::decode_key(key)
			}
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

impl RowKey {
	pub fn full_scan(store: impl Into<StoreId>) -> EncodedKeyRange {
		let store = store.into();
		EncodedKeyRange::start_end(
			Some(Self::store_start(store)),
			Some(Self::store_end(store)),
		)
	}

	pub fn store_start(store: impl Into<StoreId>) -> EncodedKey {
		let store = store.into();
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&store));
		EncodedKey::new(out)
	}

	pub fn store_end(store: impl Into<StoreId>) -> EncodedKey {
		let store = store.into();
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_store_id(&store.prev()));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, RowKey};
	use crate::{RowNumber, interface::catalog::StoreId};

	#[test]
	fn test_encode_decode() {
		let key = RowKey {
			store: StoreId::table(0xABCD),
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xFC, // kind
			0x01, // StoreId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED,
			0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = RowKey::decode(&encoded).unwrap();
		assert_eq!(key.store, StoreId::table(0xABCD));
		assert_eq!(key.row, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = RowKey {
			store: StoreId::table(1),
			row: RowNumber(100),
		};
		let key2 = RowKey {
			store: StoreId::table(1),
			row: RowNumber(200),
		};
		let key3 = RowKey {
			store: StoreId::table(2),
			row: RowNumber(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
