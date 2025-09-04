// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use reifydb_type::RowNumber;

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{EncodableKeyRange, catalog::SourceId},
	util::encoding::keycode,
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct RowKey {
	pub source: SourceId,
	pub row: RowNumber,
}

impl EncodableKey for RowKey {
	const KIND: KeyKind = KeyKind::Row;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(19); // 1 + 1 + 9 + 8
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&self.source));
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
			// 9 bytes for source + 8 bytes for row
			return None;
		}

		let source =
			keycode::deserialize_source_id(&payload[..9]).ok()?;
		let row: RowNumber =
			keycode::deserialize(&payload[9..]).ok()?;

		Some(Self {
			source,
			row,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowKeyRange {
	pub source: SourceId,
}

impl RowKeyRange {
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

		let source =
			keycode::deserialize_source_id(&payload[..9]).ok()?;
		Some(RowKeyRange {
			source,
		})
	}
}

impl EncodableKeyRange for RowKeyRange {
	const KIND: KeyKind = KeyKind::Row;

	fn start(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(11); // 1 + 1 + 9
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&self.source));
		Some(EncodedKey::new(out))
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&self.source.prev()));
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
	pub fn full_scan(source: impl Into<SourceId>) -> EncodedKeyRange {
		let source = source.into();
		EncodedKeyRange::start_end(
			Some(Self::source_start(source)),
			Some(Self::source_end(source)),
		)
	}

	pub fn source_start(source: impl Into<SourceId>) -> EncodedKey {
		let source = source.into();
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&source));
		EncodedKey::new(out)
	}

	pub fn source_end(source: impl Into<SourceId>) -> EncodedKey {
		let source = source.into();
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&source.prev()));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::RowNumber;

	use super::{EncodableKey, RowKey};
	use crate::interface::catalog::SourceId;

	#[test]
	fn test_encode_decode() {
		let key = RowKey {
			source: SourceId::table(0xABCD),
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xFC, // kind
			0x01, // SourceId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED,
			0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = RowKey::decode(&encoded).unwrap();
		assert_eq!(key.source, SourceId::table(0xABCD));
		assert_eq!(key.row, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = RowKey {
			source: SourceId::table(1),
			row: RowNumber(100),
		};
		let key2 = RowKey {
			source: SourceId::table(1),
			row: RowNumber(200),
		};
		let key3 = RowKey {
			source: SourceId::table(2),
			row: RowNumber(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
