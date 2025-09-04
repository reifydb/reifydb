// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{ColumnId, EncodableKey, KeyKind, SourceId},
	util::encoding::keycode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnKey {
	pub source: SourceId,
	pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnKey {
	const KIND: KeyKind = KeyKind::Column;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(19); // 1 + 1 + 9 + 8
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&self.source));
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
			// 9 bytes for source + 8 bytes for column
			return None;
		}

		let source =
			keycode::deserialize_source_id(&payload[..9]).ok()?;
		let column: ColumnId =
			keycode::deserialize(&payload[9..]).ok()?;

		Some(Self {
			source,
			column,
		})
	}
}

impl ColumnKey {
	pub fn full_scan(source: impl Into<SourceId>) -> EncodedKeyRange {
		let source = source.into();
		EncodedKeyRange::start_end(
			Some(Self::start(source)),
			Some(Self::end(source)),
		)
	}

	fn start(source: SourceId) -> EncodedKey {
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&source));
		EncodedKey::new(out)
	}

	fn end(source: SourceId) -> EncodedKey {
		let mut out = Vec::with_capacity(11);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&source.prev()));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use super::EncodableKey;
	use crate::interface::{
		ColumnKey,
		catalog::{ColumnId, SourceId},
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnKey {
			source: SourceId::table(0xABCD),
			column: ColumnId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xF8, // kind
			0x01, // SourceId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54,
			0x32, // source id bytes
			0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21,
			0x0F, // column id bytes
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnKey::decode(&encoded).unwrap();
		assert_eq!(key.source, 0xABCD);
		assert_eq!(key.column, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ColumnKey {
			source: SourceId::table(1),
			column: ColumnId(100),
		};
		let key2 = ColumnKey {
			source: SourceId::table(1),
			column: ColumnId(200),
		};
		let key3 = ColumnKey {
			source: SourceId::table(2),
			column: ColumnId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
