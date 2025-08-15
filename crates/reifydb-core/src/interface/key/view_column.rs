// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{ViewColumnId, ViewId},
	util::encoding::keycode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ViewColumnKey {
	pub view: ViewId,
	pub column: ViewColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ViewColumnKey {
	const KIND: KeyKind = KeyKind::ViewColumn;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(18);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.view));
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
		if payload.len() != 16 {
			return None;
		}

		keycode::deserialize(&payload[..8])
			.ok()
			.zip(keycode::deserialize(&payload[8..]).ok())
			.map(|(view, column)| Self {
				view,
				column,
			})
	}
}

impl ViewColumnKey {
	pub fn full_scan(view: ViewId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::start(view)),
			Some(Self::end(view)),
		)
	}

	fn start(view: ViewId) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&view));
		EncodedKey::new(out)
	}

	fn end(view: ViewId) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&(*view - 1)));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, ViewColumnKey};
	use crate::interface::catalog::{ViewColumnId, ViewId};

	#[test]
	fn test_encode_decode() {
		let key = ViewColumnKey {
			view: ViewId(0xABCD),
			column: ViewColumnId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xEB, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED,
			0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ViewColumnKey::decode(&encoded).unwrap();
		assert_eq!(key.view, 0xABCD);
		assert_eq!(key.column, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ViewColumnKey {
			view: ViewId(1),
			column: ViewColumnId(100),
		};
		let key2 = ViewColumnKey {
			view: ViewId(1),
			column: ViewColumnId(200),
		};
		let key3 = ViewColumnKey {
			view: ViewId(2),
			column: ViewColumnId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
