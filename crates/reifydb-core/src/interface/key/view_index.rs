// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{
		EncodableKeyRange,
		catalog::{IndexId, ViewId},
	},
	util::encoding::keycode,
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct ViewIndexKey {
	pub view: ViewId,
	pub index: IndexId,
}

impl EncodableKey for ViewIndexKey {
	const KIND: KeyKind = KeyKind::ViewIndex;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(18);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.view));
		out.extend(&keycode::serialize(&self.index));

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
			.map(|(view, index)| Self {
				view,
				index,
			})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct ViewIndexKeyRange {
	pub view: ViewId,
}

impl ViewIndexKeyRange {
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
		if payload.len() < 8 {
			return None;
		}

		let view: ViewId = keycode::deserialize(&payload[..8]).ok()?;
		Some(ViewIndexKeyRange {
			view,
		})
	}
}

impl EncodableKeyRange for ViewIndexKeyRange {
	const KIND: KeyKind = KeyKind::ViewIndex;

	fn start(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.view));
		Some(EncodedKey::new(out))
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&(*self.view - 1)));
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

impl ViewIndexKey {
	pub fn full_scan(view: ViewId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::view_start(view)),
			Some(Self::view_end(view)),
		)
	}

	pub fn view_start(view: ViewId) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&view));
		EncodedKey::new(out)
	}

	pub fn view_end(view: ViewId) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&(*view - 1)));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, ViewIndexKey};
	use crate::interface::catalog::{IndexId, ViewId};

	#[test]
	fn test_encode_decode() {
		let key = ViewIndexKey {
			view: ViewId(0xABCD),
			index: IndexId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xE9, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED,
			0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ViewIndexKey::decode(&encoded).unwrap();
		assert_eq!(key.view, 0xABCD);
		assert_eq!(key.index, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ViewIndexKey {
			view: ViewId(1),
			index: IndexId(100),
		};
		let key2 = ViewIndexKey {
			view: ViewId(1),
			index: IndexId(200),
		};
		let key3 = ViewIndexKey {
			view: ViewId(2),
			index: IndexId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
