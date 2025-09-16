// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::ViewId,
	util::encoding::keycode::{self, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ViewKey {
	pub view: ViewId,
}

const VERSION: u8 = 1;

impl EncodableKey for ViewKey {
	const KIND: KeyKind = KeyKind::View;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.view);
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
		if payload.len() != 8 {
			return None;
		}

		keycode::deserialize(&payload[..8]).ok().map(|view| Self {
			view,
		})
	}
}

impl ViewKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::view_start()),
			Some(Self::view_end()),
		)
	}

	fn view_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn view_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, ViewKey};
	use crate::interface::catalog::ViewId;

	#[test]
	fn test_encode_decode() {
		let key = ViewKey {
			view: ViewId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![
			0xFE, // version
			0xEF, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32,
		];
		assert_eq!(encoded.as_slice(), expected);

		let key = ViewKey::decode(&encoded).unwrap();
		assert_eq!(key.view, 0xABCD);
	}
}
