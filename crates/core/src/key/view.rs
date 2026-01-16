// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::id::ViewId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
	value::encoded::key::{EncodedKey, EncodedKeyRange},
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
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.view);
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

		let view = de.read_u64().ok()?;

		Some(Self {
			view: ViewId(view),
		})
	}
}

impl ViewKey {
	pub fn encoded(view: impl Into<ViewId>) -> EncodedKey {
		Self {
			view: view.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::view_start()), Some(Self::view_end()))
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
pub mod tests {
	use super::{EncodableKey, ViewKey};
	use crate::interface::catalog::id::ViewId;

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
