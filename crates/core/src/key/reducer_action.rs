// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::reducer::ReducerActionId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ReducerActionKey {
	pub action: ReducerActionId,
}

const VERSION: u8 = 1;

impl EncodableKey for ReducerActionKey {
	const KIND: KeyKind = KeyKind::ReducerAction;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.action);
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

		let action = de.read_u64().ok()?;

		Some(Self {
			action: ReducerActionId(action),
		})
	}
}

impl ReducerActionKey {
	pub fn encoded(action: impl Into<ReducerActionId>) -> EncodedKey {
		Self {
			action: action.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8((Self::KIND as u8) - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, ReducerActionKey};
	use crate::interface::catalog::reducer::ReducerActionId;

	#[test]
	fn test_encode_decode() {
		let key = ReducerActionKey {
			action: ReducerActionId(0x1234),
		};
		let encoded = key.encode();
		let decoded = ReducerActionKey::decode(&encoded).unwrap();
		assert_eq!(decoded.action, ReducerActionId(0x1234));
		assert_eq!(key, decoded);
	}
}
