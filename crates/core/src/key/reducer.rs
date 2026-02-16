// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::reducer::ReducerId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ReducerKey {
	pub reducer: ReducerId,
}

const VERSION: u8 = 1;

impl EncodableKey for ReducerKey {
	const KIND: KeyKind = KeyKind::Reducer;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.reducer);
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

		let reducer = de.read_u64().ok()?;

		Some(Self {
			reducer: ReducerId(reducer),
		})
	}
}

impl ReducerKey {
	pub fn encoded(reducer: impl Into<ReducerId>) -> EncodedKey {
		Self {
			reducer: reducer.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::reducer_start()), Some(Self::reducer_end()))
	}

	fn reducer_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn reducer_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, ReducerKey};
	use crate::interface::catalog::reducer::ReducerId;

	#[test]
	fn test_encode_decode() {
		let key = ReducerKey {
			reducer: ReducerId(0x1234),
		};
		let encoded = key.encode();
		let decoded = ReducerKey::decode(&encoded).unwrap();
		assert_eq!(decoded.reducer, ReducerId(0x1234));
		assert_eq!(key, decoded);
	}
}
