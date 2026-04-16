// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::BindingId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct BindingKey {
	pub binding: BindingId,
}

const VERSION: u8 = 1;

impl EncodableKey for BindingKey {
	const KIND: KeyKind = KeyKind::Binding;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.binding);
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

		let binding = de.read_u64().ok()?;

		Some(Self {
			binding: BindingId(binding),
		})
	}
}

impl BindingKey {
	pub fn encoded(binding: impl Into<BindingId>) -> EncodedKey {
		Self {
			binding: binding.into(),
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
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{BindingKey, EncodableKey};
	use crate::interface::catalog::id::BindingId;

	#[test]
	fn test_encode_decode() {
		let key = BindingKey {
			binding: BindingId(0xABCD),
		};
		let encoded = key.encode();
		let decoded = BindingKey::decode(&encoded).unwrap();
		assert_eq!(decoded.binding, 0xABCD);
	}
}
