// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::SourceId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SourceKey {
	pub source: SourceId,
}

const VERSION: u8 = 1;

impl EncodableKey for SourceKey {
	const KIND: KeyKind = KeyKind::Source;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.source);
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

		let source = de.read_u64().ok()?;

		Some(Self {
			source: SourceId(source),
		})
	}
}

impl SourceKey {
	pub fn encoded(source: impl Into<SourceId>) -> EncodedKey {
		Self {
			source: source.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::source_start()), Some(Self::source_end()))
	}

	fn source_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn source_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, SourceKey};
	use crate::interface::catalog::id::SourceId;

	#[test]
	fn test_encode_decode() {
		let key = SourceKey {
			source: SourceId(0x1234),
		};
		let encoded = key.encode();
		let decoded = SourceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.source, SourceId(0x1234));
		assert_eq!(key, decoded);
	}
}
