// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::SinkId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SinkKey {
	pub sink: SinkId,
}

const VERSION: u8 = 1;

impl EncodableKey for SinkKey {
	const KIND: KeyKind = KeyKind::Sink;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.sink);
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

		let sink = de.read_u64().ok()?;

		Some(Self {
			sink: SinkId(sink),
		})
	}
}

impl SinkKey {
	pub fn encoded(sink: impl Into<SinkId>) -> EncodedKey {
		Self {
			sink: sink.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::sink_start()), Some(Self::sink_end()))
	}

	fn sink_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn sink_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, SinkKey};
	use crate::interface::catalog::id::SinkId;

	#[test]
	fn test_encode_decode() {
		let key = SinkKey {
			sink: SinkId(0x1234),
		};
		let encoded = key.encode();
		let decoded = SinkKey::decode(&encoded).unwrap();
		assert_eq!(decoded.sink, SinkId(0x1234));
		assert_eq!(key, decoded);
	}
}
