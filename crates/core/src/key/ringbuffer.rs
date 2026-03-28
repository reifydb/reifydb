// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::Value;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::RingBufferId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct RingBufferKey {
	pub ringbuffer: RingBufferId,
}

impl RingBufferKey {
	pub fn new(ringbuffer: RingBufferId) -> Self {
		Self {
			ringbuffer,
		}
	}

	pub fn encoded(ringbuffer: impl Into<RingBufferId>) -> EncodedKey {
		Self::new(ringbuffer.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::ringbuffer_start()), Some(Self::ringbuffer_end()))
	}

	fn ringbuffer_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn ringbuffer_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for RingBufferKey {
	const KIND: KeyKind = KeyKind::RingBuffer;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.ringbuffer);
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

		let ringbuffer = de.read_u64().ok()?;

		Some(Self {
			ringbuffer: RingBufferId(ringbuffer),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RingBufferMetadataKey {
	pub ringbuffer: RingBufferId,
	pub partition_values: Vec<Value>,
}

impl RingBufferMetadataKey {
	pub fn new(ringbuffer: RingBufferId) -> Self {
		Self {
			ringbuffer,
			partition_values: vec![],
		}
	}

	pub fn encoded(ringbuffer: impl Into<RingBufferId>) -> EncodedKey {
		Self::new(ringbuffer.into()).encode()
	}

	pub fn encoded_partition(ringbuffer: impl Into<RingBufferId>, partition_values: Vec<Value>) -> EncodedKey {
		Self {
			ringbuffer: ringbuffer.into(),
			partition_values,
		}
		.encode()
	}

	/// Returns a range scanning all partition metadata keys for a given ringbuffer (prefix scan).
	pub fn full_scan_for_ringbuffer(ringbuffer: RingBufferId) -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(VERSION);
		start.extend_u8(Self::KIND as u8);
		start.extend_u64(ringbuffer);
		let start_key = start.to_encoded_key();

		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(VERSION);
		end.extend_u8(Self::KIND as u8);
		end.extend_u64(RingBufferId(ringbuffer.0 - 1));
		let end_key = end.to_encoded_key();

		EncodedKeyRange::start_end(Some(start_key), Some(end_key))
	}
}

impl EncodableKey for RingBufferMetadataKey {
	const KIND: KeyKind = KeyKind::RingBufferMetadata;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.ringbuffer);
		for value in &self.partition_values {
			serializer.extend_value(value);
		}
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

		let ringbuffer = de.read_u64().ok()?;

		Some(Self {
			ringbuffer: RingBufferId(ringbuffer),
			partition_values: vec![],
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_metadata_key_encode_decode_roundtrip() {
		let key = RingBufferMetadataKey::encoded_partition(
			RingBufferId(42),
			vec![Value::Utf8("east".to_string())],
		);
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let _ = (de.read_u8(), de.read_u8(), de.read_u64());
		let value = de.read_value().unwrap();
		assert_eq!(value, Value::Utf8("east".to_string()));
	}

	#[test]
	fn test_metadata_key_encode_decode_multiple() {
		let key = RingBufferMetadataKey::encoded_partition(
			RingBufferId(7),
			vec![Value::Utf8("us".to_string()), Value::Uint8(42)],
		);
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let _ = (de.read_u8(), de.read_u8(), de.read_u64());
		assert_eq!(de.read_value().unwrap(), Value::Utf8("us".to_string()));
		assert_eq!(de.read_value().unwrap(), Value::Uint8(42));
	}
}
