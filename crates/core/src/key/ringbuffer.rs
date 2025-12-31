// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::RingBufferId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
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
}

impl RingBufferMetadataKey {
	pub fn new(ringbuffer: RingBufferId) -> Self {
		Self {
			ringbuffer,
		}
	}

	pub fn encoded(ringbuffer: impl Into<RingBufferId>) -> EncodedKey {
		Self::new(ringbuffer.into()).encode()
	}
}

impl EncodableKey for RingBufferMetadataKey {
	const KIND: KeyKind = KeyKind::RingBufferMetadata;

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
