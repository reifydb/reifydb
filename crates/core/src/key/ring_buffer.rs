// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::RingBufferId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct RingBufferKey {
	pub ring_buffer: RingBufferId,
}

impl RingBufferKey {
	pub fn new(ring_buffer: RingBufferId) -> Self {
		Self {
			ring_buffer,
		}
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::ring_buffer_start()), Some(Self::ring_buffer_end()))
	}

	fn ring_buffer_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn ring_buffer_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for RingBufferKey {
	const KIND: KeyKind = KeyKind::RingBuffer;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.ring_buffer);
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

		let ring_buffer = de.read_u64().ok()?;

		Some(Self {
			ring_buffer: RingBufferId(ring_buffer),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RingBufferMetadataKey {
	pub ring_buffer: RingBufferId,
}

impl RingBufferMetadataKey {
	pub fn new(ring_buffer: RingBufferId) -> Self {
		Self {
			ring_buffer,
		}
	}
}

impl EncodableKey for RingBufferMetadataKey {
	const KIND: KeyKind = KeyKind::RingBufferMetadata;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.ring_buffer);
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

		let ring_buffer = de.read_u64().ok()?;

		Some(Self {
			ring_buffer: RingBufferId(ring_buffer),
		})
	}
}
