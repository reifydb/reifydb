// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{NamespaceId, RingBufferId},
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceRingBufferKey {
	pub namespace: NamespaceId,
	pub ringbuffer: RingBufferId,
}

impl NamespaceRingBufferKey {
	pub fn new(namespace: NamespaceId, ringbuffer: RingBufferId) -> Self {
		Self {
			namespace,
			ringbuffer,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, ringbuffer: impl Into<RingBufferId>) -> EncodedKey {
		Self::new(namespace.into(), ringbuffer.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for NamespaceRingBufferKey {
	const KIND: KeyKind = KeyKind::NamespaceRingBuffer;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.ringbuffer);
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

		let namespace = de.read_u64().ok()?;
		let ringbuffer = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			ringbuffer: RingBufferId(ringbuffer),
		})
	}
}
