// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{NamespaceId, RingBufferId},
	util::encoding::keycode::{self, KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceRingBufferKey {
	pub namespace: NamespaceId,
	pub ring_buffer: RingBufferId,
}

impl NamespaceRingBufferKey {
	pub fn new(namespace: NamespaceId, ring_buffer: RingBufferId) -> Self {
		Self {
			namespace,
			ring_buffer,
		}
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::link_start(namespace)),
			Some(Self::link_end(namespace)),
		)
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(*namespace - 1);
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
			.extend_u64(self.ring_buffer);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}

		let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}

		let payload = &key[2..];
		if payload.len() != 16 {
			return None;
		}

		let namespace: NamespaceId =
			keycode::deserialize(&payload[0..8]).ok()?;
		let ring_buffer: RingBufferId =
			keycode::deserialize(&payload[8..16]).ok()?;

		Some(Self {
			namespace,
			ring_buffer,
		})
	}
}
