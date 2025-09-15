// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey,
	interface::{NamespaceId, RingBufferId},
	util::encoding::keycode,
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
}

impl EncodableKey for NamespaceRingBufferKey {
	const KIND: KeyKind = KeyKind::NamespaceRingBuffer;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(18);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.namespace));
		out.extend(&keycode::serialize(&self.ring_buffer));
		EncodedKey::new(out)
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
