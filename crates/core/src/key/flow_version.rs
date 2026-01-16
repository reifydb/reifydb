// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::EncodedKey,
	interface::catalog::flow::FlowId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

/// Key for storing a flow's last processed CDC version.
/// Used for exactly-once processing semantics across restarts.
#[derive(Debug, Clone, PartialEq)]
pub struct FlowVersionKey {
	pub flow: FlowId,
}

const VERSION: u8 = 1;

impl EncodableKey for FlowVersionKey {
	const KIND: KeyKind = KeyKind::FlowVersion;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.flow);
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

		let flow = de.read_u64().ok()?;

		Some(Self {
			flow: FlowId(flow),
		})
	}
}

impl FlowVersionKey {
	pub fn new(flow: impl Into<FlowId>) -> Self {
		Self {
			flow: flow.into(),
		}
	}

	pub fn encoded(flow: impl Into<FlowId>) -> EncodedKey {
		Self::new(flow).encode()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, FlowVersionKey};
	use crate::interface::catalog::flow::FlowId;

	#[test]
	fn test_encode_decode() {
		let key = FlowVersionKey {
			flow: FlowId(0x1234),
		};
		let encoded = key.encode();
		let decoded = FlowVersionKey::decode(&encoded).unwrap();
		assert_eq!(decoded.flow, FlowId(0x1234));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_new_and_encoded() {
		let key = FlowVersionKey::new(42u64);
		assert_eq!(key.flow, FlowId(42));

		let encoded = FlowVersionKey::encoded(42u64);
		let decoded = FlowVersionKey::decode(&encoded).unwrap();
		assert_eq!(decoded.flow, FlowId(42));
	}
}
