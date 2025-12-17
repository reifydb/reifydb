// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::FlowId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct FlowKey {
	pub flow: FlowId,
}

const VERSION: u8 = 1;

impl EncodableKey for FlowKey {
	const KIND: KeyKind = KeyKind::Flow;

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

impl FlowKey {
	pub fn encoded(flow: impl Into<FlowId>) -> EncodedKey {
		Self {
			flow: flow.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::flow_start()), Some(Self::flow_end()))
	}

	fn flow_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn flow_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, FlowKey};
	use crate::interface::catalog::FlowId;

	#[test]
	fn test_encode_decode() {
		let key = FlowKey {
			flow: FlowId(0x1234),
		};
		let encoded = key.encode();
		let decoded = FlowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.flow, FlowId(0x1234));
		assert_eq!(key, decoded);
	}
}
