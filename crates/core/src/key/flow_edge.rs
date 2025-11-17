// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{FlowEdgeId, FlowId},
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct FlowEdgeKey {
	pub edge: FlowEdgeId,
}

const VERSION: u8 = 1;

impl EncodableKey for FlowEdgeKey {
	const KIND: KeyKind = KeyKind::FlowEdge;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.edge);
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

		let edge = de.read_u64().ok()?;

		Some(Self {
			edge: FlowEdgeId(edge),
		})
	}
}

impl FlowEdgeKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8((Self::KIND as u8) - 1);
		serializer.to_encoded_key()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlowEdgeByFlowKey {
	pub flow: FlowId,
	pub edge: FlowEdgeId,
}

impl EncodableKey for FlowEdgeByFlowKey {
	const KIND: KeyKind = KeyKind::FlowEdgeByFlow;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.flow).extend_u64(self.edge);
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
		let edge = de.read_u64().ok()?;

		Some(Self {
			flow: FlowId(flow),
			edge: FlowEdgeId(edge),
		})
	}
}

impl FlowEdgeByFlowKey {
	pub fn full_scan(flow: FlowId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start(flow)), Some(Self::end(flow)))
	}

	fn start(flow: FlowId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(flow);
		serializer.to_encoded_key()
	}

	fn end(flow: FlowId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(FlowId(flow.0 + 1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, FlowEdgeByFlowKey, FlowEdgeKey};
	use crate::interface::catalog::{FlowEdgeId, FlowId};

	#[test]
	fn test_flow_edge_key_encode_decode() {
		let key = FlowEdgeKey {
			edge: FlowEdgeId(0x1234),
		};
		let encoded = key.encode();
		let decoded = FlowEdgeKey::decode(&encoded).unwrap();
		assert_eq!(decoded.edge, FlowEdgeId(0x1234));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_flow_edge_by_flow_key_encode_decode() {
		let key = FlowEdgeByFlowKey {
			flow: FlowId(0x42),
			edge: FlowEdgeId(0x1234),
		};
		let encoded = key.encode();
		let decoded = FlowEdgeByFlowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.flow, FlowId(0x42));
		assert_eq!(decoded.edge, FlowEdgeId(0x1234));
		assert_eq!(key, decoded);
	}
}
