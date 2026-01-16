// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::flow::{FlowId, FlowNodeId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct FlowNodeKey {
	pub node: FlowNodeId,
}

const VERSION: u8 = 1;

impl EncodableKey for FlowNodeKey {
	const KIND: KeyKind = KeyKind::FlowNode;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.node);
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

		let node = de.read_u64().ok()?;

		Some(Self {
			node: FlowNodeId(node),
		})
	}
}

impl FlowNodeKey {
	pub fn encoded(node: impl Into<FlowNodeId>) -> EncodedKey {
		Self {
			node: node.into(),
		}
		.encode()
	}

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
pub struct FlowNodeByFlowKey {
	pub flow: FlowId,
	pub node: FlowNodeId,
}

impl EncodableKey for FlowNodeByFlowKey {
	const KIND: KeyKind = KeyKind::FlowNodeByFlow;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.flow).extend_u64(self.node);
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
		let node = de.read_u64().ok()?;

		Some(Self {
			flow: FlowId(flow),
			node: FlowNodeId(node),
		})
	}
}

impl FlowNodeByFlowKey {
	pub fn encoded(flow: impl Into<FlowId>, node: impl Into<FlowNodeId>) -> EncodedKey {
		Self {
			flow: flow.into(),
			node: node.into(),
		}
		.encode()
	}

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
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(FlowId(flow.0 - 1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, FlowNodeByFlowKey, FlowNodeKey};
	use crate::interface::catalog::flow::{FlowId, FlowNodeId};

	#[test]
	fn test_flow_node_key_encode_decode() {
		let key = FlowNodeKey {
			node: FlowNodeId(0x1234),
		};
		let encoded = key.encode();
		let decoded = FlowNodeKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node, FlowNodeId(0x1234));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_flow_node_by_flow_key_encode_decode() {
		let key = FlowNodeByFlowKey {
			flow: FlowId(0x42),
			node: FlowNodeId(0x1234),
		};
		let encoded = key.encode();
		let decoded = FlowNodeByFlowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.flow, FlowId(0x42));
		assert_eq!(decoded.node, FlowNodeId(0x1234));
		assert_eq!(key, decoded);
	}
}
