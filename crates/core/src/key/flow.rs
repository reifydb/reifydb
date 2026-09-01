// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;

use super::{KeyKind, typed::key::Key};
use crate::interface::catalog::flow::{FlowEdgeId, FlowId};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Flow)]
pub struct FlowKey {
	pub flow: FlowId,
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
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn flow_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod flow_key_tests {
	use super::{FlowKey, Key};
	use crate::interface::catalog::flow::FlowId;

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

	#[test]
	fn test_order_preserving() {
		let key1 = FlowKey {
			flow: FlowId(1),
		};
		let key2 = FlowKey {
			flow: FlowId(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[cfg(test)]
mod verify_byte_identical_flow_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::{FlowKey, Key};
	use crate::interface::catalog::flow::FlowId;

	fn legacy_encode(key: &FlowKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(FlowKey::KIND as u8).extend_u64(key.flow);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn matches_legacy_byte_layout() {
		for flow in [0u64, 1, 42, 0x1234, u64::MAX] {
			let key = FlowKey {
				flow: FlowId(flow),
			};
			assert_eq!(legacy_encode(&key), key.encode().as_slice().to_vec(), "flow={flow:#x}");
		}
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = FlowEdge)]
pub struct FlowEdgeKey {
	pub edge: FlowEdgeId,
}

impl FlowEdgeKey {
	pub fn encoded(edge: impl Into<FlowEdgeId>) -> EncodedKey {
		Self {
			edge: edge.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8((Self::KIND as u8) - 1);
		serializer.to_encoded_key()
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = FlowEdgeByFlow)]
pub struct FlowEdgeByFlowKey {
	pub flow: FlowId,
	pub edge: FlowEdgeId,
}

impl FlowEdgeByFlowKey {
	pub fn encoded(flow: impl Into<FlowId>, edge: impl Into<FlowEdgeId>) -> EncodedKey {
		Self {
			flow: flow.into(),
			edge: edge.into(),
		}
		.encode()
	}

	pub fn full_scan(flow: FlowId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start(flow)), Some(Self::end(flow)))
	}

	fn start(flow: FlowId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(flow);
		serializer.to_encoded_key()
	}

	fn end(flow: FlowId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(FlowId(flow.0 - 1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod flow_edge_by_flow_key_tests {
	use super::{FlowEdgeByFlowKey, FlowEdgeKey, Key};
	use crate::interface::catalog::flow::{FlowEdgeId, FlowId};

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
	fn test_flow_edge_key_order_preserving() {
		let key1 = FlowEdgeKey {
			edge: FlowEdgeId(1),
		};
		let key2 = FlowEdgeKey {
			edge: FlowEdgeId(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
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

	#[test]
	fn test_flow_edge_by_flow_key_order_preserving() {
		let key1 = FlowEdgeByFlowKey {
			flow: FlowId(1),
			edge: FlowEdgeId(100),
		};
		let key2 = FlowEdgeByFlowKey {
			flow: FlowId(1),
			edge: FlowEdgeId(200),
		};
		let key3 = FlowEdgeByFlowKey {
			flow: FlowId(2),
			edge: FlowEdgeId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded2 < encoded1, "edge ordering not preserved within same flow");
		assert!(encoded3 < encoded2, "flow ordering not preserved");
	}
}

#[cfg(test)]
mod verify_byte_identical_flow_edge_by_flow_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::{FlowEdgeByFlowKey, FlowEdgeKey, Key};
	use crate::interface::catalog::flow::{FlowEdgeId, FlowId};

	fn legacy_encode_edge(key: &FlowEdgeKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(FlowEdgeKey::KIND as u8).extend_u64(key.edge);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	fn legacy_encode_by_flow(key: &FlowEdgeByFlowKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(FlowEdgeByFlowKey::KIND as u8).extend_u64(key.flow).extend_u64(key.edge);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn flow_edge_key_matches_legacy_byte_layout() {
		for edge in [0u64, 1, 42, 0x1234, u64::MAX] {
			let key = FlowEdgeKey {
				edge: FlowEdgeId(edge),
			};
			assert_eq!(legacy_encode_edge(&key), key.encode().as_slice().to_vec(), "edge={edge:#x}");
		}
	}

	#[test]
	fn flow_edge_by_flow_key_matches_legacy_byte_layout() {
		for (flow, edge) in [(0u64, 0u64), (1, 2), (0x42, 0x1234), (u64::MAX, u64::MAX)] {
			let key = FlowEdgeByFlowKey {
				flow: FlowId(flow),
				edge: FlowEdgeId(edge),
			};
			assert_eq!(
				legacy_encode_by_flow(&key),
				key.encode().as_slice().to_vec(),
				"flow={flow:#x} edge={edge:#x}"
			);
		}
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = FlowVersion)]
pub struct FlowVersionKey {
	pub flow: FlowId,
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
pub mod flow_version_key_tests {
	use super::{FlowVersionKey, Key};
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

	#[test]
	fn test_order_preserving() {
		let key1 = FlowVersionKey {
			flow: FlowId(1),
		};
		let key2 = FlowVersionKey {
			flow: FlowId(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[cfg(test)]
mod verify_byte_identical_flow_version_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::{FlowVersionKey, Key};
	use crate::interface::catalog::flow::FlowId;

	fn legacy_encode(key: &FlowVersionKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(FlowVersionKey::KIND as u8).extend_u64(key.flow);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn matches_legacy_byte_layout() {
		for flow in [0u64, 1, 42, 0x1234, u64::MAX] {
			let key = FlowVersionKey {
				flow: FlowId(flow),
			};
			assert_eq!(legacy_encode(&key), key.encode().as_slice().to_vec(), "flow={flow:#x}");
		}
	}
}
