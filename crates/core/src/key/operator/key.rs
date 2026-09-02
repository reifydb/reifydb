// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;

use super::super::{KeyKind, typed::key::Key};
use crate::interface::catalog::flow::{FlowId, OperatorId};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Operator)]
pub struct OperatorKey {
	pub operator: OperatorId,
}

impl OperatorKey {
	pub fn encoded(operator: impl Into<OperatorId>) -> EncodedKey {
		Self {
			operator: operator.into(),
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
#[key(kind = OperatorByFlow)]
pub struct OperatorByFlowKey {
	pub flow: FlowId,
	pub operator: OperatorId,
}

impl OperatorByFlowKey {
	pub fn encoded(flow: impl Into<FlowId>, operator: impl Into<OperatorId>) -> EncodedKey {
		Self {
			flow: flow.into(),
			operator: operator.into(),
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
pub mod tests {
	use super::{Key, OperatorByFlowKey, OperatorKey};
	use crate::interface::catalog::flow::{FlowId, OperatorId};

	#[test]
	fn test_operator_key_encode_decode() {
		let key = OperatorKey {
			operator: OperatorId(0x1234),
		};
		let encoded = key.encode();
		let decoded = OperatorKey::decode(&encoded).unwrap();
		assert_eq!(decoded.operator, OperatorId(0x1234));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_key_order_preserving() {
		let key1 = OperatorKey {
			operator: OperatorId(1),
		};
		let key2 = OperatorKey {
			operator: OperatorId(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
	}

	#[test]
	fn test_operator_by_flow_key_encode_decode() {
		let key = OperatorByFlowKey {
			flow: FlowId(0x42),
			operator: OperatorId(0x1234),
		};
		let encoded = key.encode();
		let decoded = OperatorByFlowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.flow, FlowId(0x42));
		assert_eq!(decoded.operator, OperatorId(0x1234));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_by_flow_key_order_preserving() {
		let key1 = OperatorByFlowKey {
			flow: FlowId(1),
			operator: OperatorId(100),
		};
		let key2 = OperatorByFlowKey {
			flow: FlowId(1),
			operator: OperatorId(200),
		};
		let key3 = OperatorByFlowKey {
			flow: FlowId(2),
			operator: OperatorId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded2 < encoded1, "operator ordering not preserved within same flow");
		assert!(encoded3 < encoded2, "flow ordering not preserved");
	}
}

#[cfg(test)]
mod verify_byte_identical {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::{Key, OperatorByFlowKey, OperatorKey};
	use crate::interface::catalog::flow::{FlowId, OperatorId};

	fn legacy_encode_operator(key: &OperatorKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(OperatorKey::KIND as u8).extend_u64(key.operator);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	fn legacy_encode_by_flow(key: &OperatorByFlowKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(OperatorByFlowKey::KIND as u8).extend_u64(key.flow).extend_u64(key.operator);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn operator_key_matches_legacy_byte_layout() {
		for operator in [0u64, 1, 42, 0x1234, u64::MAX] {
			let key = OperatorKey {
				operator: OperatorId(operator),
			};
			assert_eq!(
				legacy_encode_operator(&key),
				key.encode().as_slice().to_vec(),
				"operator={operator:#x}"
			);
		}
	}

	#[test]
	fn operator_by_flow_key_matches_legacy_byte_layout() {
		for (flow, operator) in [(0u64, 0u64), (1, 2), (0x42, 0x1234), (u64::MAX, u64::MAX)] {
			let key = OperatorByFlowKey {
				flow: FlowId(flow),
				operator: OperatorId(operator),
			};
			assert_eq!(
				legacy_encode_by_flow(&key),
				key.encode().as_slice().to_vec(),
				"flow={flow:#x} operator={operator:#x}"
			);
		}
	}
}
