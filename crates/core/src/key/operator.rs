// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::flow::{FlowId, OperatorId};

#[derive(Debug, Clone, PartialEq)]
pub struct OperatorKey {
	pub operator: OperatorId,
}

impl EncodableKey for OperatorKey {
	const KIND: KeyKind = KeyKind::Operator;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.operator);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let operator = de.read_u64().ok()?;

		Some(Self {
			operator: OperatorId(operator),
		})
	}
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

#[derive(Debug, Clone, PartialEq)]
pub struct OperatorByFlowKey {
	pub flow: FlowId,
	pub operator: OperatorId,
}

impl EncodableKey for OperatorByFlowKey {
	const KIND: KeyKind = KeyKind::OperatorByFlow;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.flow).extend_u64(self.operator);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let flow = de.read_u64().ok()?;
		let operator = de.read_u64().ok()?;

		Some(Self {
			flow: FlowId(flow),
			operator: OperatorId(operator),
		})
	}
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
	use super::{EncodableKey, OperatorByFlowKey, OperatorKey};
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
}
