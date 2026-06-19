// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::flow::FlowNodeId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorSettingsKey {
	pub operator: FlowNodeId,
}

impl OperatorSettingsKey {
	pub fn encoded(operator: impl Into<FlowNodeId>) -> EncodedKey {
		Self {
			operator: operator.into(),
		}
		.encode()
	}
}

impl EncodableKey for OperatorSettingsKey {
	const KIND: KeyKind = KeyKind::OperatorSettings;

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

		Some(Self {
			operator: FlowNodeId(de.read_u64().ok()?),
		})
	}
}

pub struct OperatorSettingsKeyRange;

impl OperatorSettingsKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(OperatorSettingsKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(OperatorSettingsKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{
		interface::catalog::{id::TableId, shape::ShapeId},
		key::row_settings::RowSettingsKey,
	};

	#[test]
	fn test_operator_settings_key_roundtrip() {
		let key = OperatorSettingsKey {
			operator: FlowNodeId(12345),
		};

		let encoded = key.encode();
		let decoded = OperatorSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_settings_key_rejects_other_kind() {
		let other = RowSettingsKey::encoded(ShapeId::Table(TableId(1)));
		assert!(OperatorSettingsKey::decode(&other).is_none());
	}
}
