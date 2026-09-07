// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::EncodableKey;
use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::flow::OperatorId,
	key::{
		any::{Field, KeyFields, Width},
		bound::AnyKeyBoundRange,
	},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, EncodableKey, Hash)]
#[key(kind = OperatorSettings)]
pub struct OperatorSettingsKey {
	pub operator: OperatorId,
}

impl OperatorSettingsKey {
	pub fn new(operator: impl Into<OperatorId>) -> Self {
		Self {
			operator: operator.into(),
		}
	}

	pub fn encoded(operator: impl Into<OperatorId>) -> EncodedKey {
		Self::new(operator).encode()
	}

	pub fn full_scan() -> AnyKeyBoundRange {
		AnyKeyBoundRange::kind(Self::KIND)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{
		interface::catalog::{id::TableId, storage::StorageId},
		key::row::RowSettingsKey,
	};

	#[test]
	fn test_operator_settings_key_roundtrip() {
		let key = OperatorSettingsKey {
			operator: OperatorId(12345),
		};

		let encoded = key.encode();
		let decoded = OperatorSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_settings_key_rejects_other_kind() {
		let other = RowSettingsKey::encoded(StorageId::Table(TableId(1)));
		assert!(OperatorSettingsKey::decode(&other).is_none());
	}

	#[test]
	fn test_order_preserving() {
		let key1 = OperatorSettingsKey {
			operator: OperatorId(1),
		};
		let key2 = OperatorSettingsKey {
			operator: OperatorId(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[cfg(test)]
mod verify_byte_identical {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::{EncodableKey, OperatorSettingsKey};
	use crate::interface::catalog::flow::OperatorId;

	fn legacy_encode(key: &OperatorSettingsKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(OperatorSettingsKey::KIND as u8).extend_u64(key.operator);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn matches_legacy_byte_layout() {
		for operator in [0u64, 1, 42, 12345, u64::MAX] {
			let key = OperatorSettingsKey {
				operator: OperatorId(operator),
			};
			assert_eq!(legacy_encode(&key), key.encode().as_slice().to_vec(), "operator={operator:#x}");
		}
	}
}
