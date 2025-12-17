// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{ColumnId, ColumnPolicyId},
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnPolicyKey {
	pub column: ColumnId,
	pub policy: ColumnPolicyId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnPolicyKey {
	const KIND: KeyKind = KeyKind::ColumnPolicy;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.column)
			.extend_u64(self.policy);
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

		let column = de.read_u64().ok()?;
		let policy = de.read_u64().ok()?;

		Some(Self {
			column: ColumnId(column),
			policy: ColumnPolicyId(policy),
		})
	}
}

impl ColumnPolicyKey {
	pub fn encoded(column: impl Into<ColumnId>, policy: impl Into<ColumnPolicyId>) -> EncodedKey {
		Self {
			column: column.into(),
			policy: policy.into(),
		}
		.encode()
	}

	pub fn full_scan(column: ColumnId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(column)), Some(Self::link_end(column)))
	}

	fn link_start(column: ColumnId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(column);
		serializer.to_encoded_key()
	}

	fn link_end(column: ColumnId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*column - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{ColumnPolicyKey, EncodableKey};
	use crate::interface::catalog::{ColumnId, ColumnPolicyId};

	#[test]
	fn test_encode_decode() {
		let key = ColumnPolicyKey {
			column: ColumnId(0xABCD),
			policy: ColumnPolicyId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xF6, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnPolicyKey::decode(&encoded).unwrap();
		assert_eq!(key.column, 0xABCD);
		assert_eq!(key.policy, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ColumnPolicyKey {
			column: ColumnId(1),
			policy: ColumnPolicyId(100),
		};
		let key2 = ColumnPolicyKey {
			column: ColumnId(1),
			policy: ColumnPolicyId(200),
		};
		let key3 = ColumnPolicyKey {
			column: ColumnId(2),
			policy: ColumnPolicyId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
