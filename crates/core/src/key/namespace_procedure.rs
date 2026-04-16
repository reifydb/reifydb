// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::{NamespaceId, ProcedureId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceProcedureKey {
	pub namespace: NamespaceId,
	pub procedure: ProcedureId,
}

const VERSION: u8 = 1;

impl EncodableKey for NamespaceProcedureKey {
	const KIND: KeyKind = KeyKind::NamespaceProcedure;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.procedure);
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

		let namespace = de.read_u64().ok()?;
		let procedure = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			procedure: ProcedureId::from_raw(procedure),
		})
	}
}

impl NamespaceProcedureKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, procedure: impl Into<ProcedureId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			procedure: procedure.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, NamespaceProcedureKey};
	use crate::interface::catalog::id::{NamespaceId, ProcedureId};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceProcedureKey {
			namespace: NamespaceId(0xABCD),
			procedure: ProcedureId::from_raw(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let key = NamespaceProcedureKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
		assert_eq!(key.procedure, 0x123456789ABCDEF0);
	}
}
