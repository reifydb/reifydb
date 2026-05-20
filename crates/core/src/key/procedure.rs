// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::ProcedureId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ProcedureKey {
	pub procedure: ProcedureId,
}

impl EncodableKey for ProcedureKey {
	const KIND: KeyKind = KeyKind::Procedure;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.procedure);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let procedure = de.read_u64().ok()?;

		Some(Self {
			procedure: ProcedureId::from_raw(procedure),
		})
	}
}

impl ProcedureKey {
	pub fn encoded(procedure: impl Into<ProcedureId>) -> EncodedKey {
		Self {
			procedure: procedure.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::procedure_start()), Some(Self::procedure_end()))
	}

	fn procedure_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn procedure_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, ProcedureKey};
	use crate::interface::catalog::id::ProcedureId;

	#[test]
	fn test_encode_decode() {
		let key = ProcedureKey {
			procedure: ProcedureId::from_raw(0xABCD),
		};
		let encoded = key.encode();
		let key = ProcedureKey::decode(&encoded).unwrap();
		assert_eq!(key.procedure, 0xABCD);
	}
}
