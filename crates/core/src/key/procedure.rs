// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::KeyKind;
use crate::{interface::catalog::id::ProcedureId, key::typed::key::Key};

#[derive(Debug, Clone, PartialEq)]
pub struct ProcedureKey {
	pub procedure: ProcedureId,
}

impl Key for ProcedureKey {
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
pub mod procedure_key_tests {
	use super::{Key, ProcedureKey};
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

#[derive(Debug, Clone, PartialEq)]
pub struct ProcedureParamKey {
	pub procedure: ProcedureId,
	pub param_index: u16,
}

impl Key for ProcedureParamKey {
	const KIND: KeyKind = KeyKind::ProcedureParam;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.procedure).extend_u16(self.param_index);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let procedure = de.read_u64().ok()?;
		let param_index = de.read_u16().ok()?;

		Some(Self {
			procedure: ProcedureId::from_raw(procedure),
			param_index,
		})
	}
}

impl ProcedureParamKey {
	pub fn encoded(procedure: impl Into<ProcedureId>, param_index: u16) -> EncodedKey {
		Self {
			procedure: procedure.into(),
			param_index,
		}
		.encode()
	}

	pub fn full_scan(procedure: ProcedureId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::params_start(procedure)), Some(Self::params_end(procedure)))
	}

	fn params_start(procedure: ProcedureId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(procedure);
		serializer.to_encoded_key()
	}

	fn params_end(procedure: ProcedureId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*procedure - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod procedure_param_key_tests {
	use super::{Key, ProcedureParamKey};
	use crate::interface::catalog::id::ProcedureId;

	#[test]
	fn test_encode_decode() {
		let key = ProcedureParamKey {
			procedure: ProcedureId::from_raw(0xCAFE),
			param_index: 7,
		};
		let encoded = key.encode();
		let key = ProcedureParamKey::decode(&encoded).unwrap();
		assert_eq!(key.procedure, 0xCAFE);
		assert_eq!(key.param_index, 7);
	}
}
