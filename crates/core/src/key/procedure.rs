// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;

use super::KeyTag;
use crate::{
	interface::catalog::id::ProcedureId,
	key::{
		any::{Field, KeyFields, Width},
		bound::TaggedKeyBoundRange,
	},
};

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Procedure)]
pub struct ProcedureKey {
	pub procedure: ProcedureId,
}

impl ProcedureKey {
	pub fn new(procedure: impl Into<ProcedureId>) -> Self {
		Self {
			procedure: procedure.into(),
		}
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod procedure_key_tests {
	use super::ProcedureKey;
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

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = ProcedureParam)]
pub struct ProcedureParamKey {
	pub procedure: ProcedureId,
	pub param_index: u16,
}

impl ProcedureParamKey {
	pub fn new(procedure: impl Into<ProcedureId>, param_index: u16) -> Self {
		Self {
			procedure: procedure.into(),
			param_index,
		}
	}

	pub fn full_scan(procedure: ProcedureId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, *procedure as u128)])
	}
}

#[cfg(test)]
pub mod procedure_param_key_tests {
	use super::ProcedureParamKey;
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
