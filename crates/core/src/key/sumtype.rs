// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::sumtype::SumTypeId;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct SumTypeKey {
	pub sumtype: SumTypeId,
}

impl SumTypeKey {
	pub fn new(sumtype: SumTypeId) -> Self {
		Self {
			sumtype,
		}
	}

	pub fn encoded(sumtype: impl Into<SumTypeId>) -> EncodedKey {
		Self::new(sumtype.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for SumTypeKey {
	const KIND: KeyKind = KeyKind::SumType;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.sumtype);
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

		let sumtype = de.read_u64().ok()?;

		Some(Self {
			sumtype: SumTypeId(sumtype),
		})
	}
}
