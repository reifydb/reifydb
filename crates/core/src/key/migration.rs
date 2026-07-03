// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use crate::{
	interface::catalog::id::MigrationId,
	key::{EncodableKey, KeyKind},
};

#[derive(Debug, Clone, PartialEq)]
pub struct MigrationKey {
	pub migration: MigrationId,
}

impl MigrationKey {
	pub fn new(migration: MigrationId) -> Self {
		Self {
			migration,
		}
	}

	pub fn encoded(migration: impl Into<MigrationId>) -> EncodedKey {
		Self::new(migration.into()).encode()
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
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for MigrationKey {
	const KIND: KeyKind = KeyKind::Migration;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.migration);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let migration = de.read_u64().ok()?;

		Some(Self {
			migration: MigrationId(migration),
		})
	}
}
