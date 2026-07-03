// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use crate::{
	interface::catalog::id::MigrationEventId,
	key::{EncodableKey, KeyKind},
};

#[derive(Debug, Clone, PartialEq)]
pub struct MigrationEventKey {
	pub event: MigrationEventId,
}

impl MigrationEventKey {
	pub fn new(event: MigrationEventId) -> Self {
		Self {
			event,
		}
	}

	pub fn encoded(event: impl Into<MigrationEventId>) -> EncodedKey {
		Self::new(event.into()).encode()
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

impl EncodableKey for MigrationEventKey {
	const KIND: KeyKind = KeyKind::MigrationEvent;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.event);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let event = de.read_u64().ok()?;

		Some(Self {
			event: MigrationEventId(event),
		})
	}
}
