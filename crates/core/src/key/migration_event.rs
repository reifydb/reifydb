// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::MigrationEventId,
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

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

impl EncodableKey for MigrationEventKey {
	const KIND: KeyKind = KeyKind::MigrationEvent;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.event);
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

		let event = de.read_u64().ok()?;

		Some(Self {
			event: MigrationEventId(event),
		})
	}
}
