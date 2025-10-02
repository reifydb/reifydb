// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, EncodedKeyRange, KeyKind};
use crate::{
	EncodedKey,
	interface::PrimaryKeyId,
	util::encoding::keycode::{self, KeySerializer},
};

#[derive(Debug, Clone)]
pub struct PrimaryKeyKey {
	pub primary_key: PrimaryKeyId,
}

const VERSION: u8 = 1;

impl EncodableKey for PrimaryKeyKey {
	const KIND: KeyKind = KeyKind::PrimaryKey;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.primary_key);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}
		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}
		let primary_key: PrimaryKeyId = keycode::deserialize(&key[2..]).ok()?;
		Some(Self {
			primary_key,
		})
	}
}

impl PrimaryKeyKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::primary_key_start()), Some(Self::primary_key_end()))
	}

	fn primary_key_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn primary_key_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}
