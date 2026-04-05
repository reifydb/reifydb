// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::str::FromStr;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::config::ConfigKey,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigStorageKey {
	pub key: ConfigKey,
}

impl ConfigStorageKey {
	pub fn new(key: ConfigKey) -> Self {
		Self {
			key,
		}
	}

	pub fn for_key(key: ConfigKey) -> EncodedKey {
		Self::new(key).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(2);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(2);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}
}

impl EncodableKey for ConfigStorageKey {
	const KIND: KeyKind = KeyKind::ConfigStorage;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_str(self.key.to_string());
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

		let config_key_str = de.read_str().ok()?;
		let key = ConfigKey::from_str(&config_key_str)
			.expect("failed to decode ConfigKey from storage, unknown key");

		Some(Self {
			key,
		})
	}
}
