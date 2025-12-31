// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SystemVersionKey {
	pub version: SystemVersion,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum SystemVersion {
	Storage = 0x01,
}

impl From<SystemVersion> for u8 {
	fn from(version: SystemVersion) -> Self {
		version as u8
	}
}
impl TryFrom<u8> for SystemVersion {
	type Error = serde::de::value::Error;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0x01 => Ok(Self::Storage),
			_ => Err(serde::de::Error::custom(format!("Invalid SystemVersion value: {value:#04x}"))),
		}
	}
}

impl SystemVersionKey {
	pub fn encoded(version: SystemVersion) -> EncodedKey {
		Self {
			version,
		}
		.encode()
	}
}

const VERSION: u8 = 1;

impl EncodableKey for SystemVersionKey {
	const KIND: KeyKind = KeyKind::SystemVersion;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(3);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_serialize(&self.version);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self>
	where
		Self: Sized,
	{
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let version_enum = de.read_u8().ok()?.try_into().ok()?;

		Some(Self {
			version: version_enum,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, SystemVersion, SystemVersionKey};

	#[test]
	fn test_encode_decode_storage_version() {
		let key = SystemVersionKey {
			version: SystemVersion::Storage,
		};
		let encoded = key.encode();
		let expected = vec![
			0xFE, // version
			0xF5, // kind
			0xFE,
		];
		assert_eq!(encoded.as_slice(), expected);

		let key = SystemVersionKey::decode(&encoded).unwrap();
		assert_eq!(key.version, SystemVersion::Storage);
	}
}
