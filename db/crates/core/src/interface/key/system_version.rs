// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey,
	util::encoding::keycode::{self, KeySerializer},
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
			_ => Err(serde::de::Error::custom(format!(
				"Invalid SystemVersion value: {value:#04x}"
			))),
		}
	}
}

const VERSION: u8 = 1;

impl EncodableKey for SystemVersionKey {
	const KIND: KeyKind = KeyKind::SystemVersion;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(3);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_serialize(&self.version);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self>
	where
		Self: Sized,
	{
		if key.len() < 2 {
			return None;
		}

		let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}

		let payload = &key[2..];
		if payload.len() != 1 {
			return None;
		}

		keycode::deserialize(&payload[..1]).ok().map(|version| Self {
			version,
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
