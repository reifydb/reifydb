// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::key::{EncodableKey, KeyKind};
use reifydb_core::EncodedKey;
use reifydb_core::encoding::keycode;
use serde::{Deserialize, Serialize};

#[derive(Debug,Clone, PartialEq)]
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
            _ => {
                Err(serde::de::Error::custom(format!("Invalid SystemVersion value: {value:#04x}")))
            }
        }
    }
}

const VERSION: u8 = 1;

impl EncodableKey for SystemVersionKey {
    const KIND: KeyKind = KeyKind::SystemVersion;

    fn encode(&self) -> EncodedKey {
        let mut out = Vec::with_capacity(2);
        out.extend(&keycode::serialize(&VERSION));
        out.extend(&keycode::serialize(&Self::KIND));
        out.extend(&keycode::serialize(&self.version));
        EncodedKey::new(out)
    }

    fn decode(version: u8, payload: &[u8]) -> Option<Self>
    where
        Self: Sized,
    {
        assert_eq!(version, VERSION);
        assert_eq!(payload.len(), 1);
        keycode::deserialize(&payload[..1]).ok().map(|version| Self { version })
    }
}

#[cfg(test)]
mod tests {
    use crate::key::system_version::SystemVersion;
    use crate::key::{EncodableKey, SystemVersionKey};

    #[test]
    fn test_encode_decode_storage_version() {
        let key = SystemVersionKey { version: SystemVersion::Storage };
        let encoded = key.encode();
        let expected = vec![
            0xFE, // version
            0xF5, // kind
            0xFE,
        ];
        assert_eq!(encoded.as_slice(), expected);
        
        let key = SystemVersionKey::decode(1, &encoded[2..]).unwrap();
        assert_eq!(key.version, SystemVersion::Storage);
    }
}
