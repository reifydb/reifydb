// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, str::FromStr};

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey, serializer::KeySerializer};
use smallvec::{SmallVec, smallvec};

use super::KeyTag;
use crate::{
	interface::catalog::config::ConfigKey,
	key::{
		any::{ByteEncoding, Field, KeyFields},
		bound::TaggedKeyBoundRange,
	},
};

#[derive(Debug, Clone, PartialEq, Hash)]
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

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

impl ConfigStorageKey {
	pub const TAG: KeyTag = KeyTag::ConfigStorage;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(31);
		serializer.extend_u8(Self::TAG as u8).extend_str(self.key.to_string());
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::TAG {
			return None;
		}

		let config_key_str = de.read_str().ok()?;
		let key = ConfigKey::from_str(&config_key_str).ok()?;

		Some(Self {
			key,
		})
	}
}

impl KeyFields for ConfigStorageKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::BytesDesc(ByteEncoding::Escaped, Cow::Owned(self.key.to_string().into_bytes()))]
	}
}
