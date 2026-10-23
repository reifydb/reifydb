// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;

use super::KeyTag;
use crate::{
	interface::catalog::config::ConfigKey,
	key::{
		any::{ByteEncoding, Field, KeyFields},
		bound::TaggedKeyBoundRange,
	},
};

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = ConfigStorage)]
pub struct ConfigStorageKey {
	#[key(display)]
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
