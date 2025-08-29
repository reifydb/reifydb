// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, EncodedKeyRange, KeyKind};
use crate::{EncodedKey, interface::PrimaryKeyId, util::encoding::keycode};

#[derive(Debug, Clone)]
pub struct PrimaryKeyKey {
	pub primary_key: PrimaryKeyId,
}

const VERSION: u8 = 1;

impl EncodableKey for PrimaryKeyKey {
	const KIND: KeyKind = KeyKind::PrimaryKey;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.primary_key));
		EncodedKey::new(out)
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}
		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}
		let primary_key: PrimaryKeyId =
			keycode::deserialize(&key[2..]).ok()?;
		Some(Self {
			primary_key,
		})
	}
}

impl PrimaryKeyKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::primary_key_start()),
			Some(Self::primary_key_end()),
		)
	}

	fn primary_key_start() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		EncodedKey::new(out)
	}

	fn primary_key_end() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&(Self::KIND as u8 - 1)));
		EncodedKey::new(out)
	}
}
