// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange, interface::catalog::NamespaceId,
	util::encoding::keycode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceKey {
	pub namespace: NamespaceId,
}

const VERSION: u8 = 1;

impl EncodableKey for NamespaceKey {
	const KIND: KeyKind = KeyKind::Namespace;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.namespace));
		EncodedKey::new(out)
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
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
		if payload.len() != 8 {
			return None;
		}

		keycode::deserialize(&payload[..8]).ok().map(|namespace| Self {
			namespace,
		})
	}
}

impl NamespaceKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::namespace_start()),
			Some(Self::namespace_end()),
		)
	}

	fn namespace_start() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		EncodedKey::new(out)
	}

	fn namespace_end() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&(Self::KIND as u8 - 1)));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, NamespaceKey};
	use crate::interface::catalog::NamespaceId;

	#[test]
	fn test_encode_decode() {
		let key = NamespaceKey {
			namespace: NamespaceId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![
			0xFE, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54,
			0x32,
		];
		assert_eq!(encoded.as_slice(), expected);

		let key = NamespaceKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
	}
}
