// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::SequenceId,
	util::encoding::keycode::{self, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SystemSequenceKey {
	pub sequence: SequenceId,
}

const VERSION: u8 = 1;

impl EncodableKey for SystemSequenceKey {
	const KIND: KeyKind = KeyKind::SystemSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.sequence.0);
		serializer.to_encoded_key()
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

		keycode::deserialize(&payload).ok().map(|sequence| Self {
			sequence,
		})
	}
}

impl SystemSequenceKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::sequence_start()), Some(Self::sequence_end()))
	}

	fn sequence_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn sequence_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, SystemSequenceKey};
	use crate::interface::catalog::SequenceId;

	#[test]
	fn test_encode_decode() {
		let key = SystemSequenceKey {
			sequence: SequenceId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![
			0xFE, // version
			0xFA, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32,
		];
		assert_eq!(encoded.as_slice(), expected);

		let key = SystemSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.sequence.0, 0xABCD);
	}
}
