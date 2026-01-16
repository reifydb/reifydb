// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::id::SequenceId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
	value::encoded::key::{EncodedKey, EncodedKeyRange},
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
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let sequence = de.read_u64().ok()?;

		Some(Self {
			sequence: SequenceId(sequence),
		})
	}
}

impl SystemSequenceKey {
	pub fn encoded(sequence: impl Into<SequenceId>) -> EncodedKey {
		Self {
			sequence: sequence.into(),
		}
		.encode()
	}

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
pub mod tests {
	use super::{EncodableKey, SystemSequenceKey};
	use crate::interface::catalog::id::SequenceId;

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
