// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::SourceId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct RowSequenceKey {
	pub source: SourceId,
}

const VERSION: u8 = 1;

impl EncodableKey for RowSequenceKey {
	const KIND: KeyKind = KeyKind::RowSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11); // 1 + 1 + 9
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_source_id(self.source);
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

		let source = de.read_source_id().ok()?;

		Some(Self {
			source,
		})
	}
}

impl RowSequenceKey {
	pub fn encoded(source: impl Into<SourceId>) -> EncodedKey {
		Self {
			source: source.into(),
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
mod tests {
	use super::{EncodableKey, RowSequenceKey};
	use crate::interface::catalog::SourceId;

	#[test]
	fn test_encode_decode() {
		let key = RowSequenceKey {
			source: SourceId::table(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![
			0xFE, // version
			0xF7, // kind
			0x01, // SourceId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, // source id bytes
		];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.source, 0xABCD);
	}
}
