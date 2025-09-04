// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange, interface::catalog::SourceId,
	util::encoding::keycode,
};

#[derive(Debug, Clone, PartialEq)]
pub struct RowSequenceKey {
	pub source: SourceId,
}

const VERSION: u8 = 1;

impl EncodableKey for RowSequenceKey {
	const KIND: KeyKind = KeyKind::RowSequence;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(11); // 1 + 1 + 9
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize_source_id(&self.source));
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
		if payload.len() != 9 {
			// 9 bytes for source
			return None;
		}

		keycode::deserialize_source_id(&payload[..9]).ok().map(
			|source| Self {
				source,
			},
		)
	}
}

impl RowSequenceKey {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::sequence_start()),
			Some(Self::sequence_end()),
		)
	}

	fn sequence_start() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		EncodedKey::new(out)
	}

	fn sequence_end() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&(Self::KIND as u8 - 1)));
		EncodedKey::new(out)
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
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54,
			0x32, // source id bytes
		];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.source, 0xABCD);
	}
}
