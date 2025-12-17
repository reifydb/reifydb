// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::DictionaryId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

const VERSION: u8 = 1;

/// Key for storing dictionary metadata
#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryKey {
	pub dictionary: DictionaryId,
}

impl DictionaryKey {
	pub fn new(dictionary: DictionaryId) -> Self {
		Self {
			dictionary,
		}
	}

	pub fn encoded(dictionary: impl Into<DictionaryId>) -> EncodedKey {
		Self::new(dictionary.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::dictionary_start()), Some(Self::dictionary_end()))
	}

	fn dictionary_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn dictionary_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for DictionaryKey {
	const KIND: KeyKind = KeyKind::Dictionary;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.dictionary);
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

		let dictionary = de.read_u64().ok()?;

		Some(Self {
			dictionary: DictionaryId(dictionary),
		})
	}
}

/// Key for dictionary entries: hash(value) -> (id, value)
/// Uses xxh3_128 hash of the value for fixed-size keys
#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryEntryKey {
	pub dictionary: DictionaryId,
	pub hash: [u8; 16], // xxh3_128 hash of the value
}

impl DictionaryEntryKey {
	pub fn new(dictionary: DictionaryId, hash: [u8; 16]) -> Self {
		Self {
			dictionary,
			hash,
		}
	}

	pub fn encoded(dictionary: impl Into<DictionaryId>, hash: [u8; 16]) -> EncodedKey {
		Self::new(dictionary.into(), hash).encode()
	}

	pub fn full_scan(dictionary: DictionaryId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::entry_start(dictionary)), Some(Self::entry_end(dictionary)))
	}

	fn entry_start(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(dictionary);
		serializer.to_encoded_key()
	}

	fn entry_end(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*dictionary - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for DictionaryEntryKey {
	const KIND: KeyKind = KeyKind::DictionaryEntry;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.dictionary)
			.extend_bytes(&self.hash);
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

		let dictionary = de.read_u64().ok()?;
		let hash_bytes = de.read_raw(16).ok()?;
		let mut hash = [0u8; 16];
		hash.copy_from_slice(hash_bytes);

		Some(Self {
			dictionary: DictionaryId(dictionary),
			hash,
		})
	}
}

/// Key for reverse lookup: id -> row_number (for decoding)
#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryEntryIndexKey {
	pub dictionary: DictionaryId,
	pub id: u64,
}

impl DictionaryEntryIndexKey {
	pub fn new(dictionary: DictionaryId, id: u64) -> Self {
		Self {
			dictionary,
			id,
		}
	}

	pub fn encoded(dictionary: impl Into<DictionaryId>, id: u64) -> EncodedKey {
		Self::new(dictionary.into(), id).encode()
	}

	pub fn full_scan(dictionary: DictionaryId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::index_start(dictionary)), Some(Self::index_end(dictionary)))
	}

	fn index_start(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(dictionary);
		serializer.to_encoded_key()
	}

	fn index_end(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*dictionary - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for DictionaryEntryIndexKey {
	const KIND: KeyKind = KeyKind::DictionaryEntryIndex;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.dictionary)
			.extend_u64(self.id);
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

		let dictionary = de.read_u64().ok()?;
		let id = de.read_u64().ok()?;

		Some(Self {
			dictionary: DictionaryId(dictionary),
			id,
		})
	}
}

/// Key for dictionary entry ID sequence
#[derive(Debug, Clone, PartialEq)]
pub struct DictionarySequenceKey {
	pub dictionary: DictionaryId,
}

impl DictionarySequenceKey {
	pub fn new(dictionary: DictionaryId) -> Self {
		Self {
			dictionary,
		}
	}

	pub fn encoded(dictionary: impl Into<DictionaryId>) -> EncodedKey {
		Self::new(dictionary.into()).encode()
	}
}

impl EncodableKey for DictionarySequenceKey {
	const KIND: KeyKind = KeyKind::DictionarySequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.dictionary);
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

		let dictionary = de.read_u64().ok()?;

		Some(Self {
			dictionary: DictionaryId(dictionary),
		})
	}
}

/// Key range for dictionary entry index scans
#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryEntryIndexKeyRange {
	pub dictionary: DictionaryId,
	pub start_id: Option<u64>,
	pub end_id: Option<u64>,
}

impl DictionaryEntryIndexKeyRange {
	pub fn new(dictionary: DictionaryId, start_id: Option<u64>, end_id: Option<u64>) -> Self {
		Self {
			dictionary,
			start_id,
			end_id,
		}
	}

	pub fn full(dictionary: DictionaryId) -> Self {
		Self {
			dictionary,
			start_id: None,
			end_id: None,
		}
	}
}

impl EncodableKeyRange for DictionaryEntryIndexKeyRange {
	const KIND: KeyKind = KeyKind::DictionaryEntryIndex;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.dictionary);
		if let Some(id) = self.start_id {
			serializer.extend_u64(id);
		}
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		if let Some(id) = self.end_id {
			let mut serializer = KeySerializer::with_capacity(18);
			serializer
				.extend_u8(VERSION)
				.extend_u8(Self::KIND as u8)
				.extend_u64(self.dictionary)
				.extend_u64(id - 1);
			Some(serializer.to_encoded_key())
		} else {
			let mut serializer = KeySerializer::with_capacity(10);
			serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*self.dictionary - 1);
			Some(serializer.to_encoded_key())
		}
	}

	fn decode(_range: &EncodedKeyRange) -> (Option<Self>, Option<Self>) {
		// Range decoding not typically needed
		(None, None)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_dictionary_key_encode_decode() {
		let key = DictionaryKey {
			dictionary: DictionaryId(0x1234),
		};
		let encoded = key.encode();
		let decoded = DictionaryKey::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
	}

	#[test]
	fn test_dictionary_entry_key_encode_decode() {
		let key = DictionaryEntryKey {
			dictionary: DictionaryId(42),
			hash: [
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
				0x0f, 0x10,
			],
		};
		let encoded = key.encode();
		let decoded = DictionaryEntryKey::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
		assert_eq!(decoded.hash, key.hash);
	}

	#[test]
	fn test_dictionary_entry_index_key_encode_decode() {
		let key = DictionaryEntryIndexKey {
			dictionary: DictionaryId(99),
			id: 12345,
		};
		let encoded = key.encode();
		let decoded = DictionaryEntryIndexKey::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
		assert_eq!(decoded.id, key.id);
	}

	#[test]
	fn test_dictionary_sequence_key_encode_decode() {
		let key = DictionarySequenceKey {
			dictionary: DictionaryId(7),
		};
		let encoded = key.encode();
		let decoded = DictionarySequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
	}

	#[test]
	fn test_dictionary_key_full_scan() {
		use std::ops::Bound;
		let range = DictionaryKey::full_scan();
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_dictionary_entry_key_full_scan() {
		use std::ops::Bound;
		let range = DictionaryEntryKey::full_scan(DictionaryId(42));
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_dictionary_entry_index_key_full_scan() {
		use std::ops::Bound;
		let range = DictionaryEntryIndexKey::full_scan(DictionaryId(42));
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_dictionary_entry_index_key_range() {
		let range = DictionaryEntryIndexKeyRange::full(DictionaryId(42));
		let start = range.start();
		let end = range.end();
		assert!(start.is_some());
		assert!(end.is_some());
	}
}
