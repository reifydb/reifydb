// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::dictionary::DictionaryId;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::NamespaceId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceDictionaryKey {
	pub namespace: NamespaceId,
	pub dictionary: DictionaryId,
}

impl NamespaceDictionaryKey {
	pub fn new(namespace: NamespaceId, dictionary: DictionaryId) -> Self {
		Self {
			namespace,
			dictionary,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, dictionary: impl Into<DictionaryId>) -> EncodedKey {
		Self::new(namespace.into(), dictionary.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for NamespaceDictionaryKey {
	const KIND: KeyKind = KeyKind::NamespaceDictionary;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.dictionary);
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

		let namespace = de.read_u64().ok()?;
		let dictionary = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			dictionary: DictionaryId(dictionary),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_namespace_dictionary_key_encode_decode() {
		let key = NamespaceDictionaryKey {
			namespace: NamespaceId(1025),
			dictionary: DictionaryId(2048),
		};
		let encoded = key.encode();
		let decoded = NamespaceDictionaryKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, key.namespace);
		assert_eq!(decoded.dictionary, key.dictionary);
	}

	#[test]
	fn test_namespace_dictionary_key_full_scan() {
		use std::ops::Bound;
		let range = NamespaceDictionaryKey::full_scan(NamespaceId(1025));
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}
}
