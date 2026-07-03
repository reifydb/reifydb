// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::dictionary::Dictionary,
	key::{
		EncodableKey,
		dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey},
	},
};
use reifydb_transaction::{dictionary::DictionaryReader, multi::RangeScope};
use reifydb_value::{
	Result,
	util::hash::xxh3_128,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

use super::FlowTransaction;

impl DictionaryReader for FlowTransaction {
	fn read(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		self.get(key)
	}

	fn max_index_id(&mut self, dictionary: DictionaryId) -> Result<Option<u128>> {
		let range = DictionaryEntryIndexKey::full_scan(dictionary);
		let mut iter = self.range(range, RangeScope::All, 1);
		match iter.next() {
			Some(result) => Ok(DictionaryEntryIndexKey::decode(&result?.key).map(|key| key.id)),
			None => Ok(None),
		}
	}
}

impl FlowTransaction {
	pub fn find_dictionary(&self, id: DictionaryId) -> Option<Dictionary> {
		self.catalog().cache().find_dictionary_at(id, self.version())
	}

	pub fn find_dictionary_by_name(&self, name: &str) -> Option<Dictionary> {
		let version = self.version();
		let (namespace_name, dictionary_name) = name.rsplit_once("::")?;
		let namespace = self.catalog().cache().find_namespace_by_name_at(namespace_name, version)?;
		self.catalog().cache().find_dictionary_by_name_at(namespace.id(), dictionary_name, version)
	}

	pub fn find_in_dictionary(
		&mut self,
		dictionary: &Dictionary,
		value: &Value,
	) -> Result<Option<DictionaryEntryId>> {
		let value_bytes = to_stdvec(value).expect("failed to serialize dictionary value");
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();
		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
		match self.get(&entry_key)? {
			Some(v) => {
				let id = u128::from_be_bytes(v.0[..16].try_into().unwrap());
				Ok(Some(DictionaryEntryId::from_u128(id, dictionary.id_type.clone())?))
			}
			None => Ok(None),
		}
	}

	pub fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		let index_key = DictionaryEntryIndexKey::new(dictionary.id, id.to_u128()).encode();
		match self.get(&index_key)? {
			Some(v) => Ok(Some(from_bytes(&v.0).expect("failed to deserialize dictionary value"))),
			None => Ok(None),
		}
	}
}
