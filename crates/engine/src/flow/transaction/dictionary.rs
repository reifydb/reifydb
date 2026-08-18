// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_core::interface::catalog::dictionary::Dictionary;
use reifydb_value::{
	Result,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};
use tracing::instrument;

use super::FlowTransaction;

impl FlowTransaction<'_, '_> {
	pub fn find_dictionary(&self, id: DictionaryId) -> Option<Dictionary> {
		self.catalog().cache().find_dictionary_at(id, self.version())
	}

	pub fn find_dictionary_by_name(&self, name: &str) -> Option<Dictionary> {
		let version = self.version();
		let (namespace_name, dictionary_name) = name.rsplit_once("::")?;
		let namespace = self.catalog().cache().find_namespace_by_name_at(namespace_name, version)?;
		self.catalog().cache().find_dictionary_by_name_at(namespace.id(), dictionary_name, version)
	}

	#[instrument(name = "flow::dictionary::find", level = "trace", skip(self, dictionary, value), fields(dictionary_id = dictionary.id.0))]
	pub fn find_in_dictionary(
		&mut self,
		dictionary: &Dictionary,
		value: &Value,
	) -> Result<Option<DictionaryEntryId>> {
		self.dictionary_allocators().find(dictionary, value)
	}

	#[instrument(name = "flow::dictionary::resolve", level = "trace", skip(self, dictionary, id), fields(dictionary_id = dictionary.id.0))]
	pub fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.dictionary_allocators().get(dictionary, id.to_u128())? {
			Some(bytes) => Ok(Some(from_bytes(&bytes).expect("failed to deserialize dictionary value"))),
			None => Ok(None),
		}
	}
}
