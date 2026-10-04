// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

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

use crate::transaction::FlowTransaction;

pub trait DictionaryExtension: FlowTransaction {
	fn find_dictionary(&self, id: DictionaryId) -> Option<Dictionary> {
		self.catalog().cache().find_dictionary(id)
	}

	fn find_dictionary_by_name(&self, name: &str) -> Option<Dictionary> {
		let version = self.version();
		let (namespace_name, dictionary_name) = name.rsplit_once("::")?;
		let namespace = self.catalog().cache().find_namespace_by_name_at(namespace_name, version)?;
		self.catalog().cache().find_dictionary_by_name_at(namespace.id(), dictionary_name, version)
	}

	#[instrument(name = "flow::dictionary::find", level = "trace", skip(self, dictionary, value), fields(dictionary_id = dictionary.id.0))]
	fn find_in_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		self.dictionary_allocators().find(dictionary, value)
	}

	#[instrument(name = "flow::dictionary::resolve", level = "trace", skip(self, dictionary, id), fields(dictionary_id = dictionary.id.0))]
	fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.dictionary_allocators().get(dictionary, id.to_u128())? {
			Some(bytes) => Ok(Some(from_bytes(&bytes).expect("failed to deserialize dictionary value"))),
			None => Ok(None),
		}
	}

	#[instrument(name = "flow::dictionary::find_many", level = "trace", skip(self, dictionary, values), fields(dictionary_id = dictionary.id.0, values = values.len()))]
	fn find_many_in_dictionary(
		&mut self,
		dictionary: &Dictionary,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>> {
		self.dictionary_allocators().find_batch(dictionary, values)
	}

	#[instrument(name = "flow::dictionary::resolve_many", level = "trace", skip(self, dictionary, ids), fields(dictionary_id = dictionary.id.0, ids = ids.len()))]
	fn get_many_from_dictionary(
		&mut self,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>> {
		let raw: Vec<u128> = ids.iter().map(|id| id.to_u128()).collect();
		let answers = self.dictionary_allocators().get_batch(dictionary, &raw)?;
		let mut decoded: HashMap<u128, Value> = HashMap::new();
		Ok(raw.into_iter()
			.zip(answers)
			.map(|(id, bytes)| {
				bytes.map(|bytes| {
					decoded.entry(id)
						.or_insert_with(|| {
							from_bytes(&bytes)
								.expect("failed to deserialize dictionary value")
						})
						.clone()
				})
			})
			.collect())
	}
}

impl<T: FlowTransaction> DictionaryExtension for T {}
