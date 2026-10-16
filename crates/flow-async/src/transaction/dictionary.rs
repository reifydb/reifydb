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

	#[instrument(name = "flow::dictionary::find", level = "trace", skip(self, dictionary, value), fields(dictionary_id = dictionary.id.0))]
	fn find_in_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		self.dictionary_allocators().find(dictionary, value)
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
