// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, hash_map::Entry};

use postcard::from_bytes;
use reifydb_core::{interface::catalog::dictionary::Dictionary, internal_error};
use reifydb_value::{
	Result,
	value::{Value, dictionary::DictionaryEntryId},
};

use crate::{dictionary::DictionaryAllocatorRegistry, transaction::Transaction};

impl Transaction<'_> {
	pub fn get_from_dictionary(
		&mut self,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>> {
		match self {
			Transaction::Command(cmd) => get_from_registry(cmd.dictionary_allocators(), dictionary, ids),
			Transaction::Admin(admin) => get_from_registry(admin.dictionary_allocators(), dictionary, ids),
			Transaction::Test(t) => get_from_registry(t.inner.dictionary_allocators(), dictionary, ids),
			Transaction::Query(query) => {
				get_from_registry(Some(query.dictionary_allocators()), dictionary, ids)
			}
		}
	}
}

fn get_from_registry(
	registry: Option<DictionaryAllocatorRegistry>,
	dictionary: &Dictionary,
	ids: &[DictionaryEntryId],
) -> Result<Vec<Option<Value>>> {
	let registry = registry.ok_or_else(|| internal_error!("dictionary allocator registry is not configured"))?;
	let raw: Vec<u128> = ids.iter().map(|id| id.to_u128()).collect();
	let answers = registry.get_batch(dictionary, &raw)?;
	let mut decoded: HashMap<u128, Value> = HashMap::new();
	raw.into_iter()
		.zip(answers)
		.map(|(id, bytes)| {
			let Some(bytes) = bytes else {
				return Ok(None);
			};
			let value = match decoded.entry(id) {
				Entry::Occupied(found) => found.get().clone(),
				Entry::Vacant(slot) => slot
					.insert(from_bytes(&bytes)
						.map_err(|e| internal_error!("Failed to deserialize value: {}", e))?)
					.clone(),
			};
			Ok(Some(value))
		})
		.collect()
}
