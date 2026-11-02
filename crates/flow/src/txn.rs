// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	delta::SetPre,
	interface::{
		catalog::{dictionary::Dictionary, id::ViewId},
		change::Diff,
	},
	key::any::TaggedKey,
};
use reifydb_value::{
	Result,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

pub trait Get {
	fn get(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>>;
}

pub trait Batch {
	fn set(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()>;

	fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<()>;

	fn set_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()> {
		let (keys, rows) = entries.into_iter().map(|(key, row, _)| (key.encode(), row)).unzip();
		self.set(keys, rows)
	}

	fn remove_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<()> {
		self.remove(entries.iter().map(|(key, _)| key.encode()).collect())
	}
}

pub trait StorageTxn: Get + Batch {}

impl<T: Get + Batch> StorageTxn for T {}

pub trait Emit {
	fn emit(&mut self, view: ViewId, diff: Diff) -> Result<()>;
}

pub trait Resolve {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>>;

	fn resolve(&mut self, dictionary: &Dictionary, ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>>;
}

pub trait Find: Resolve {
	fn find(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<Option<DictionaryEntryId>>>;
}

pub trait Intern: Find {
	fn intern(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<DictionaryEntryId>>;
}
