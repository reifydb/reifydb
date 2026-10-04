// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::interface::{
	catalog::{dictionary::Dictionary, id::ViewId},
	change::Diff,
};
use reifydb_value::{
	Result,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

pub trait Get {
	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>>;
}

pub trait Batch {
	fn set(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()>;

	fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<()>;
}

pub trait Emit {
	fn emit(&mut self, view: ViewId, diff: Diff) -> Result<()>;
}

pub trait Resolve {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>>;

	fn resolve(&mut self, dictionary: &Dictionary, ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>>;
}

pub trait Intern: Resolve {
	fn find(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<Option<DictionaryEntryId>>>;

	fn intern(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<DictionaryEntryId>>;
}
