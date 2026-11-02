// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	delta::SetPre,
	interface::{
		catalog::{dictionary::Dictionary, id::ViewId},
		change::Diff,
		store::MultiVersionRow,
	},
	key::{any::TaggedKey, bound::TaggedKeyBoundRange},
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

	fn get_tagged(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<EncodedBytes>>> {
		let keys: Vec<EncodedKey> = keys.iter().map(TaggedKey::encode).collect();
		self.get(&keys)
	}
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

	fn remove_tagged(&mut self, keys: Vec<TaggedKey>) -> Result<()> {
		self.remove(keys.iter().map(TaggedKey::encode).collect())
	}
}

pub trait Range {
	fn range(
		&mut self,
		range: TaggedKeyBoundRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>>;
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
