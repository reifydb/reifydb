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

pub trait FlowGet {
	fn get(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>>;
}

pub trait Get {
	fn get_tagged(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<EncodedBytes>>>;
}

impl<T: FlowGet> Get for T {
	fn get_tagged(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<EncodedBytes>>> {
		let keys: Vec<EncodedKey> = keys.iter().map(TaggedKey::encode).collect();
		self.get(&keys)
	}
}

pub trait FlowBatch {
	fn set(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()>;

	fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<()>;
}

pub trait Batch {
	fn set_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()>;

	fn remove_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<()>;

	fn remove_tagged(&mut self, keys: Vec<TaggedKey>) -> Result<()>;
}

impl<T: FlowBatch> Batch for T {
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

pub trait StorageTransaction: Get + Batch {}

impl<T: Get + Batch> StorageTransaction for T {}

pub trait FlowStorageTransaction: FlowGet + FlowBatch {}

impl<T: FlowGet + FlowBatch> FlowStorageTransaction for T {}

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
