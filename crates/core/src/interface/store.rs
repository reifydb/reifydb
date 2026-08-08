// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_value::{Result, util::cowvec::CowVec};

use crate::{
	common::CommitVersion,
	delta::Delta,
	interface::catalog::{object::ObjectId, storage::StorageId},
	key::{EncodableKeyRange, Key, partitioned_row::PartitionedRowKeyRange, row::RowKeyRange},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
	Buffer,
	Persistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
	Multi,

	Source(StorageId),

	PartitionedSource(ObjectId),
}

pub fn classify_key(key: &EncodedKey) -> EntryKind {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => EntryKind::Source(row_key.storage),
		Some(Key::PartitionedRow(partitioned_key)) => EntryKind::PartitionedSource(partitioned_key.object),
		_ => EntryKind::Multi,
	}
}

pub fn classify_range(range: &EncodedKeyRange) -> Option<EntryKind> {
	if let (Some(start), Some(_end)) = RowKeyRange::decode(range) {
		return Some(EntryKind::Source(start.storage));
	}

	if let (Some(start), Some(_end)) = PartitionedRowKeyRange::decode(range) {
		return Some(EntryKind::PartitionedSource(start.object));
	}

	None
}

#[derive(Debug, Clone)]
pub struct MultiVersionRow {
	pub key: EncodedKey,
	pub bytes: EncodedBytes,
	pub version: CommitVersion,
}

#[derive(Debug, Clone)]
pub struct SingleVersionRow {
	pub key: EncodedKey,
	pub bytes: EncodedBytes,
}

#[derive(Debug, Clone)]
pub struct MultiVersionBatch {
	pub items: Vec<MultiVersionRow>,

	pub has_more: bool,
}

impl MultiVersionBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

pub trait MultiVersionCommit: Send + Sync {
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()>;
}

pub trait MultiVersionGet: Send + Sync {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>>;
}

pub trait MultiVersionContains: Send + Sync {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool>;
}

pub trait MultiVersionGetPrevious: Send + Sync {
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> Result<Option<MultiVersionRow>>;
}

pub trait MultiVersionStore:
	Send + Sync + Clone + MultiVersionCommit + MultiVersionGet + MultiVersionGetPrevious + MultiVersionContains + 'static
{
}

#[derive(Debug, Clone)]
pub struct SingleVersionBatch {
	pub items: Vec<SingleVersionRow>,

	pub has_more: bool,
}

impl SingleVersionBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

pub trait SingleVersionCommit: Send + Sync {
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()>;
}

pub trait SingleVersionGet: Send + Sync {
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>>;
}

pub trait SingleVersionContains: Send + Sync {
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
}

pub trait SingleVersionSet: SingleVersionCommit {
	fn set(&mut self, key: &EncodedKey, bytes: EncodedBytes) -> Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				bytes: bytes.clone(),
			}]),
		)
	}
}

pub trait SingleVersionRemove: SingleVersionCommit {
	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		Self::commit(self, CowVec::new(vec![Delta::remove_silent(key.clone())]))
	}
}

pub trait SingleVersionRange: Send + Sync {
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	fn range(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_batch(range, 1024)
	}

	fn prefix(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionRangeRev: Send + Sync {
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	fn range_rev(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_rev_batch(range, 1024)
	}

	fn prefix_rev(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionStore:
	Send
	+ Sync
	+ Clone
	+ SingleVersionCommit
	+ SingleVersionGet
	+ SingleVersionContains
	+ SingleVersionSet
	+ SingleVersionRemove
	+ SingleVersionRange
	+ SingleVersionRangeRev
	+ 'static
{
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

	use super::{EntryKind, classify_key, classify_range};
	use crate::{
		interface::catalog::{id::TableId, object::ObjectId, storage::StorageId},
		key::{
			partitioned_row::{PartitionedRowKey, RowLocator},
			row::RowKey,
		},
	};

	fn part(v: &str) -> Partition {
		Partition::of(&[Value::Utf8(v.to_string())])
	}

	#[test]
	fn classify_key_partitioned_row_is_partitioned_source() {
		let object = ObjectId::Table(TableId(7));
		let key = PartitionedRowKey::encoded(object, part("us"), RowLocator::Row(RowNumber(1)));
		assert_eq!(classify_key(&key), EntryKind::PartitionedSource(object));
	}

	#[test]
	fn classify_key_row_is_still_source() {
		let storage = StorageId::Table(TableId(7));
		let key = RowKey::encoded(storage, RowNumber(1));
		assert_eq!(classify_key(&key), EntryKind::Source(storage));
	}

	#[test]
	fn classify_range_all_partition_forms_are_partitioned_source() {
		let object = ObjectId::Table(TableId(9));
		let p = part("us");
		let last = PartitionedRowKey::encoded(object, p, RowLocator::Row(RowNumber(5)));
		assert_eq!(
			classify_range(&PartitionedRowKey::partition_range(object, p)),
			Some(EntryKind::PartitionedSource(object))
		);
		assert_eq!(
			classify_range(&PartitionedRowKey::partition_scan_range(object, p, Some(&last))),
			Some(EntryKind::PartitionedSource(object))
		);
		assert_eq!(
			classify_range(&PartitionedRowKey::scan_range(object, None)),
			Some(EntryKind::PartitionedSource(object))
		);
		assert_eq!(
			classify_range(&PartitionedRowKey::full_scan(object)),
			Some(EntryKind::PartitionedSource(object))
		);
	}

	#[test]
	fn classify_range_row_range_is_still_source() {
		let storage = StorageId::Table(TableId(9));
		assert_eq!(classify_range(&RowKey::full_scan(storage)), Some(EntryKind::Source(storage)));
	}
}
