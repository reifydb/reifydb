// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;
#[cfg(feature = "testing")]
use std::sync::Mutex;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::storage::StorageId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::{
		any::TaggedKey,
		bound::TaggedKeyBoundRange,
		row::{StoragePartitionedRowKey, StorageRowKey},
	},
};
use reifydb_value::Result;

use super::{MultiTransaction, manager::TransactionManagerQuery, version::StandardVersionProvider};
#[cfg(feature = "testing")]
use crate::counting::OpCounts;
use crate::multi::{RangeScope, lease::VersionLeaseGuard, types::TransactionValue};

pub struct MultiReadTransaction {
	pub(crate) engine: MultiTransaction,
	pub(crate) tm: TransactionManagerQuery<StandardVersionProvider>,
	#[allow(dead_code)]
	pub(crate) lease: Option<VersionLeaseGuard>,
	#[cfg(feature = "testing")]
	op_counts: Mutex<OpCounts>,
}

impl MultiReadTransaction {
	pub fn new(engine: MultiTransaction, version: Option<CommitVersion>) -> Result<Self> {
		let tm = engine.tm.query(version)?;
		Ok(Self {
			engine,
			tm,
			lease: None,
			#[cfg(feature = "testing")]
			op_counts: Mutex::default(),
		})
	}

	pub fn new_with_lease(engine: MultiTransaction, lease: VersionLeaseGuard) -> Result<Self> {
		let version = lease.version();
		let tm = engine.tm.query(Some(version))?;
		Ok(Self {
			engine,
			tm,
			lease: Some(lease),
			#[cfg(feature = "testing")]
			op_counts: Mutex::default(),
		})
	}
}

impl MultiReadTransaction {
	pub fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.tm.read_as_of_version_exclusive(version);
	}

	pub fn read_as_of_version_inclusive(&mut self, version: CommitVersion) {
		self.read_as_of_version_exclusive(CommitVersion(version.0 + 1))
	}

	#[cfg(feature = "testing")]
	pub fn op_counts(&self) -> OpCounts {
		self.op_counts.lock().unwrap().clone()
	}

	pub fn get<K: Into<TaggedKey> + Clone>(&self, key: &K) -> Result<Option<TransactionValue>> {
		let version = self.tm.version();
		let key = key.clone().into();
		#[cfg(feature = "testing")]
		{
			self.op_counts.lock().unwrap().key(key.kind()).gets += 1;
		}
		Ok(self.engine.get(&key, version)?.map(Into::into))
	}

	pub fn contains<K: Into<TaggedKey> + Clone>(&self, key: &K) -> Result<bool> {
		let version = self.tm.version();
		let key = key.clone().into();
		#[cfg(feature = "testing")]
		{
			self.op_counts.lock().unwrap().key(key.kind()).contains += 1;
		}
		self.engine.contains_key(&key, version)
	}

	pub fn get_many(&self, keys: &[TaggedKey]) -> Result<Vec<Option<TransactionValue>>> {
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		#[cfg(feature = "testing")]
		{
			let mut counts = self.op_counts.lock().unwrap();
			counts.get_many_call();
			for key in keys {
				counts.key(key.kind()).batched_gets += 1;
			}
		}
		let version = self.tm.version();
		Ok(self.engine.get_many(keys, version)?.into_iter().map(|found| found.map(Into::into)).collect())
	}

	pub fn contains_many(&self, keys: &[TaggedKey]) -> Result<Vec<bool>> {
		Ok(self.get_many(keys)?.iter().map(Option::is_some).collect())
	}

	pub fn scan(&self) -> Result<MultiVersionBatch<TaggedKey>> {
		let items: Vec<_> = self
			.range_encoded(EncodedKeyRange::all(), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch<TaggedKey>> {
		let items: Vec<_> = self
			.range_encoded(EncodedKeyRange::prefix(prefix), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix_rev(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch<TaggedKey>> {
		let items: Vec<_> = self
			.range_rev_encoded(EncodedKeyRange::prefix(prefix), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn range_encoded(
		&self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range(range, multi_scope, batch_size))
	}

	pub fn range_rev_encoded(
		&self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range_rev(range, multi_scope, batch_size))
	}

	pub fn range(
		&self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range(range.encode(), multi_scope, batch_size))
	}

	pub fn range_row(
		&self,
		storage: StorageId,
		start: Bound<StorageRowKey>,
		end: Bound<StorageRowKey>,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<StorageRowKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range_row(storage, start, end, multi_scope, batch_size))
	}

	pub fn range_partitioned_row(
		&self,
		storage: StorageId,
		start: Bound<StoragePartitionedRowKey>,
		end: Bound<StoragePartitionedRowKey>,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<StoragePartitionedRowKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range_partitioned_row(storage, start, end, multi_scope, batch_size))
	}

	pub fn range_rev(
		&self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range_rev(range.encode(), multi_scope, batch_size))
	}
}

impl Clone for MultiReadTransaction {
	fn clone(&self) -> Self {
		Self {
			engine: self.engine.clone(),
			tm: self.tm.clone(),
			lease: None,
			#[cfg(feature = "testing")]
			op_counts: Mutex::default(),
		}
	}
}
