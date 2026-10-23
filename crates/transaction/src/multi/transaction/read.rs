// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;
#[cfg(feature = "testing")]
use std::sync::Mutex;

use reifydb_codec::key::encoded::EncodedKeyRange;
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

	pub fn get(&self, keys: &[TaggedKey]) -> Result<Vec<Option<TransactionValue>>> {
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		#[cfg(feature = "testing")]
		{
			let mut counts = self.op_counts.lock().unwrap();
			counts.get_call(keys.iter().map(TaggedKey::kind));
		}
		let version = self.tm.version();
		Ok(self.engine.get(keys, version)?.into_iter().map(|found| found.map(Into::into)).collect())
	}

	pub fn contains(&self, keys: &[TaggedKey]) -> Result<Vec<bool>> {
		Ok(self.get(keys)?.iter().map(Option::is_some).collect())
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

	pub fn range_encoded(
		&self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		#[cfg(feature = "testing")]
		self.op_counts.lock().unwrap().range_call();
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range(range, multi_scope, batch_size))
	}

	pub fn range(
		&self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		#[cfg(feature = "testing")]
		self.op_counts.lock().unwrap().range_call();
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
		#[cfg(feature = "testing")]
		self.op_counts.lock().unwrap().range_call();
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
		#[cfg(feature = "testing")]
		self.op_counts.lock().unwrap().range_call();
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range_partitioned_row(storage, start, end, multi_scope, batch_size))
	}

	pub fn range_rev(
		&self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		#[cfg(feature = "testing")]
		self.op_counts.lock().unwrap().range_call();
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
