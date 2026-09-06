// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::storage::StorageId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::{
		any::AnyKey,
		bound::AnyKeyBoundRange,
		row::{StoragePartitionedRowKey, StorageRowKey},
	},
};
use reifydb_value::Result;
use tracing::instrument;

use super::{MultiTransaction, manager::TransactionManagerQuery, version::StandardVersionProvider};
use crate::multi::{RangeScope, lease::VersionLeaseGuard, types::TransactionValue};

pub struct MultiReadTransaction {
	pub(crate) engine: MultiTransaction,
	pub(crate) tm: TransactionManagerQuery<StandardVersionProvider>,
	#[allow(dead_code)]
	pub(crate) lease: Option<VersionLeaseGuard>,
}

impl MultiReadTransaction {
	pub fn new(engine: MultiTransaction, version: Option<CommitVersion>) -> Result<Self> {
		let tm = engine.tm.query(version)?;
		Ok(Self {
			engine,
			tm,
			lease: None,
		})
	}

	pub fn new_with_lease(engine: MultiTransaction, lease: VersionLeaseGuard) -> Result<Self> {
		let version = lease.version();
		let tm = engine.tm.query(Some(version))?;
		Ok(Self {
			engine,
			tm,
			lease: Some(lease),
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

	pub fn get<K: Into<AnyKey> + Clone>(&self, key: &K) -> Result<Option<TransactionValue>> {
		let version = self.tm.version();
		Ok(self.engine.get(&key.clone().into(), version)?.map(Into::into))
	}

	#[instrument(name = "transaction::get_many", level = "trace", skip(self, keys), fields(key_count = keys.len()))]
	pub fn get_many(&self, keys: &[EncodedKey]) -> Result<HashMap<EncodedKey, MultiVersionRow>> {
		let version = self.tm.version();
		self.engine.store.get_many(keys, version)
	}

	pub fn contains<K: Into<AnyKey> + Clone>(&self, key: &K) -> Result<bool> {
		let version = self.tm.version();
		self.engine.contains_key(&key.clone().into(), version)
	}

	pub fn scan(&self) -> Result<MultiVersionBatch<AnyKey>> {
		let items: Vec<_> = self
			.range_encoded(EncodedKeyRange::all(), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch<AnyKey>> {
		let items: Vec<_> = self
			.range_encoded(EncodedKeyRange::prefix(prefix), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix_rev(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch<AnyKey>> {
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
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<AnyKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range(range, multi_scope, batch_size))
	}

	pub fn range_rev_encoded(
		&self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<AnyKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.tm.version());
		Box::new(self.engine.store.range_rev(range, multi_scope, batch_size))
	}

	pub fn range(
		&self,
		range: AnyKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<AnyKey>>> + Send + '_> {
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
		range: AnyKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<AnyKey>>> + Send + '_> {
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
		}
	}
}
