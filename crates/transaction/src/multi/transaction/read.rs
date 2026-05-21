// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::store::{MultiVersionBatch, MultiVersionRow},
};
use reifydb_type::Result;

use super::{MultiTransaction, manager::TransactionManagerQuery, version::StandardVersionProvider};
use crate::multi::{lease::VersionLeaseGuard, types::TransactionValue};

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

	pub fn get(&self, key: &EncodedKey) -> Result<Option<TransactionValue>> {
		let version = self.tm.version();
		Ok(self.engine.get(key, version)?.map(Into::into))
	}

	pub fn get_many(&self, keys: &[EncodedKey]) -> Result<HashMap<EncodedKey, MultiVersionRow>> {
		let version = self.tm.version();
		self.engine.store.get_many(keys, version)
	}

	pub fn contains_key(&self, key: &EncodedKey) -> Result<bool> {
		let version = self.tm.version();
		self.engine.contains_key(key, version)
	}

	pub fn scan(&self) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn scan_rev(&self) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self.range_rev(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self.range(EncodedKeyRange::prefix(prefix), 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix_rev(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> =
			self.range_rev(EncodedKeyRange::prefix(prefix), 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn range(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let version = self.tm.version();
		Box::new(self.engine.store.range(range, version, batch_size))
	}

	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let version = self.tm.version();
		Box::new(self.engine.store.range_rev(range, version, batch_size))
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
