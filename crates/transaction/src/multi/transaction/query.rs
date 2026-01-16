// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{
	common::CommitVersion,
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::store::{MultiVersionBatch, MultiVersionValues},
};
use reifydb_type::Result;

use super::{TransactionMulti, manager::TransactionManagerQuery, version::StandardVersionProvider};
use crate::multi::types::TransactionValue;

pub struct QueryTransaction {
	pub(crate) engine: TransactionMulti,
	pub(crate) tm: TransactionManagerQuery<StandardVersionProvider>,
}

impl QueryTransaction {
	pub fn new(engine: TransactionMulti, version: Option<CommitVersion>) -> Result<Self> {
		let tm = engine.tm.query(version)?;
		Ok(Self {
			engine,
			tm,
		})
	}
}

impl QueryTransaction {
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

	pub fn contains_key(&self, key: &EncodedKey) -> Result<bool> {
		let version = self.tm.version();
		Ok(self.engine.contains_key(key, version)?)
	}

	pub fn scan(&self) -> Result<MultiVersionBatch> {
		let items: Vec<_> =
			self.range(EncodedKeyRange::all(), 1024).collect::<std::result::Result<Vec<_>, _>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn scan_rev(&self) -> Result<MultiVersionBatch> {
		let items: Vec<_> =
			self.range_rev(EncodedKeyRange::all(), 1024).collect::<std::result::Result<Vec<_>, _>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self
			.range(EncodedKeyRange::prefix(prefix), 1024)
			.collect::<std::result::Result<Vec<_>, _>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix_rev(&self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self
			.range_rev(EncodedKeyRange::prefix(prefix), 1024)
			.collect::<std::result::Result<Vec<_>, _>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	/// Create a streaming iterator for forward range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// and maintains cursor state internally.
	pub fn range(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_> {
		let version = self.tm.version();
		Box::new(self.engine.store.range(range, version, batch_size))
	}

	/// Create a streaming iterator for reverse range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// in reverse key order and maintains cursor state internally.
	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_> {
		let version = self.tm.version();
		Box::new(self.engine.store.range_rev(range, version, batch_size))
	}
}
