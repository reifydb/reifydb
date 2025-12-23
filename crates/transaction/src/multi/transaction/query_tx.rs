// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange};
use reifydb_store_transaction::MultiVersionBatch;

use super::{TransactionMulti, query::TransactionManagerQuery, version::StandardVersionProvider};
use crate::multi::types::TransactionValue;

pub struct QueryTransaction {
	pub(crate) engine: TransactionMulti,
	pub(crate) tm: TransactionManagerQuery<StandardVersionProvider>,
}

impl QueryTransaction {
	pub async fn new(engine: TransactionMulti, version: Option<CommitVersion>) -> crate::Result<Self> {
		let tm = engine.tm.query(version).await?;
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

	pub async fn get(&self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>> {
		let version = self.tm.version();
		Ok(self.engine.get(key, version).await?.map(Into::into))
	}

	pub async fn contains_key(&self, key: &EncodedKey) -> crate::Result<bool> {
		let version = self.tm.version();
		Ok(self.engine.contains_key(key, version).await?)
	}

	pub async fn scan(&self) -> crate::Result<MultiVersionBatch> {
		self.range(EncodedKeyRange::all()).await
	}

	pub async fn scan_rev(&self) -> crate::Result<MultiVersionBatch> {
		self.range_rev(EncodedKeyRange::all()).await
	}

	pub async fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<MultiVersionBatch> {
		let version = self.tm.version();
		Ok(self.engine.range_batch(range, version, batch_size).await?)
	}

	pub async fn range(&self, range: EncodedKeyRange) -> crate::Result<MultiVersionBatch> {
		self.range_batch(range, 1024).await
	}

	pub async fn range_rev_batch(
		&self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		let version = self.tm.version();
		Ok(self.engine.range_rev_batch(range, version, batch_size).await?)
	}

	pub async fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<MultiVersionBatch> {
		self.range_rev_batch(range, 1024).await
	}

	pub async fn prefix(&self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix)).await
	}

	pub async fn prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix)).await
	}
}
