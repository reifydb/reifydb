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
use reifydb_store_transaction::{MultiVersionRange, MultiVersionRangeRev, TransactionStore};

use crate::multi::{
	transaction::{
		query::TransactionManagerQuery, serializable::TransactionSerializable, version::StandardVersionProvider,
	},
	types::TransactionValue,
};

pub struct QueryTransaction {
	pub(crate) engine: TransactionSerializable,
	pub(crate) tm: TransactionManagerQuery<StandardVersionProvider>,
}

impl QueryTransaction {
	pub fn new(engine: TransactionSerializable, version: Option<CommitVersion>) -> crate::Result<Self> {
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

	pub fn get(&self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>> {
		let version = self.tm.version();
		Ok(self.engine.get(key, version)?.map(Into::into))
	}

	pub fn contains_key(&self, key: &EncodedKey) -> crate::Result<bool> {
		let version = self.tm.version();
		Ok(self.engine.contains_key(key, version)?)
	}

	pub fn scan(&self) -> crate::Result<<TransactionStore as MultiVersionRange>::RangeIter<'_>> {
		self.range(EncodedKeyRange::all())
	}

	pub fn scan_rev(&self) -> crate::Result<<TransactionStore as MultiVersionRangeRev>::RangeIterRev<'_>> {
		self.range_rev(EncodedKeyRange::all())
	}

	pub fn range_batched(
		&self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<<TransactionStore as MultiVersionRange>::RangeIter<'_>> {
		let version = self.tm.version();
		Ok(self.engine.range_batched(range, version, batch_size)?)
	}

	pub fn range(
		&self,
		range: EncodedKeyRange,
	) -> crate::Result<<TransactionStore as MultiVersionRange>::RangeIter<'_>> {
		self.range_batched(range, 1024)
	}

	pub fn range_rev_batched(
		&self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<<TransactionStore as MultiVersionRangeRev>::RangeIterRev<'_>> {
		let version = self.tm.version();
		Ok(self.engine.range_rev_batched(range, version, batch_size)?)
	}

	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
	) -> crate::Result<<TransactionStore as MultiVersionRangeRev>::RangeIterRev<'_>> {
		self.range_rev_batched(range, 1024)
	}

	pub fn prefix(
		&self,
		prefix: &EncodedKey,
	) -> crate::Result<<TransactionStore as MultiVersionRange>::RangeIter<'_>> {
		self.range(EncodedKeyRange::prefix(prefix))
	}

	pub fn prefix_rev(
		&self,
		prefix: &EncodedKey,
	) -> crate::Result<<TransactionStore as MultiVersionRangeRev>::RangeIterRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
