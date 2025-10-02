// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{MultiVersionStore, SingleVersionTransaction},
};

use crate::mvcc::{
	transaction::{optimistic::Optimistic, query::TransactionManagerQuery, version::StdVersionProvider},
	types::TransactionValue,
};

pub struct QueryTransaction<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> {
	pub(crate) engine: Optimistic<MVS, SMVT>,
	pub(crate) tm: TransactionManagerQuery<StdVersionProvider<SMVT>>,
}

impl<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> QueryTransaction<MVS, SMVT> {
	pub fn new(engine: Optimistic<MVS, SMVT>, version: Option<CommitVersion>) -> crate::Result<Self> {
		let tm = engine.tm.query(version)?;
		Ok(Self {
			engine,
			tm,
		})
	}
}

impl<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> QueryTransaction<MVS, SMVT> {
	pub fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.tm.read_as_of_version_exclusive(version);
	}

	pub fn read_as_of_version_inclusive(&mut self, version: CommitVersion) {
		self.read_as_of_version_exclusive(version + 1)
	}

	pub fn get(&self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>> {
		let version = self.tm.version();
		Ok(self.engine.get(key, version)?.map(Into::into))
	}

	pub fn contains_key(&self, key: &EncodedKey) -> crate::Result<bool> {
		let version = self.tm.version();
		Ok(self.engine.contains_key(key, version)?)
	}

	pub fn scan(&self) -> crate::Result<MVS::ScanIter<'_>> {
		let version = self.tm.version();
		Ok(self.engine.scan(version)?)
	}

	pub fn scan_rev(&self) -> crate::Result<MVS::ScanIterRev<'_>> {
		let version = self.tm.version();
		Ok(self.engine.scan_rev(version)?)
	}

	pub fn range(&self, range: EncodedKeyRange) -> crate::Result<MVS::RangeIter<'_>> {
		let version = self.tm.version();
		Ok(self.engine.range(range, version)?)
	}

	pub fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<MVS::RangeIterRev<'_>> {
		let version = self.tm.version();
		Ok(self.engine.range_rev(range, version)?)
	}

	pub fn prefix(&self, prefix: &EncodedKey) -> crate::Result<MVS::RangeIter<'_>> {
		self.range(EncodedKeyRange::prefix(prefix))
	}

	pub fn prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<MVS::RangeIterRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
