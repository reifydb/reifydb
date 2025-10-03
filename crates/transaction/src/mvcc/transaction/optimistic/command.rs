// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::ops::RangeBounds;

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, event::transaction::PostCommitEvent,
	interface::SingleVersionTransaction, value::encoded::EncodedValues,
};
use reifydb_type::Error;

use super::*;
use crate::mvcc::{
	transaction::{
		TransactionManagerCommand, range::TransactionRangeIter, range_rev::TransactionRangeRevIter,
		scan::TransactionScanIter, scan_rev::TransactionScanRevIter, version::StdVersionProvider,
	},
	types::TransactionValue,
};

pub struct CommandTransaction<MVS: MultiVersionStore, SVT: SingleVersionTransaction> {
	engine: OptimisticTransaction<MVS, SVT>,
	pub(crate) tm: TransactionManagerCommand<StdVersionProvider<SVT>>,
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> CommandTransaction<MVS, SVT> {
	pub fn new(engine: OptimisticTransaction<MVS, SVT>) -> crate::Result<Self> {
		let tm = engine.tm.write()?;
		Ok(Self {
			engine,
			tm,
		})
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> CommandTransaction<MVS, SVT> {
	pub fn commit(&mut self) -> Result<CommitVersion, Error> {
		let mut version: Option<CommitVersion> = None;
		let mut deltas = CowVec::with_capacity(8);
		let transaction_id = self.tm.id();

		self.tm.commit(|pending| {
			for p in pending {
				if version.is_none() {
					version = Some(p.version);
				}

				debug_assert_eq!(version.unwrap(), p.version);
				deltas.push(p.delta);
			}

			if let Some(version) = version {
				self.engine.multi.commit(deltas.clone(), version, transaction_id)?;
			}
			Ok(())
		})?;

		if let Some(version) = version {
			self.engine.event_bus.emit(PostCommitEvent {
				deltas,
				version,
			});
		}

		Ok(version.unwrap_or(CommitVersion(0)))
	}
}

impl<MVS: MultiVersionStore, SVT: SingleVersionTransaction> CommandTransaction<MVS, SVT> {
	pub fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.tm.read_as_of_version_exclusive(version);
	}

	pub fn read_as_of_version_inclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		self.read_as_of_version_exclusive(CommitVersion(version.0 + 1));
		Ok(())
	}

	pub fn rollback(&mut self) -> Result<(), Error> {
		self.tm.rollback()
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		let version = self.tm.version();
		match self.tm.contains_key(key)? {
			Some(true) => Ok(true),
			Some(false) => Ok(false),
			None => self.engine.multi.contains(key, version),
		}
	}

	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>, Error> {
		let version = self.tm.version();
		match self.tm.get(key)? {
			Some(v) => {
				if v.values().is_some() {
					Ok(Some(v.into()))
				} else {
					Ok(None)
				}
			}
			None => Ok(self.engine.multi.get(key, version)?.map(TransactionValue::from)),
		}
	}

	pub fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<(), Error> {
		self.tm.set(key, values)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
		self.tm.remove(key)
	}

	pub fn scan(&mut self) -> Result<TransactionScanIter<'_, MVS>, Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let pending = pw.iter();
		let commited = self.engine.multi.scan(version)?;

		Ok(TransactionScanIter::new(pending, commited, Some(marker)))
	}

	pub fn scan_rev(&mut self) -> Result<TransactionScanRevIter<'_, MVS>, Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let pending = pw.iter().rev();
		let commited = self.engine.multi.scan_rev(version)?;

		Ok(TransactionScanRevIter::new(pending, commited, Some(marker)))
	}

	pub fn range(&mut self, range: EncodedKeyRange) -> Result<TransactionRangeIter<'_, MVS>, Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();
		let pending = pw.range((start, end));
		let commited = self.engine.multi.range(range, version)?;

		Ok(TransactionRangeIter::new(pending, commited, Some(marker)))
	}

	pub fn range_rev(&mut self, range: EncodedKeyRange) -> Result<TransactionRangeRevIter<'_, MVS>, Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();
		let pending = pw.range((start, end));
		let commited = self.engine.multi.range_rev(range, version)?;

		Ok(TransactionRangeRevIter::new(pending.rev(), commited, Some(marker)))
	}

	pub fn prefix<'a>(&'a mut self, prefix: &EncodedKey) -> Result<TransactionRangeIter<'a, MVS>, Error> {
		self.range(EncodedKeyRange::prefix(prefix))
	}

	pub fn prefix_rev<'a>(&'a mut self, prefix: &EncodedKey) -> Result<TransactionRangeRevIter<'a, MVS>, Error> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
