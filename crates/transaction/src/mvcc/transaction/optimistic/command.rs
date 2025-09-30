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
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange,
	event::transaction::PostCommitEvent,
	interface::{MultiVersionStorage, SingleVersionTransaction},
	value::row::EncodedRow,
};
use reifydb_type::Error;

use super::*;
use crate::mvcc::{
	transaction::{
		TransactionManagerCommand, iter::TransactionIter, iter_rev::TransactionIterRev,
		range::TransactionRange, range_rev::TransactionRangeRev, version::StdVersionProvider,
	},
	types::TransactionValue,
};

pub struct CommandTransaction<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> {
	engine: Optimistic<MVS, SMVT>,
	pub(crate) tm: TransactionManagerCommand<StdVersionProvider<SMVT>>,
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> CommandTransaction<MVS, SMVT> {
	pub fn new(engine: Optimistic<MVS, SMVT>) -> crate::Result<Self> {
		let tm = engine.tm.write()?;
		Ok(Self {
			engine,
			tm,
		})
	}
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> CommandTransaction<MVS, SMVT> {
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

		Ok(version.unwrap_or(0))
	}
}

impl<MVS: MultiVersionStorage, SMVT: SingleVersionTransaction> CommandTransaction<MVS, SMVT> {
	pub fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.tm.read_as_of_version_exclusive(version);
	}

	pub fn read_as_of_version_inclusive(&mut self, version: CommitVersion) -> Result<(), reifydb_type::Error> {
		self.read_as_of_version_exclusive(version + 1);
		Ok(())
	}

	pub fn rollback(&mut self) -> Result<(), reifydb_type::Error> {
		self.tm.rollback()
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, reifydb_type::Error> {
		let version = self.tm.version();
		match self.tm.contains_key(key)? {
			Some(true) => Ok(true),
			Some(false) => Ok(false),
			None => self.engine.multi.contains(key, version),
		}
	}

	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>, reifydb_type::Error> {
		let version = self.tm.version();
		match self.tm.get(key)? {
			Some(v) => {
				if v.row().is_some() {
					Ok(Some(v.into()))
				} else {
					Ok(None)
				}
			}
			None => Ok(self.engine.multi.get(key, version)?.map(TransactionValue::from)),
		}
	}

	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), reifydb_type::Error> {
		self.tm.set(key, row)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<(), reifydb_type::Error> {
		self.tm.remove(key)
	}

	pub fn scan(&mut self) -> Result<TransactionIter<'_, MVS>, reifydb_type::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let pending = pw.iter();
		let commited = self.engine.multi.scan(version)?;

		Ok(TransactionIter::new(pending, commited, Some(marker)))
	}

	pub fn scan_rev(&mut self) -> Result<TransactionIterRev<'_, MVS>, reifydb_type::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let pending = pw.iter().rev();
		let commited = self.engine.multi.scan_rev(version)?;

		Ok(TransactionIterRev::new(pending, commited, Some(marker)))
	}

	pub fn range(&mut self, range: EncodedKeyRange) -> Result<TransactionRange<'_, MVS>, reifydb_type::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();
		let pending = pw.range((start, end));
		let commited = self.engine.multi.range(range, version)?;

		Ok(TransactionRange::new(pending, commited, Some(marker)))
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<TransactionRangeRev<'_, MVS>, reifydb_type::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();
		let pending = pw.range((start, end));
		let commited = self.engine.multi.range_rev(range, version)?;

		Ok(TransactionRangeRev::new(pending.rev(), commited, Some(marker)))
	}

	pub fn prefix<'a>(&'a mut self, prefix: &EncodedKey) -> Result<TransactionRange<'a, MVS>, reifydb_type::Error> {
		self.range(EncodedKeyRange::prefix(prefix))
	}

	pub fn prefix_rev<'a>(
		&'a mut self,
		prefix: &EncodedKey,
	) -> Result<TransactionRangeRev<'a, MVS>, reifydb_type::Error> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
