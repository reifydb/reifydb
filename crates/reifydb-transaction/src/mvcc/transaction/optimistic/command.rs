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
	CowVec, event::transaction::PostCommitEvent, row::EncodedRow,
};

use super::*;
use crate::mvcc::{
	transaction::{
		TransactionManagerCommand, iter::TransactionIter,
		iter_rev::TransactionIterRev, range::TransactionRange,
		range_rev::TransactionRangeRev, version::StdVersionProvider,
	},
	types::TransactionValue,
};

pub struct CommandTransaction<VS: VersionedStorage, UT: UnversionedTransaction>
{
	engine: Optimistic<VS, UT>,
	pub(crate) tm: TransactionManagerCommand<StdVersionProvider<UT>>,
}

impl<VS: VersionedStorage, UT: UnversionedTransaction>
	CommandTransaction<VS, UT>
{
	pub fn new(engine: Optimistic<VS, UT>) -> crate::Result<Self> {
		let tm = engine.tm.write()?;
		Ok(Self {
			engine,
			tm,
		})
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction>
	CommandTransaction<VS, UT>
{
	pub fn commit(
		&mut self,
	) -> Result<reifydb_core::Version, reifydb_core::Error> {
		let mut version: Option<Version> = None;
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
				self.engine.versioned.commit(
					deltas.clone(),
					version,
					transaction_id,
				)?;
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

impl<VS: VersionedStorage, UT: UnversionedTransaction>
	CommandTransaction<VS, UT>
{
	pub fn version(&self) -> Version {
		self.tm.version()
	}

	pub fn as_of_version(&mut self, version: Version) {
		self.tm.as_of_version(version);
	}

	pub fn rollback(&mut self) -> Result<(), reifydb_core::Error> {
		self.tm.rollback()
	}

	pub fn contains_key(
		&mut self,
		key: &EncodedKey,
	) -> Result<bool, reifydb_core::Error> {
		let version = self.tm.version();
		match self.tm.contains_key(key)? {
			Some(true) => Ok(true),
			Some(false) => Ok(false),
			None => self.engine.versioned.contains(key, version),
		}
	}

	pub fn get(
		&mut self,
		key: &EncodedKey,
	) -> Result<Option<TransactionValue>, reifydb_core::Error> {
		let version = self.tm.version();
		match self.tm.get(key)? {
			Some(v) => {
				if v.row().is_some() {
					Ok(Some(v.into()))
				} else {
					Ok(None)
				}
			}
			None => Ok(self
				.engine
				.versioned
				.get(key, version)?
				.map(TransactionValue::from)),
		}
	}

	pub fn set(
		&mut self,
		key: &EncodedKey,
		row: EncodedRow,
	) -> Result<(), reifydb_core::Error> {
		self.tm.set(key, row)
	}

	pub fn remove(
		&mut self,
		key: &EncodedKey,
	) -> Result<(), reifydb_core::Error> {
		self.tm.remove(key)
	}

	pub fn scan(
		&mut self,
	) -> Result<TransactionIter<'_, VS>, reifydb_core::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let pending = pw.iter();
		let commited = self.engine.versioned.scan(version)?;

		Ok(TransactionIter::new(pending, commited, Some(marker)))
	}

	pub fn scan_rev(
		&mut self,
	) -> Result<TransactionIterRev<'_, VS>, reifydb_core::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let pending = pw.iter().rev();
		let commited = self.engine.versioned.scan_rev(version)?;

		Ok(TransactionIterRev::new(pending, commited, Some(marker)))
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<TransactionRange<'_, VS>, reifydb_core::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();
		let pending = pw.range((start, end));
		let commited = self.engine.versioned.range(range, version)?;

		Ok(TransactionRange::new(pending, commited, Some(marker)))
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<TransactionRangeRev<'_, VS>, reifydb_core::Error> {
		let version = self.tm.version();
		let (marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();
		let pending = pw.range((start, end));
		let commited =
			self.engine.versioned.range_rev(range, version)?;

		Ok(TransactionRangeRev::new(
			pending.rev(),
			commited,
			Some(marker),
		))
	}

	pub fn prefix<'a>(
		&'a mut self,
		prefix: &EncodedKey,
	) -> Result<TransactionRange<'a, VS>, reifydb_core::Error> {
		self.range(EncodedKeyRange::prefix(prefix))
	}

	pub fn prefix_rev<'a>(
		&'a mut self,
		prefix: &EncodedKey,
	) -> Result<TransactionRangeRev<'a, VS>, reifydb_core::Error> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
