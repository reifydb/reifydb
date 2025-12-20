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
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRange, MultiVersionRangeRev,
};
use reifydb_type::{Error, util::hex};
use tracing::instrument;

use super::{
	Transaction, TransactionManagerCommand, range::TransactionRangeIter, range_rev::TransactionRangeRevIter,
	version::StandardVersionProvider,
};
use crate::multi::types::TransactionValue;

pub struct CommandTransaction {
	engine: Transaction,
	pub(crate) tm: TransactionManagerCommand<StandardVersionProvider>,
}

impl CommandTransaction {
	#[instrument(name = "transaction::command::new", level = "debug", skip(engine))]
	pub fn new(engine: Transaction) -> crate::Result<Self> {
		let tm = engine.tm.write()?;
		Ok(Self {
			engine,
			tm,
		})
	}
}

impl CommandTransaction {
	#[instrument(name = "transaction::command::commit", level = "info", skip(self), fields(pending_count = self.tm.pending_writes().len()))]
	pub fn commit(&mut self) -> Result<CommitVersion, Error> {
		let mut version: Option<CommitVersion> = None;
		let mut deltas = CowVec::with_capacity(8);

		self.tm.commit(|pending| {
			for p in pending {
				if version.is_none() {
					version = Some(p.version);
				}

				debug_assert_eq!(version.unwrap(), p.version);
				deltas.push(p.delta);
			}

			if let Some(version) = version {
				self.engine.store.commit(deltas.clone(), version)?;
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

impl CommandTransaction {
	pub fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	pub fn pending_writes(&self) -> &crate::multi::pending::PendingWrites {
		self.tm.pending_writes()
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.tm.read_as_of_version_exclusive(version);
	}

	pub fn read_as_of_version_inclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		self.read_as_of_version_exclusive(CommitVersion(version.0 + 1));
		Ok(())
	}

	#[instrument(name = "transaction::command::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> Result<(), Error> {
		self.tm.rollback()
	}

	#[instrument(name = "transaction::command::contains_key", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, reifydb_type::Error> {
		let version = self.tm.version();
		match self.tm.contains_key(key)? {
			Some(true) => Ok(true),
			Some(false) => Ok(false),
			None => self.engine.store.contains(key, version),
		}
	}

	#[instrument(name = "transaction::command::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
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
			None => Ok(self.engine.store.get(key, version)?.map(Into::into)),
		}
	}

	#[instrument(name = "transaction::command::set", level = "trace", skip(self, values), fields(key_hex = %hex::encode(key.as_ref()), value_len = values.as_ref().len()))]
	pub fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<(), reifydb_type::Error> {
		self.tm.set(key, values)
	}

	#[instrument(name = "transaction::command::remove", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<(), reifydb_type::Error> {
		self.tm.remove(key)
	}

	pub fn range_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<TransactionRangeIter<'_, TransactionStore>, reifydb_type::Error> {
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());
		let pending = pw.range((start, end));
		let commited = self.engine.store.range_batched(range, version, batch_size)?;

		Ok(TransactionRangeIter::new(pending, commited, Some(marker)))
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<TransactionRangeIter<'_, TransactionStore>, reifydb_type::Error> {
		self.range_batched(range, 1024)
	}

	pub fn range_rev_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<TransactionRangeRevIter<'_, TransactionStore>, reifydb_type::Error> {
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());
		let pending = pw.range((start, end));
		let commited = self.engine.store.range_rev_batched(range, version, batch_size)?;

		Ok(TransactionRangeRevIter::new(pending.rev(), commited, Some(marker)))
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<TransactionRangeRevIter<'_, TransactionStore>, reifydb_type::Error> {
		self.range_rev_batched(range, 1024)
	}

	pub fn prefix<'a>(
		&'a mut self,
		prefix: &EncodedKey,
	) -> Result<TransactionRangeIter<'a, TransactionStore>, reifydb_type::Error> {
		self.range(EncodedKeyRange::prefix(prefix))
	}

	pub fn prefix_rev<'a>(
		&'a mut self,
		prefix: &EncodedKey,
	) -> Result<TransactionRangeRevIter<'a, TransactionStore>, reifydb_type::Error> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}

use reifydb_store_transaction::TransactionStore;
