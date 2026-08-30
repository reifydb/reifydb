// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod persistent;
pub mod point;
pub mod range;

use std::ops::Bound;

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_store_commit::{
	MultiVersionScope, RangeBatch, RangeCursor, TierBatch, VersionedGetResult, store::CommitStore,
};
use reifydb_value::Result;

pub trait TierStorage: Send + Sync + Clone + 'static {
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult>;

	fn get_many(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		let mut out = Vec::with_capacity(keys.len());
		for &key in keys {
			out.push(self.get(table, key, version)?);
		}
		Ok(out)
	}

	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		Ok(matches!(self.get(table, key, version)?, VersionedGetResult::Value { .. }))
	}

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()>;

	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch>;

	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch>;

	fn ensure_table(&self, table: EntryKind) -> Result<()>;

	fn clear_table(&self, table: EntryKind) -> Result<()>;
}

impl TierStorage for CommitStore {
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		CommitStore::get(self, table, key, version)
	}

	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		CommitStore::contains(self, table, key, version)
	}

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		CommitStore::set(self, version, batches)
	}

	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		CommitStore::range_next(self, table, cursor, start, end, scope, batch_size)
	}

	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		CommitStore::range_rev_next(self, table, cursor, start, end, scope, batch_size)
	}

	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		CommitStore::ensure_table(self, table)
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		CommitStore::clear_table(self, table)
	}
}
