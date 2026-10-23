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
	fn get(&self, table: EntryKind, keys: &[&[u8]], version: CommitVersion) -> Result<Vec<VersionedGetResult>>;

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
}

impl TierStorage for CommitStore {
	fn get(&self, table: EntryKind, keys: &[&[u8]], version: CommitVersion) -> Result<Vec<VersionedGetResult>> {
		Ok(keys.iter().map(|&key| CommitStore::get(self, table, key, version)).collect())
	}

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		CommitStore::set(self, version, batches);
		Ok(())
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
		Ok(CommitStore::range_next(self, table, cursor, start, end, scope, batch_size))
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
		Ok(CommitStore::range_rev_next(self, table, cursor, start, end, scope, batch_size))
	}

	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		CommitStore::ensure_table(self, table);
		Ok(())
	}
}
