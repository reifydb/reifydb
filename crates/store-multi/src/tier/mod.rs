// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_type::{Result, util::cowvec::CowVec};

pub type TierBatch = HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>>;

#[derive(Debug, Clone)]
pub struct RawEntry {
	pub key: CowVec<u8>,
	pub version: CommitVersion,
	pub value: Option<CowVec<u8>>,
}

#[derive(Debug, Clone)]
pub struct RangeBatch {
	pub entries: Vec<RawEntry>,

	pub has_more: bool,
}

impl RangeBatch {
	pub fn empty() -> Self {
		Self {
			entries: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}
}

#[derive(Debug, Clone)]
pub struct RangeCursor {
	pub last_key: Option<CowVec<u8>>,

	pub exhausted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct HistoricalCursor {
	pub last_key: Option<CowVec<u8>>,
	pub last_version: Option<CommitVersion>,
	pub exhausted: bool,
}

impl HistoricalCursor {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

impl RangeCursor {
	pub fn new() -> Self {
		Self {
			last_key: None,
			exhausted: false,
		}
	}

	pub fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

impl Default for RangeCursor {
	fn default() -> Self {
		Self::new()
	}
}

pub trait TierStorage: Send + Sync + Clone + 'static {
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>>;

	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		Ok(self.get(table, key, version)?.is_some())
	}

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()>;

	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch>;

	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch>;

	fn ensure_table(&self, table: EntryKind) -> Result<()>;

	fn clear_table(&self, table: EntryKind) -> Result<()>;

	fn drop(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()>;

	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>>;

	fn scan_historical_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		cursor: &mut HistoricalCursor,
		batch_size: usize,
	) -> Result<Vec<(CowVec<u8>, CommitVersion)>>;
}

pub trait TierBackend: TierStorage {}
