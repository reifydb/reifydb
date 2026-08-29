// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod commit;
pub mod persistent;
pub mod point;
pub mod range;

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::store::EntryKind, key::typed::MultiKey};
use reifydb_store::coverage::cursor::{Cursor, ScannedStop};
use reifydb_value::{Result, util::cowvec::CowVec};

use crate::MultiVersionScope;

pub type DisplacedValues = Vec<(EncodedKey, u64)>;

pub type TierBatch = HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>>;

#[derive(Debug, Clone)]
pub enum VersionedGetResult {
	Value {
		value: CowVec<u8>,
		version: CommitVersion,
	},

	Tombstone,

	NotFound,
}

impl VersionedGetResult {
	pub fn value(self) -> Option<CowVec<u8>> {
		match self {
			VersionedGetResult::Value {
				value,
				..
			} => Some(value),
			VersionedGetResult::Tombstone | VersionedGetResult::NotFound => None,
		}
	}
}

#[derive(Debug, Clone)]
pub struct RawEntry {
	pub key: EncodedKey,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeStop {
	Scanned,

	AbsentTable,
}

pub type RangeCursor = Cursor<RangeStop, MultiKey>;

impl ScannedStop for RangeStop {
	fn scanned(&self) -> bool {
		matches!(self, RangeStop::Scanned)
	}
}

#[derive(Debug, Clone, Default)]
pub struct HistoricalCursor {
	pub last_key: Option<EncodedKey>,
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

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<DisplacedValues>;

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

pub trait TierBackend: TierStorage {}
