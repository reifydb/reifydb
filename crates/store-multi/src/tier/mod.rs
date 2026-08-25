// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod commit;
pub mod persistent;
pub mod read;

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
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

/// Why a tier stopped a range scan, which is a different question from whether it stopped.
///
/// Only [`RangeStop::Scanned`] is a statement about the range itself: the tier read it and found nothing
/// beyond what it returned. The other two end the scan without having read the span at all, so a coverage
/// claim taken from such a chunk would answer for keys nothing ever examined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeStop {
	/// The tier read to the end of the range.
	Scanned,

	/// The tier holds no table for this entry kind, so it read nothing.
	AbsentTable,

	/// The tier is shut down and its readers are gone, so it read nothing.
	ShutDown,
}

#[derive(Debug, Clone)]
pub struct RangeCursor {
	pub last_key: Option<EncodedKey>,

	pub exhausted: bool,

	/// Set by the tier that exhausted this cursor, and left as none by a tier that has no answer.
	///
	/// None refuses a coverage claim, so a tier that stops without naming a reason understates rather
	/// than answering for a span it never read.
	pub stop: Option<RangeStop>,
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

impl RangeCursor {
	pub fn new() -> Self {
		Self {
			last_key: None,
			exhausted: false,
			stop: None,
		}
	}

	pub fn is_exhausted(&self) -> bool {
		self.exhausted
	}

	/// Whether this cursor stopped because a tier read the range to its end, which is the only stop a
	/// coverage claim may be taken from.
	pub fn scanned_to_end(&self) -> bool {
		self.exhausted && self.stop == Some(RangeStop::Scanned)
	}
}

impl Default for RangeCursor {
	fn default() -> Self {
		Self::new()
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
