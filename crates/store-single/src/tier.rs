// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_value::{Result, util::cowvec::CowVec};

#[derive(Debug, Clone)]
pub struct RawEntry {
	pub key: EncodedKey,
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
	pub last_key: Option<EncodedKey>,

	pub exhausted: bool,
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
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>>;

	fn contains(&self, key: &[u8]) -> Result<bool> {
		Ok(self.get(key)?.is_some())
	}

	fn get_with_tombstone(&self, key: &[u8]) -> Result<Option<Option<CowVec<u8>>>>;

	fn set(&self, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> Result<()>;

	fn range_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch>;

	fn range_rev_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch>;

	fn ensure_table(&self) -> Result<()>;

	fn clear_table(&self) -> Result<()>;
}

pub trait TierBackend: TierStorage {}
