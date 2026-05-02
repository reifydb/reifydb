// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_type::{Result, util::cowvec::CowVec};

use crate::tier::{HistoricalCursor, RangeBatch, RangeCursor, TierBackend, TierStorage};

/// Cold storage tier.
///
/// This is a placeholder enum with no variants yet.
/// Will be implemented when cold tier storage is needed.
#[derive(Clone)]
pub enum ColdStorage {}

impl TierStorage for ColdStorage {
	fn get(&self, _table: EntryKind, _key: &[u8], _version: CommitVersion) -> Result<Option<CowVec<u8>>> {
		match *self {}
	}

	fn set(
		&self,
		_version: CommitVersion,
		_batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>>,
	) -> Result<()> {
		match *self {}
	}

	fn range_next(
		&self,
		_table: EntryKind,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_version: CommitVersion,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	fn range_rev_next(
		&self,
		_table: EntryKind,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_version: CommitVersion,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	fn ensure_table(&self, _table: EntryKind) -> Result<()> {
		match *self {}
	}

	fn clear_table(&self, _table: EntryKind) -> Result<()> {
		match *self {}
	}

	fn drop(&self, _batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
		match *self {}
	}

	fn get_all_versions(&self, _table: EntryKind, _key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		match *self {}
	}

	fn scan_historical_below(
		&self,
		_table: EntryKind,
		_cutoff: CommitVersion,
		_cursor: &mut HistoricalCursor,
		_batch_size: usize,
	) -> Result<Vec<(CowVec<u8>, CommitVersion)>> {
		match *self {}
	}
}

impl TierBackend for ColdStorage {}
