// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Warm storage tier.
//!
//! Placeholder for future warm tier storage implementation.

use std::{collections::HashMap, ops::Bound};

use reifydb_type::{Result, util::cowvec::CowVec};

use crate::tier::{EntryKind, RangeBatch, RangeCursor, TierBackend, TierStorage};

/// Warm storage tier.
///
/// This is a placeholder enum with no variants yet.
/// Will be implemented when warm tier storage is needed.
#[derive(Clone)]
pub enum WarmStorage {}

impl TierStorage for WarmStorage {
	fn get(&self, _table: EntryKind, _key: &[u8]) -> Result<Option<CowVec<u8>>> {
		match *self {}
	}

	fn set(&self, _batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>>) -> Result<()> {
		match *self {}
	}

	fn range_next(
		&self,
		_table: EntryKind,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
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

	fn drop(&self, _batches: HashMap<EntryKind, Vec<CowVec<u8>>>) -> Result<()> {
		match *self {}
	}
}

impl TierBackend for WarmStorage {}
