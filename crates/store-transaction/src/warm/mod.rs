// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Warm storage tier.
//!
//! Placeholder for future warm tier storage implementation.

use std::{collections::HashMap, ops::Bound};

use async_trait::async_trait;
use reifydb_type::Result;

use crate::tier::{RangeBatch, RangeCursor, Store, TierBackend, TierStorage};

/// Warm storage tier.
///
/// This is a placeholder enum with no variants yet.
/// Will be implemented when warm tier storage is needed.
#[derive(Clone)]
pub enum WarmStorage {}

#[async_trait]
impl TierStorage for WarmStorage {
	async fn get(&self, _table: Store, _key: &[u8]) -> Result<Option<Vec<u8>>> {
		match *self {}
	}

	async fn set(&self, _batches: HashMap<Store, Vec<(Vec<u8>, Option<Vec<u8>>)>>) -> Result<()> {
		match *self {}
	}

	async fn range_next(
		&self,
		_table: Store,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	async fn range_rev_next(
		&self,
		_table: Store,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	async fn ensure_table(&self, _table: Store) -> Result<()> {
		match *self {}
	}

	async fn clear_table(&self, _table: Store) -> Result<()> {
		match *self {}
	}
}

impl TierBackend for WarmStorage {}
