// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cold storage tier.
//!
//! Placeholder for future cold tier storage implementation.

use std::{collections::HashMap, ops::Bound};

use async_trait::async_trait;
use reifydb_type::Result;

use crate::tier::{RangeBatch, TableId, TierBackend, TierStorage};

/// Cold storage tier.
///
/// This is a placeholder enum with no variants yet.
/// Will be implemented when cold tier storage is needed.
#[derive(Clone)]
pub enum ColdStorage {}

#[async_trait]
impl TierStorage for ColdStorage {
	async fn get(&self, _table: TableId, _key: &[u8]) -> Result<Option<Vec<u8>>> {
		match *self {}
	}

	async fn set(&self, _batches: HashMap<TableId, Vec<(Vec<u8>, Option<Vec<u8>>)>>) -> Result<()> {
		match *self {}
	}

	async fn range_batch(
		&self,
		_table: TableId,
		_start: Bound<Vec<u8>>,
		_end: Bound<Vec<u8>>,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	async fn range_rev_batch(
		&self,
		_table: TableId,
		_start: Bound<Vec<u8>>,
		_end: Bound<Vec<u8>>,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	async fn ensure_table(&self, _table: TableId) -> Result<()> {
		match *self {}
	}

	async fn clear_table(&self, _table: TableId) -> Result<()> {
		match *self {}
	}
}

impl TierBackend for ColdStorage {}
