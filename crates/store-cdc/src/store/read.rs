// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use tracing::instrument;

use crate::{
	storage::{CdcStorageResult, normalize_range_inclusive},
	store::CdcStore,
	types::Block,
};

impl CdcStore {
	#[instrument(name = "store::cdc::block_for", level = "trace", skip(self), fields(version = version.0))]
	pub(crate) fn block_for(&self, version: CommitVersion) -> CdcStorageResult<Option<Arc<Block>>> {
		if let Some(read) = self.read.as_ref()
			&& let Some(block) = read.block_containing(version)
		{
			return Ok((block.max_version() >= self.persistent.truncated_before()).then_some(block));
		}
		let Some(block) = self.persistent.load_block_containing(version)? else {
			return Ok(None);
		};
		if block.max_version() < self.persistent.truncated_before() {
			return Ok(None);
		}
		if let Some(read) = self.read.as_ref() {
			read.insert(Arc::clone(&block));
		}
		Ok(Some(block))
	}

	#[instrument(name = "store::cdc::next_available", level = "trace", skip(self), fields(from = from.0))]
	fn next_available(&self, from: CommitVersion) -> CdcStorageResult<Option<CommitVersion>> {
		Ok(self.persistent.summaries_from(from, 1)?.first().map(|summary| summary.min_version))
	}

	#[instrument(name = "store::cdc::bridge", level = "trace", skip(self, held), fields(from = from.0))]
	fn bridge(
		&self,
		from: CommitVersion,
		held: usize,
		boundary: Option<CommitVersion>,
	) -> CdcStorageResult<Option<CommitVersion>> {
		if held > 0 && from < self.persistent.truncated_before() {
			return Ok(None);
		}
		Ok(match (self.next_available(from)?, boundary) {
			(Some(next), Some(boundary)) => Some(next.min(boundary)),
			(next, boundary) => next.or(boundary),
		})
	}

	#[instrument(name = "store::cdc::holds_any", level = "trace", skip(self), fields(lo = lo.0, hi = hi.0))]
	fn holds_any(&self, lo: CommitVersion, hi: CommitVersion) -> CdcStorageResult<bool> {
		if lo > hi {
			return Ok(false);
		}
		if !self.commit.range(lo, hi, 1).is_empty() {
			return Ok(true);
		}
		let mut cursor = lo;
		while cursor <= hi {
			let Some(block) = self.block_for(cursor)? else {
				match self.next_available(cursor)? {
					Some(next) if next > cursor => cursor = next,
					_ => return Ok(false),
				}
				continue;
			};
			if block.entries.iter().any(|cdc| cdc.version >= cursor && cdc.version <= hi) {
				return Ok(true);
			}
			let Some(next) = block.max_version().0.checked_add(1) else {
				return Ok(false);
			};
			cursor = CommitVersion(next);
		}
		Ok(false)
	}

	pub(crate) fn walk_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		let want = batch_size as usize;
		let Some((lo, hi)) = normalize_range_inclusive(start, end) else {
			return Ok(CdcBatch {
				items: Vec::new(),
				has_more: false,
			});
		};
		if want == 0 {
			let has_more = self.holds_any(lo, hi)?;
			return Ok(CdcBatch {
				items: Vec::new(),
				has_more,
			});
		}

		let mut items: Vec<Cdc> = Vec::new();
		let mut cursor = lo;
		let mut exhausted = false;

		while !exhausted && items.len() < want && cursor <= hi {
			let pending = self.commit.range(cursor, hi, want - items.len());
			let boundary = pending.first().map(|cdc| cdc.version);

			while items.len() < want && cursor <= hi && boundary.is_none_or(|edge| cursor < edge) {
				let block = match self.block_for(cursor)? {
					Some(block) => block,
					None => match self.bridge(cursor, items.len(), boundary)? {
						Some(next) if next > cursor && next <= hi => {
							cursor = next;
							continue;
						}
						_ => {
							exhausted = true;
							break;
						}
					},
				};
				for cdc in block.entries.iter() {
					if cdc.version < cursor {
						continue;
					}
					if cdc.version > hi || items.len() == want {
						break;
					}
					if boundary.is_some_and(|edge| cdc.version >= edge) {
						break;
					}
					items.push((**cdc).clone());
				}
				let Some(next) = block.max_version().0.checked_add(1) else {
					exhausted = true;
					break;
				};
				cursor = CommitVersion(next);
			}

			if exhausted || items.len() >= want {
				break;
			}
			let Some(boundary) = boundary else {
				break;
			};
			if cursor < boundary {
				break;
			}

			let drained = items.len();
			for cdc in pending {
				if items.len() >= want || cdc.version > hi {
					break;
				}
				if cdc.version < cursor {
					continue;
				}
				if cdc.version > cursor && !items.is_empty() {
					break;
				}
				items.push((*cdc).clone());
				match cdc.version.0.checked_add(1) {
					Some(next) => cursor = CommitVersion(next),
					None => exhausted = true,
				}
			}
			if items.len() == drained {
				break;
			}
		}

		let has_more = match items.last() {
			Some(last) => match last.version.0.checked_add(1) {
				Some(next) => self.holds_any(CommitVersion(next), hi)?,
				None => false,
			},
			None => false,
		};
		Ok(CdcBatch {
			items,
			has_more,
		})
	}
}
