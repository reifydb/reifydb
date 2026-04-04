// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	encoded::row::EncodedRow,
	interface::catalog::shape::ShapeId,
	key::row::RowKey,
	row::{RowTtl, RowTtlAnchor},
};
use reifydb_type::{Result, util::cowvec::CowVec};

use super::stats::GcScanStats;
use crate::{
	hot::storage::HotStorage,
	tier::{EntryKind, RangeCursor, TierStorage},
};

/// A row identified as expired during scanning.
pub(crate) struct ExpiredRow {
	pub key: CowVec<u8>,
}

/// Scan a single shape for rows whose TTL has expired.
///
/// Iterates all rows in the shape, reads the anchor timestamp from each
/// row's trailer, and returns those that have exceeded their TTL duration.
pub(crate) fn scan_shape_for_expired(
	storage: &HotStorage,
	shape_id: ShapeId,
	ttl_config: &RowTtl,
	now_nanos: u64,
	batch_size: usize,
) -> Result<Vec<ExpiredRow>> {
	let range = RowKey::full_scan(shape_id);
	let table = EntryKind::Source(shape_id);

	let start = bound_as_ref(&range.start);
	let end = bound_as_ref(&range.end);

	let mut cursor = RangeCursor::new();
	let mut expired = Vec::new();

	loop {
		let batch = storage.range_next(table, &mut cursor, start, end, CommitVersion(u64::MAX), batch_size)?;

		for entry in &batch.entries {
			let Some(ref value) = entry.value else {
				// Tombstone — skip
				continue;
			};

			let row = EncodedRow(value.clone());
			let anchor_nanos = match ttl_config.anchor {
				RowTtlAnchor::Created => row.created_at_nanos(),
				RowTtlAnchor::Updated => row.updated_at_nanos(),
			};

			// Skip rows without timestamps (pre-timestamp era)
			if anchor_nanos == 0 {
				continue;
			}

			if now_nanos.saturating_sub(anchor_nanos) >= ttl_config.duration_nanos {
				expired.push(ExpiredRow {
					key: entry.key.clone(),
				});
			}
		}

		if !batch.has_more || cursor.is_exhausted() {
			break;
		}
	}

	Ok(expired)
}

fn bound_as_ref(bound: &Bound<impl AsRef<[u8]>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_ref()),
		Bound::Excluded(v) => Bound::Excluded(v.as_ref()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

/// Drop all versions of expired rows from storage.
///
/// For each expired row key, fetches all stored versions and physically
/// removes them. Returns stats about the operation.
// TODO: batch version lookups — currently O(N) individual get_all_versions
// calls for N expired rows. Consider a bulk API when large TTL bursts occur.
pub(crate) fn drop_expired_keys(
	storage: &HotStorage,
	shape_id: ShapeId,
	expired: &[ExpiredRow],
	stats: &mut GcScanStats,
) -> Result<()> {
	if expired.is_empty() {
		return Ok(());
	}

	let table = EntryKind::Source(shape_id);
	let mut drop_batch: Vec<(CowVec<u8>, CommitVersion)> = Vec::new();

	let shape_bytes = stats.bytes_reclaimed.entry(shape_id).or_insert(0);

	for row in expired {
		let versions = storage.get_all_versions(table, &row.key)?;
		for (version, value) in &versions {
			if let Some(v) = value {
				*shape_bytes += v.len() as u64;
			}
			drop_batch.push((row.key.clone(), *version));
			stats.versions_dropped += 1;
		}
	}

	if !drop_batch.is_empty() {
		let mut batches = HashMap::new();
		batches.insert(table, drop_batch);
		storage.drop(batches)?;
	}

	Ok(())
}
