// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	encoded::row::EncodedRow,
	interface::{catalog::shape::ShapeId, store::EntryKind},
	key::row::RowKey,
	row::RowTtl,
};
use reifydb_type::{Result, util::cowvec::CowVec};

use super::ScanStats;
use crate::{
	hot::storage::HotStorage,
	tier::{RangeCursor, TierStorage},
};

/// A row identified as expired during scanning.
pub(crate) struct ExpiredRow {
	pub shape_id: ShapeId,
	pub key: CowVec<u8>,
	pub scanned_bytes: u64,
}

#[derive(Debug)]
pub(crate) enum ScanResult {
	Yielded,
	Exhausted,
}

/// Scan a single shape for rows whose TTL has expired.
///
/// Iterates all rows in the shape, reads the anchor timestamp from each
/// row's trailer, and returns those that have exceeded their TTL duration.
pub(crate) fn scan_shape_by_created_at(
	storage: &HotStorage,
	shape_id: ShapeId,
	ttl_config: &RowTtl,
	now_nanos: u64,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredRow>, ScanResult)> {
	let range = RowKey::full_scan(shape_id);
	let table = EntryKind::Source(shape_id);

	let start = bound_as_ref(&range.start);
	let end = bound_as_ref(&range.end);

	let mut expired = Vec::new();
	let mut batch_cursor = cursor.clone();
	let batch = storage.range_next(table, &mut batch_cursor, start, end, CommitVersion(u64::MAX), batch_size)?;

	for entry in &batch.entries {
		if let Some(ref value) = entry.value {
			let row = EncodedRow(value.clone());
			let anchor_nanos = row.created_at_nanos();
			assert!(
				anchor_nanos > 0,
				"Row is missing created_at timestamp — this is an invariant violation"
			);

			if now_nanos.saturating_sub(anchor_nanos) >= ttl_config.duration_nanos {
				expired.push(ExpiredRow {
					shape_id,
					key: entry.key.clone(),
					scanned_bytes: value.len() as u64,
				});
			}
		}
	}

	*cursor = batch_cursor;
	if !batch.has_more || cursor.exhausted {
		Ok((expired, ScanResult::Exhausted))
	} else {
		Ok((expired, ScanResult::Yielded))
	}
}

pub(crate) fn scan_shape_by_updated_at(
	storage: &HotStorage,
	shape_id: ShapeId,
	ttl_config: &RowTtl,
	now_nanos: u64,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredRow>, ScanResult)> {
	let range = RowKey::full_scan(shape_id);
	let table = EntryKind::Source(shape_id);

	let start = bound_as_ref(&range.start);
	let end = bound_as_ref(&range.end);

	let mut expired = Vec::new();
	let mut batch_cursor = cursor.clone();
	let batch = storage.range_next(table, &mut batch_cursor, start, end, CommitVersion(u64::MAX), batch_size)?;

	for entry in &batch.entries {
		if let Some(ref value) = entry.value {
			let row = EncodedRow(value.clone());
			let anchor_nanos = row.updated_at_nanos();
			assert!(
				anchor_nanos > 0,
				"Row is missing updated_at timestamp — this is an invariant violation"
			);

			if now_nanos.saturating_sub(anchor_nanos) >= ttl_config.duration_nanos {
				expired.push(ExpiredRow {
					shape_id,
					key: entry.key.clone(),
					scanned_bytes: value.len() as u64,
				});
			}
		}
	}

	*cursor = batch_cursor;
	if !batch.has_more || cursor.exhausted {
		Ok((expired, ScanResult::Exhausted))
	} else {
		Ok((expired, ScanResult::Yielded))
	}
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
pub(crate) fn drop_expired_keys(storage: &HotStorage, expired: &[ExpiredRow], stats: &mut ScanStats) -> Result<()> {
	if expired.is_empty() {
		return Ok(());
	}

	let mut drop_batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>> = HashMap::new();

	for row in expired {
		let table = EntryKind::Source(row.shape_id);
		let shape_bytes = stats.bytes_reclaimed.entry(row.shape_id).or_insert(0);
		let drop_batch = drop_batches.entry(table).or_default();

		let versions = storage.get_all_versions(table, &row.key)?;
		for (version, value) in &versions {
			if let Some(v) = value {
				*shape_bytes += v.len() as u64;
			}
			drop_batch.push((row.key.clone(), *version));
			stats.versions_dropped += 1;
		}
	}

	if !drop_batches.is_empty() {
		storage.drop(drop_batches)?;
	}

	Ok(())
}
