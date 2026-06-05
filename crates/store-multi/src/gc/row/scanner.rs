// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::{catalog::shape::ShapeId, store::EntryKind},
	key::row::RowKey,
};
use reifydb_value::Result;

use super::ScanStats;
use crate::{
	MultiVersionScope,
	tier::{RangeCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};

pub struct ExpiredRow {
	pub shape_id: ShapeId,
	pub key: EncodedKey,
	pub scanned_bytes: u64,
}

#[derive(Debug)]
pub enum ScanResult {
	Yielded,
	Exhausted,
}

pub fn scan_shape_expired(
	storage: &MultiCommitBufferTier,
	shape_id: ShapeId,
	cutoff_version: CommitVersion,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredRow>, ScanResult)> {
	let range = RowKey::full_scan(shape_id);
	let table = EntryKind::Source(shape_id);

	let start = bound_as_ref(&range.start);
	let end = bound_as_ref(&range.end);

	let mut expired = Vec::new();
	let mut batch_cursor = cursor.clone();
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(u64::MAX),
	};
	let batch = storage.range_next(table, &mut batch_cursor, start, end, scope, batch_size)?;

	for entry in &batch.entries {
		if let Some(ref value) = entry.value
			&& entry.version <= cutoff_version
		{
			expired.push(ExpiredRow {
				shape_id,
				key: entry.key.clone(),
				scanned_bytes: value.len() as u64,
			});
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

// TODO: batch version lookups - currently O(N) individual get_all_versions

pub fn drop_expired_keys(storage: &MultiCommitBufferTier, expired: &[ExpiredRow], stats: &mut ScanStats) -> Result<()> {
	if expired.is_empty() {
		return Ok(());
	}

	let mut drop_batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();

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
