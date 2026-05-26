// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{catalog::flow::FlowNodeId, store::EntryKind},
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
	row::{Ttl, TtlAnchor},
};
use reifydb_type::Result;

use super::OperatorScanStats;
use crate::{
	buffer::tier::MultiBufferTier,
	gc::row::scanner::ScanResult,
	tier::{RangeCursor, TierStorage},
};

pub struct ExpiredOperatorState {
	pub node_id: FlowNodeId,
	pub key: EncodedKey,
	pub scanned_bytes: u64,
}

pub fn scan_operator_by_created_at(
	storage: &MultiBufferTier,
	node_id: FlowNodeId,
	ttl: &Ttl,
	now_nanos: u64,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredOperatorState>, ScanResult)> {
	let range = FlowNodeStateKey::node_range(node_id);
	let table = EntryKind::Operator(node_id);

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
				"Operator-state row is missing created_at timestamp - this is an invariant violation"
			);

			if now_nanos.saturating_sub(anchor_nanos) >= ttl.duration_nanos {
				expired.push(ExpiredOperatorState {
					node_id,
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

pub fn scan_operator_by_updated_at(
	storage: &MultiBufferTier,
	node_id: FlowNodeId,
	ttl: &Ttl,
	now_nanos: u64,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredOperatorState>, ScanResult)> {
	let range = FlowNodeStateKey::node_range(node_id);
	let table = EntryKind::Operator(node_id);

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
				"Operator-state row is missing updated_at timestamp - this is an invariant violation"
			);

			if now_nanos.saturating_sub(anchor_nanos) >= ttl.duration_nanos {
				expired.push(ExpiredOperatorState {
					node_id,
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

pub(crate) const JOIN_LEFT_PREFIX: u8 = 0x01;
pub(crate) const JOIN_RIGHT_PREFIX: u8 = 0x02;

pub fn scan_operator_join(
	storage: &MultiBufferTier,
	node_id: FlowNodeId,
	left: Option<&Ttl>,
	right: Option<&Ttl>,
	now_nanos: u64,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredOperatorState>, ScanResult)> {
	let range = FlowNodeStateKey::node_range(node_id);
	let table = EntryKind::Operator(node_id);

	let start = bound_as_ref(&range.start);
	let end = bound_as_ref(&range.end);

	let mut expired = Vec::new();
	let mut batch_cursor = cursor.clone();
	let batch = storage.range_next(table, &mut batch_cursor, start, end, CommitVersion(u64::MAX), batch_size)?;

	for entry in &batch.entries {
		let Some(ref value) = entry.value else {
			continue;
		};

		let side_prefix = FlowNodeStateKey::decode(&entry.key).and_then(|k| k.key.first().copied());
		let ttl = match side_prefix {
			Some(JOIN_LEFT_PREFIX) => left,
			Some(JOIN_RIGHT_PREFIX) => right,
			_ => None,
		};
		let Some(ttl) = ttl else {
			continue;
		};

		let row = EncodedRow(value.clone());
		let anchor_nanos = match ttl.anchor {
			TtlAnchor::Created => row.created_at_nanos(),
			TtlAnchor::Updated => row.updated_at_nanos(),
		};
		assert!(
			anchor_nanos > 0,
			"Join-state row is missing its TTL anchor timestamp - this is an invariant violation"
		);

		if now_nanos.saturating_sub(anchor_nanos) >= ttl.duration_nanos {
			expired.push(ExpiredOperatorState {
				node_id,
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

pub fn drop_expired_operator_keys(
	storage: &MultiBufferTier,
	expired: &[ExpiredOperatorState],
	stats: &mut OperatorScanStats,
) -> Result<()> {
	if expired.is_empty() {
		return Ok(());
	}

	let mut drop_batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();

	for row in expired {
		let table = EntryKind::Operator(row.node_id);
		let node_bytes = stats.bytes_reclaimed.entry(row.node_id).or_insert(0);
		let drop_batch = drop_batches.entry(table).or_default();

		let versions = storage.get_all_versions(table, &row.key)?;
		for (version, value) in &versions {
			if let Some(v) = value {
				*node_bytes += v.len() as u64;
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

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_core::{
		common::CommitVersion,
		encoded::row::SHAPE_HEADER_SIZE,
		interface::{catalog::flow::FlowNodeId, store::EntryKind},
		key::{flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
		row::{Ttl, TtlAnchor, TtlCleanupMode},
	};
	use reifydb_type::util::cowvec::CowVec;

	use super::*;
	use crate::{buffer::tier::MultiBufferTier, tier::TierStorage};

	fn row_with_created(payload: &[u8], created_at: u64) -> CowVec<u8> {
		row_with(payload, created_at, created_at)
	}

	fn row_with(payload: &[u8], created_at: u64, updated_at: u64) -> CowVec<u8> {
		let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
		buf[8..16].copy_from_slice(&created_at.to_le_bytes());
		buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
		buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
		CowVec::new(buf)
	}

	#[test]
	fn scan_drops_expired_data_state_but_never_internal_state() {
		let storage = MultiBufferTier::memory();
		let node = FlowNodeId(1);
		let table = EntryKind::Operator(node);

		let old_data = FlowNodeStateKey::encoded(node, vec![1u8]);
		let fresh_data = FlowNodeStateKey::encoded(node, vec![2u8]);
		// An OLD internal-state row (e.g. a row-number mapping). It must stay immune even though
		// its anchor is well past the TTL, because operator GC only scans the data-state range.
		let old_internal = FlowNodeInternalStateKey::encoded(node, vec![9u8]);

		storage.set(
			CommitVersion(1),
			HashMap::from([(
				table,
				vec![
					(old_data.clone(), Some(row_with_created(b"old", 1))),
					(fresh_data.clone(), Some(row_with_created(b"new", 10_000))),
					(old_internal.clone(), Some(row_with_created(b"map", 1))),
				],
			)]),
		)
		.unwrap();

		let ttl = Ttl {
			duration_nanos: 100,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let now = 1_000;

		let mut cursor = RangeCursor::default();
		let (expired, _) = scan_operator_by_created_at(&storage, node, &ttl, now, 4096, &mut cursor).unwrap();

		// Exactly the old data-state row is expired: the fresh data row is within TTL, and the
		// old internal-state row is outside the data-state scan range entirely.
		assert_eq!(expired.len(), 1, "only the old data-state row should be expired");
		assert_eq!(expired[0].key, old_data);

		let mut stats = OperatorScanStats::default();
		drop_expired_operator_keys(&storage, &expired, &mut stats).unwrap();

		// The internal-state row survives the drop - immune to operator GC.
		let internal_versions = storage.get_all_versions(table, old_internal.as_ref()).unwrap();
		assert!(
			internal_versions.iter().any(|(_, v)| v.is_some()),
			"internal state must never be reclaimed by the operator GC"
		);

		// Re-scanning finds nothing: the expired data row is gone, the fresh one stays.
		let mut cursor = RangeCursor::default();
		let (expired_after, _) =
			scan_operator_by_created_at(&storage, node, &ttl, now, 4096, &mut cursor).unwrap();
		assert!(expired_after.is_empty(), "the expired data-state row should have been dropped");
	}

	#[test]
	fn join_scan_evicts_per_side_and_never_touches_schema_rows() {
		let storage = MultiBufferTier::memory();
		let node = FlowNodeId(2);
		let table = EntryKind::Operator(node);

		// Left side (0x01) and right side (0x02) each with an old and a fresh row, plus the
		// per-side schema rows (0x03 left, 0x04 right) which carry no TTL and must survive.
		let left_old = FlowNodeStateKey::encoded(node, vec![JOIN_LEFT_PREFIX, 1]);
		let left_fresh = FlowNodeStateKey::encoded(node, vec![JOIN_LEFT_PREFIX, 2]);
		let right_old = FlowNodeStateKey::encoded(node, vec![JOIN_RIGHT_PREFIX, 1]);
		let right_fresh = FlowNodeStateKey::encoded(node, vec![JOIN_RIGHT_PREFIX, 2]);
		let left_schema = FlowNodeStateKey::encoded(node, vec![0x03u8]);
		let right_schema = FlowNodeStateKey::encoded(node, vec![0x04u8]);

		storage.set(
			CommitVersion(1),
			HashMap::from([(
				table,
				vec![
					(left_old.clone(), Some(row_with_created(b"lo", 1))),
					(left_fresh.clone(), Some(row_with_created(b"lf", 10_000))),
					(right_old.clone(), Some(row_with_created(b"ro", 1))),
					(right_fresh.clone(), Some(row_with_created(b"rf", 10_000))),
					(left_schema.clone(), Some(row_with_created(b"ls", 1))),
					(right_schema.clone(), Some(row_with_created(b"rs", 1))),
				],
			)]),
		)
		.unwrap();

		let ttl = Ttl {
			duration_nanos: 100,
			anchor: TtlAnchor::Updated,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let now = 1_000;

		// Both sides configured: each side's old row expires; fresh rows and schema rows survive.
		let mut cursor = RangeCursor::default();
		let (expired, _) =
			scan_operator_join(&storage, node, Some(&ttl), Some(&ttl), now, 4096, &mut cursor).unwrap();
		let keys: Vec<&EncodedKey> = expired.iter().map(|e| &e.key).collect();
		assert_eq!(expired.len(), 2, "exactly the two old side rows expire");
		assert!(keys.contains(&&left_old) && keys.contains(&&right_old));
		assert!(!keys.contains(&&left_fresh) && !keys.contains(&&right_fresh), "fresh rows survive");
		assert!(
			!keys.contains(&&left_schema) && !keys.contains(&&right_schema),
			"schema rows are never scanned"
		);

		// Asymmetric: only the left side has a TTL -> only the old left row is eligible.
		let mut cursor = RangeCursor::default();
		let (expired_left_only, _) =
			scan_operator_join(&storage, node, Some(&ttl), None, now, 4096, &mut cursor).unwrap();
		assert_eq!(expired_left_only.len(), 1);
		assert_eq!(expired_left_only[0].key, left_old);
	}

	#[test]
	fn join_scan_respects_the_configured_anchor() {
		// A row created long ago but updated recently must be evicted under a Created anchor and
		// kept under an Updated anchor - the scan must honor the per-side anchor, not force one.
		let storage = MultiBufferTier::memory();
		let node = FlowNodeId(3);
		let table = EntryKind::Operator(node);
		let left = FlowNodeStateKey::encoded(node, vec![JOIN_LEFT_PREFIX, 1]);
		storage.set(
			CommitVersion(1),
			HashMap::from([(table, vec![(left.clone(), Some(row_with(b"l", 1, 10_000)))])]),
		)
		.unwrap();
		let now = 1_000;
		let created = Ttl {
			duration_nanos: 100,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let updated = Ttl {
			anchor: TtlAnchor::Updated,
			..created.clone()
		};

		let mut cursor = RangeCursor::default();
		let (created_expired, _) =
			scan_operator_join(&storage, node, Some(&created), None, now, 4096, &mut cursor).unwrap();
		assert_eq!(created_expired.len(), 1, "Created anchor: an old created_at must expire");

		let mut cursor = RangeCursor::default();
		let (updated_expired, _) =
			scan_operator_join(&storage, node, Some(&updated), None, now, 4096, &mut cursor).unwrap();
		assert!(updated_expired.is_empty(), "Updated anchor: a fresh updated_at must survive");
	}
}
