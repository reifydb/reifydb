// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::FlowNodeId, store::EntryKind},
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_value::Result;

use super::OperatorScanMetrics;
use crate::{
	MultiVersionScope,
	gc::ScanResult,
	tier::{RangeCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};

pub struct ExpiredOperatorState {
	pub node_id: FlowNodeId,
	pub key: EncodedKey,
	pub version: CommitVersion,
	pub scanned_bytes: u64,
}

pub fn scan_operator_expired(
	storage: &MultiCommitBufferTier,
	node_id: FlowNodeId,
	cutoff_version: CommitVersion,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredOperatorState>, ScanResult)> {
	let range = FlowNodeStateKey::node_range(node_id);
	let table = EntryKind::Operator(node_id);

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
			expired.push(ExpiredOperatorState {
				node_id,
				key: entry.key.clone(),
				version: entry.version,
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

pub(crate) const JOIN_LEFT_PREFIX: u8 = 0x01;
pub(crate) const JOIN_RIGHT_PREFIX: u8 = 0x02;

pub fn scan_operator_join(
	storage: &MultiCommitBufferTier,
	node_id: FlowNodeId,
	left_cutoff: Option<CommitVersion>,
	right_cutoff: Option<CommitVersion>,
	batch_size: usize,
	cursor: &mut RangeCursor,
) -> Result<(Vec<ExpiredOperatorState>, ScanResult)> {
	let range = FlowNodeStateKey::node_range(node_id);
	let table = EntryKind::Operator(node_id);

	let start = bound_as_ref(&range.start);
	let end = bound_as_ref(&range.end);

	let mut expired = Vec::new();
	let mut batch_cursor = cursor.clone();
	let batch = storage.range_next(
		table,
		&mut batch_cursor,
		start,
		end,
		MultiVersionScope::AsOf {
			read: CommitVersion(u64::MAX),
		},
		batch_size,
	)?;

	for entry in &batch.entries {
		let Some(ref value) = entry.value else {
			continue;
		};

		let side_prefix = FlowNodeStateKey::decode(&entry.key).and_then(|k| k.key.first().copied());
		let cutoff = match side_prefix {
			Some(JOIN_LEFT_PREFIX) => left_cutoff,
			Some(JOIN_RIGHT_PREFIX) => right_cutoff,
			_ => None,
		};
		let Some(cutoff) = cutoff else {
			continue;
		};

		if entry.version <= cutoff {
			expired.push(ExpiredOperatorState {
				node_id,
				key: entry.key.clone(),
				version: entry.version,
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
	storage: &MultiCommitBufferTier,
	expired: &[ExpiredOperatorState],
	stats: &mut OperatorScanMetrics,
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
			if *version > row.version {
				continue;
			}
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

	use reifydb_codec::encoded::row::SHAPE_HEADER_SIZE;
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::flow::FlowNodeId, store::EntryKind},
		key::{flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
	};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::tier::{TierStorage, commit::buffer::MultiCommitBufferTier};

	fn row(payload: &[u8]) -> CowVec<u8> {
		let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
		buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
		CowVec::new(buf)
	}

	#[test]
	fn ttl_drop_never_reclaims_a_version_written_after_the_scan() {
		let storage = MultiCommitBufferTier::memory();
		let node = FlowNodeId(4);
		let table = EntryKind::Operator(node);
		let key = FlowNodeStateKey::encoded(node, vec![1u8]);

		storage.set(CommitVersion(1), HashMap::from([(table, vec![(key.clone(), Some(row(b"old")))])]))
			.unwrap();

		let mut cursor = RangeCursor::default();
		let (expired, _) = scan_operator_expired(&storage, node, CommitVersion(2), 4096, &mut cursor).unwrap();
		assert_eq!(expired.len(), 1, "the v1 row sits at or below the cutoff, so the scan must select it");

		// The scan has released its guard. A flow apply commits fresh state for the same key, far above
		// the cutoff - exactly the collision the GC is most likely to hit, because the key that just aged
		// out is the one about to be touched again.
		storage.set(CommitVersion(9), HashMap::from([(table, vec![(key.clone(), Some(row(b"live")))])]))
			.unwrap();

		let mut stats = OperatorScanMetrics::default();
		drop_expired_operator_keys(&storage, &expired, &mut stats).unwrap();

		let survivors = storage.get_all_versions(table, key.as_ref()).unwrap();
		assert!(
			survivors.iter().any(|(version, value)| *version == CommitVersion(9) && value.is_some()),
			"operator TTL GC reclaimed v9, which was written after the scan and is far above the cutoff. \
			 The scan proved only that v1 was expired; the drop re-reads the key and takes whatever it \
			 finds, so live operator state committed in the gap is destroyed - a join's build-side row or \
			 an aggregation's accumulator vanishes with no error and no log line"
		);
		assert_eq!(
			stats.versions_dropped, 1,
			"only the version the scan proved expired may be reclaimed, but {} were dropped",
			stats.versions_dropped
		);
	}

	#[test]
	fn join_drop_never_reclaims_a_version_written_after_the_scan() {
		let storage = MultiCommitBufferTier::memory();
		let node = FlowNodeId(5);
		let table = EntryKind::Operator(node);
		let key = FlowNodeStateKey::encoded(node, vec![JOIN_LEFT_PREFIX, 7u8]);

		storage.set(CommitVersion(1), HashMap::from([(table, vec![(key.clone(), Some(row(b"old")))])]))
			.unwrap();

		let mut cursor = RangeCursor::default();
		let (expired, _) = scan_operator_join(
			&storage,
			node,
			Some(CommitVersion(2)),
			Some(CommitVersion(2)),
			4096,
			&mut cursor,
		)
		.unwrap();
		assert_eq!(expired.len(), 1, "the v1 left-side row is at or below its side's cutoff");

		storage.set(CommitVersion(9), HashMap::from([(table, vec![(key.clone(), Some(row(b"live")))])]))
			.unwrap();

		let mut stats = OperatorScanMetrics::default();
		drop_expired_operator_keys(&storage, &expired, &mut stats).unwrap();

		let survivors = storage.get_all_versions(table, key.as_ref()).unwrap();
		assert!(
			survivors.iter().any(|(version, value)| *version == CommitVersion(9) && value.is_some()),
			"the join scan feeds the same drop function as the TTL scan, so it inherits the same defect: \
			 a build-side row committed after the scan is reclaimed, and subsequent probes miss it"
		);
	}

	#[test]
	fn scan_drops_expired_data_state_but_never_internal_state() {
		let storage = MultiCommitBufferTier::memory();
		let node = FlowNodeId(1);
		let table = EntryKind::Operator(node);

		let old_data = FlowNodeStateKey::encoded(node, vec![1u8]);
		let fresh_data = FlowNodeStateKey::encoded(node, vec![2u8]);
		// An internal-state row (e.g. a row-number mapping). It must stay immune even though it is
		// older than the cutoff, because operator GC only scans the data-state range.
		let old_internal = FlowNodeInternalStateKey::encoded(node, vec![9u8]);

		// Old data + internal at v1; the fresh data row at v3.
		storage.set(
			CommitVersion(1),
			HashMap::from([(
				table,
				vec![(old_data.clone(), Some(row(b"old"))), (old_internal.clone(), Some(row(b"map")))],
			)]),
		)
		.unwrap();
		storage.set(CommitVersion(3), HashMap::from([(table, vec![(fresh_data.clone(), Some(row(b"new")))])]))
			.unwrap();

		// Cutoff sits between the two writes: the v1 data row is expired, the v3 data row survives.
		let cutoff = CommitVersion(2);
		let mut cursor = RangeCursor::default();
		let (expired, _) = scan_operator_expired(&storage, node, cutoff, 4096, &mut cursor).unwrap();

		assert_eq!(expired.len(), 1, "only the data row written at or below the cutoff version should expire");
		assert_eq!(expired[0].key, old_data);

		let mut stats = OperatorScanMetrics::default();
		drop_expired_operator_keys(&storage, &expired, &mut stats).unwrap();

		// The internal-state row survives the drop - immune to operator GC.
		let internal_versions = storage.get_all_versions(table, old_internal.as_ref()).unwrap();
		assert!(
			internal_versions.iter().any(|(_, v)| v.is_some()),
			"internal state must never be reclaimed by the operator GC"
		);

		// Re-scanning finds nothing: the expired data row is gone, the fresh one stays.
		let mut cursor = RangeCursor::default();
		let (expired_after, _) = scan_operator_expired(&storage, node, cutoff, 4096, &mut cursor).unwrap();
		assert!(expired_after.is_empty(), "the expired data-state row should have been dropped");
	}

	#[test]
	fn join_scan_evicts_per_side_and_never_touches_schema_rows() {
		let storage = MultiCommitBufferTier::memory();
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

		// Old rows + schema rows at v1; fresh rows at v3.
		storage.set(
			CommitVersion(1),
			HashMap::from([(
				table,
				vec![
					(left_old.clone(), Some(row(b"lo"))),
					(right_old.clone(), Some(row(b"ro"))),
					(left_schema.clone(), Some(row(b"ls"))),
					(right_schema.clone(), Some(row(b"rs"))),
				],
			)]),
		)
		.unwrap();
		storage.set(
			CommitVersion(3),
			HashMap::from([(
				table,
				vec![(left_fresh.clone(), Some(row(b"lf"))), (right_fresh.clone(), Some(row(b"rf")))],
			)]),
		)
		.unwrap();

		// Both sides cut off at v2: each side's v1 row expires; v3 rows and schema rows survive.
		let cutoff = CommitVersion(2);
		let mut cursor = RangeCursor::default();
		let (expired, _) =
			scan_operator_join(&storage, node, Some(cutoff), Some(cutoff), 4096, &mut cursor).unwrap();
		let keys: Vec<&EncodedKey> = expired.iter().map(|e| &e.key).collect();
		assert_eq!(expired.len(), 2, "exactly the two old side rows expire");
		assert!(keys.contains(&&left_old) && keys.contains(&&right_old));
		assert!(!keys.contains(&&left_fresh) && !keys.contains(&&right_fresh), "fresh rows survive");
		assert!(
			!keys.contains(&&left_schema) && !keys.contains(&&right_schema),
			"schema rows are never scanned"
		);

		// Asymmetric: only the left side has a cutoff -> only the old left row is eligible.
		let mut cursor = RangeCursor::default();
		let (expired_left_only, _) =
			scan_operator_join(&storage, node, Some(cutoff), None, 4096, &mut cursor).unwrap();
		assert_eq!(expired_left_only.len(), 1);
		assert_eq!(expired_left_only[0].key, left_old);
	}

	#[test]
	fn join_scan_applies_independent_per_side_cutoffs() {
		// The two join sides expire on independent cutoff versions: a same-aged row is evicted on
		// the side whose cutoff reaches its version and kept on the side whose cutoff is below it.
		let storage = MultiCommitBufferTier::memory();
		let node = FlowNodeId(3);
		let table = EntryKind::Operator(node);
		let left = FlowNodeStateKey::encoded(node, vec![JOIN_LEFT_PREFIX, 1]);
		let right = FlowNodeStateKey::encoded(node, vec![JOIN_RIGHT_PREFIX, 1]);
		storage.set(
			CommitVersion(5),
			HashMap::from([(
				table,
				vec![(left.clone(), Some(row(b"l"))), (right.clone(), Some(row(b"r")))],
			)]),
		)
		.unwrap();

		// Left cutoff (10) reaches the rows' version -> left expires; right cutoff (3) is below -> right
		// survives.
		let mut cursor = RangeCursor::default();
		let (expired, _) = scan_operator_join(
			&storage,
			node,
			Some(CommitVersion(10)),
			Some(CommitVersion(3)),
			4096,
			&mut cursor,
		)
		.unwrap();
		assert_eq!(expired.len(), 1, "only the side whose cutoff reaches the row's version expires");
		assert_eq!(expired[0].key, left);
	}
}
