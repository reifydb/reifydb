// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	encoded::row::EncodedRow,
	interface::{catalog::flow::FlowNodeId, store::EntryKind},
	key::flow_node_state::FlowNodeStateKey,
	row::Ttl,
};
use reifydb_type::{Result, util::cowvec::CowVec};

use super::OperatorScanStats;
use crate::{
	buffer::storage::BufferStorage,
	gc::row::scanner::ScanResult,
	tier::{RangeCursor, TierStorage},
};

pub struct ExpiredOperatorState {
	pub node_id: FlowNodeId,
	pub key: CowVec<u8>,
	pub scanned_bytes: u64,
}

pub fn scan_operator_by_created_at(
	storage: &BufferStorage,
	node_id: FlowNodeId,
	ttl_config: &Ttl,
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

			if now_nanos.saturating_sub(anchor_nanos) >= ttl_config.duration_nanos {
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
	storage: &BufferStorage,
	node_id: FlowNodeId,
	ttl_config: &Ttl,
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

			if now_nanos.saturating_sub(anchor_nanos) >= ttl_config.duration_nanos {
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

fn bound_as_ref(bound: &Bound<impl AsRef<[u8]>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_ref()),
		Bound::Excluded(v) => Bound::Excluded(v.as_ref()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

pub fn drop_expired_operator_keys(
	storage: &BufferStorage,
	expired: &[ExpiredOperatorState],
	stats: &mut OperatorScanStats,
) -> Result<()> {
	if expired.is_empty() {
		return Ok(());
	}

	let mut drop_batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>> = HashMap::new();

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
