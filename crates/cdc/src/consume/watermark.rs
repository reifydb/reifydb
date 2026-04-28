// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, key::cdc_consumer::CdcConsumerKeyRange};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

/// Computes the consumer watermark by finding the minimum checkpoint version
/// across all registered CDC consumers.
///
/// The watermark represents the lowest commit version that any consumer has
/// checkpointed. Retention policies must not clean up versions at or above
/// this watermark, as consumers still need them.
pub fn compute_watermark(txn: &mut Transaction<'_>) -> Result<CommitVersion> {
	let mut min_version: Option<CommitVersion> = None;
	for multi in txn.range(CdcConsumerKeyRange::full_scan(), 1024)? {
		let multi = multi?;
		if let Some(version) = decode_checkpoint_row(&multi.row) {
			min_version = Some(min_version.map_or(version, |m| m.min(version)));
		}
	}
	// If no consumers exist, return CommitVersion(1) as safe default;
	// this prevents any cleanup when there are no consumers registered.
	Ok(min_version.unwrap_or(CommitVersion(1)))
}

/// Checkpoint values are stored as 8-byte big-endian u64. Returns `None` if
/// the row is too short (treated as "no checkpoint").
#[inline]
fn decode_checkpoint_row(row: &[u8]) -> Option<CommitVersion> {
	if row.len() < 8 {
		return None;
	}
	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&row[0..8]);
	Some(CommitVersion(u64::from_be_bytes(buffer)))
}
