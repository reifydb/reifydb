// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Consumer watermark tracking for CDC retention coordination
//!
//! This module provides functionality to compute the minimum checkpoint version
//! across all CDC consumers. This watermark serves as a safety floor for retention
//! policies - versions at or above the watermark cannot be cleaned up because
//! consumers still need them.

use reifydb_core::{common::CommitVersion, key::cdc_consumer::CdcConsumerKeyRange};
use reifydb_transaction::transaction::AsTransaction;

/// Computes the consumer watermark by finding the minimum checkpoint version
/// across all registered CDC consumers.
///
/// The watermark represents the lowest commit version that any consumer has
/// checkpointed. Retention policies must not clean up versions at or above
/// this watermark, as consumers still need them.
///
/// # Consumer Discovery
///
/// Consumers are discovered by scanning checkpoint keys. A consumer exists
/// if and only if it has written a checkpoint. Consumers without checkpoints
/// are not considered.
///
/// # Return Value
///
/// Returns the minimum `CommitVersion` across all consumer checkpoints.
/// If no consumers exist (no checkpoints found), returns `CommitVersion(1)`
/// as a safe default (prevents cleanup of all versions).
///
/// # Example
///
/// ```ignore
/// let watermark = compute_watermark(&mut txn)?;
/// // Now retention can safely cleanup versions < watermark
/// ```
pub fn compute_watermark(txn: &mut impl AsTransaction) -> reifydb_type::Result<CommitVersion> {
	let mut min_version: Option<CommitVersion> = None;

	for multi in txn.as_transaction().range(CdcConsumerKeyRange::full_scan(), 1024)? {
		let multi = multi?;
		// Checkpoint values are stored as 8-byte big-endian u64
		if multi.values.len() >= 8 {
			let mut buffer = [0u8; 8];
			buffer.copy_from_slice(&multi.values[0..8]);
			let version = CommitVersion(u64::from_be_bytes(buffer));

			// Track minimum version across all consumers
			min_version = Some(match min_version {
				None => version,
				Some(current_min) => current_min.min(version),
			});
		}
	}

	// If no consumers exist, return CommitVersion(1) as safe default
	// This prevents any cleanup when there are no consumers registered
	Ok(min_version.unwrap_or(CommitVersion(1)))
}
