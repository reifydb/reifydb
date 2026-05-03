// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, key::cdc_consumer::CdcConsumerKeyRange};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

pub fn compute_watermark(txn: &mut Transaction<'_>) -> Result<CommitVersion> {
	let mut min_version: Option<CommitVersion> = None;
	for multi in txn.range(CdcConsumerKeyRange::full_scan(), 1024)? {
		let multi = multi?;
		if let Some(version) = decode_checkpoint_row(&multi.row) {
			min_version = Some(min_version.map_or(version, |m| m.min(version)));
		}
	}

	Ok(min_version.unwrap_or(CommitVersion(1)))
}

#[inline]
fn decode_checkpoint_row(row: &[u8]) -> Option<CommitVersion> {
	if row.len() < 8 {
		return None;
	}
	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&row[0..8]);
	Some(CommitVersion(u64::from_be_bytes(buffer)))
}
