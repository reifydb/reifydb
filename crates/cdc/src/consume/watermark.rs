// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::{common::CommitVersion, key::cdc_consumer::CdcConsumerKeyRange};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

#[derive(Debug, Clone, Default)]
pub struct CdcConsumerWatermark(Arc<AtomicU64>);

impl CdcConsumerWatermark {
	pub fn new() -> Self {
		Self(Arc::new(AtomicU64::new(0)))
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.0.load(Ordering::Acquire))
	}

	pub fn store(&self, v: CommitVersion) {
		self.0.store(v.0, Ordering::Release);
	}
}

pub fn compute_watermark(txn: &mut Transaction<'_>) -> Result<Option<CommitVersion>> {
	let mut min_version: Option<CommitVersion> = None;
	for multi in txn.range(CdcConsumerKeyRange::full_scan(), 1024)? {
		let multi = multi?;
		if let Some(version) = decode_checkpoint_row(&multi.row) {
			min_version = Some(min_version.map_or(version, |m| m.min(version)));
		}
	}

	Ok(min_version)
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
