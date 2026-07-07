// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::{common::CommitVersion, key::cdc_consumer::CdcConsumerKeyRange};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::Result;

#[derive(Debug, Clone, Default)]
pub struct CdcConsumerWatermark {
	progress: Arc<AtomicU64>,
	gc_floor: Option<Arc<AtomicU64>>,
}

impl CdcConsumerWatermark {
	pub fn new() -> Self {
		Self {
			progress: Arc::new(AtomicU64::new(0)),
			gc_floor: None,
		}
	}

	pub fn from_handle(gc_floor: Arc<AtomicU64>) -> Self {
		Self {
			progress: Arc::new(AtomicU64::new(0)),
			gc_floor: Some(gc_floor),
		}
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.progress.load(Ordering::Acquire))
	}

	pub fn store(&self, v: CommitVersion) {
		if let Some(gc_floor) = &self.gc_floor {
			gc_floor.store(v.0, Ordering::Release);
		}
		self.progress.store(v.0, Ordering::Release);
	}
}

#[derive(Clone)]
pub struct FlowCaughtUpWatermark {
	sample: Arc<dyn Fn() -> CommitVersion + Send + Sync>,
}

impl FlowCaughtUpWatermark {
	pub fn new<F>(sample: F) -> Self
	where
		F: Fn() -> CommitVersion + Send + Sync + 'static,
	{
		Self {
			sample: Arc::new(sample),
		}
	}

	pub fn get(&self) -> CommitVersion {
		(self.sample)()
	}
}

pub fn compute_watermark(txn: &mut Transaction<'_>) -> Result<Option<CommitVersion>> {
	let mut min_version: Option<CommitVersion> = None;
	for multi in txn.range(CdcConsumerKeyRange::full_scan(), RangeScope::All, 1024)? {
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
