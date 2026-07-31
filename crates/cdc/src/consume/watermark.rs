// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::ConsumerClass,
	key::{
		EncodableKey,
		cdc_consumer::{CdcConsumerKey, CdcConsumerKeyRange},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::Result;

use super::checkpoint::CheckpointRow;

#[derive(Debug, Clone, Default)]
pub struct CdcConsumerWatermark {
	progress: Arc<AtomicU64>,
}

impl CdcConsumerWatermark {
	pub fn new() -> Self {
		Self {
			progress: Arc::new(AtomicU64::new(0)),
		}
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.progress.load(Ordering::Acquire))
	}

	pub fn store(&self, v: CommitVersion) {
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

pub fn compute_pinning_watermark(txn: &mut Transaction<'_>) -> Result<Option<CommitVersion>> {
	let mut min_version: Option<CommitVersion> = None;
	for multi in txn.range(CdcConsumerKeyRange::full_scan(), RangeScope::All, 1024)? {
		let multi = multi?;
		if CdcConsumerKey::decode(&multi.key).is_none() {
			continue;
		}
		let Some(row) = CheckpointRow::decode(&multi.row) else {
			continue;
		};
		if row.class != ConsumerClass::Pinning {
			continue;
		}
		min_version = Some(min_version.map_or(row.version, |m| m.min(row.version)));
	}

	Ok(min_version)
}
