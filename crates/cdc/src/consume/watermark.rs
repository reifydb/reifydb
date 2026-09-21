// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::internal::internal,
	interface::{catalog::flow::FlowId, cdc::ConsumerClass},
	key::{any::TaggedKey, cdc::CdcConsumerKey},
	lifecycle::watermark::CheckpointFloor,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{Result, error::Error};
use tracing::warn;

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
	poisoned: Arc<dyn Fn() -> Vec<(FlowId, String)> + Send + Sync>,
	stalled: Arc<dyn Fn() -> Vec<FlowId> + Send + Sync>,
}

impl FlowCaughtUpWatermark {
	pub fn new<F, P, S>(sample: F, poisoned: P, stalled: S) -> Self
	where
		F: Fn() -> CommitVersion + Send + Sync + 'static,
		P: Fn() -> Vec<(FlowId, String)> + Send + Sync + 'static,
		S: Fn() -> Vec<FlowId> + Send + Sync + 'static,
	{
		Self {
			sample: Arc::new(sample),
			poisoned: Arc::new(poisoned),
			stalled: Arc::new(stalled),
		}
	}

	pub fn get(&self) -> CommitVersion {
		(self.sample)()
	}

	pub fn poisoned(&self) -> Vec<(FlowId, String)> {
		(self.poisoned)()
	}

	pub fn stalled(&self) -> Vec<FlowId> {
		(self.stalled)()
	}
}

pub fn compute_pinning_watermark(
	txn: &mut Transaction<'_>,
	floor: Option<&dyn CheckpointFloor>,
) -> Result<Option<CommitVersion>> {
	let mut min_version: Option<CommitVersion> = None;
	for multi in txn.range(CdcConsumerKey::full_scan(), RangeScope::All, 1024)? {
		let multi = multi?;
		if !matches!(&multi.key, TaggedKey::CdcConsumer(_)) {
			continue;
		}
		let Some(bytes) = CheckpointRow::decode(&multi.bytes) else {
			return Err(Error(Box::new(internal("a cdc consumer checkpoint row could not be decoded"))));
		};
		if bytes.class != ConsumerClass::Pinning {
			continue;
		}
		min_version = Some(min_version.map_or(bytes.version, |m| m.min(bytes.version)));
	}

	if let Some(floor) = floor {
		match floor.floor() {
			Ok(Some(durable)) => {
				min_version = Some(min_version.map_or(durable, |m| m.min(durable)));
			}
			Ok(None) => {}
			Err(err) => {
				warn!(error = %err, "checkpoint floor unavailable, pinning cdc retention at version zero");
				min_version = Some(CommitVersion(0));
			}
		}
	}

	Ok(min_version)
}
