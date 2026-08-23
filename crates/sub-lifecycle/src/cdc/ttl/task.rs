// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::consume::host::CdcHost;
use reifydb_core::{
	event::{EventBus, metric::CdcEvictedEvent},
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask, watermark::CheckpointFloor},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_cdc::storage::{CdcStorage, Cutoff};
use reifydb_value::{Result, value::duration::Duration};
use tracing::{debug, error, instrument};

use crate::cdc::ttl::cutoff;

pub struct CdcTtlTask<S, H> {
	storage: Arc<S>,
	host: H,
	event_bus: EventBus,
	clock: Clock,
	checkpoint_floor: Option<Arc<dyn CheckpointFloor>>,
}

impl<S, H> CdcTtlTask<S, H>
where
	S: CdcStorage + Send + Sync + 'static,
	H: CdcHost,
{
	pub fn new(
		storage: S,
		host: H,
		event_bus: EventBus,
		clock: Clock,
		checkpoint_floor: Option<Arc<dyn CheckpointFloor>>,
	) -> Self {
		Self {
			storage: Arc::new(storage),
			host,
			event_bus,
			clock,
			checkpoint_floor,
		}
	}

	#[instrument(name = "lifecycle::cdc::ttl::evict", level = "debug", skip_all)]
	fn evict_slice(&self) -> Result<Progress> {
		let Some(cutoff) = cutoff::find_eviction_target(
			&*self.storage,
			&self.host,
			&self.clock,
			self.checkpoint_floor.as_deref(),
		)?
		else {
			return Ok(Progress::Exhausted);
		};
		let batch_size = (self.host.catalog().get_config_uint8(ConfigKey::CdcTtlScanBatchSize) as usize).max(1);
		let result = self.storage.drop_before(cutoff, batch_size)?;
		if result.count.as_u64() > 0 {
			let kept = match cutoff {
				Cutoff::Version(version) => version,
				Cutoff::Unbounded => self.storage.truncated_before()?,
			};
			debug!(cutoff = kept.0, deleted = result.count.as_u64(), "CDC TTL eviction batch completed");
			self.event_bus.emit(CdcEvictedEvent::new(result.entries, kept));
		}
		Ok(if result.more_remaining {
			Progress::Yielded
		} else {
			Progress::Exhausted
		})
	}
}

impl<S, H> LifecycleTask for CdcTtlTask<S, H>
where
	S: CdcStorage + Send + Sync + 'static,
	H: CdcHost,
{
	fn name(&self) -> &'static str {
		"cdc-ttl"
	}

	fn interval(&self) -> Duration {
		self.host.catalog().get_config_duration(ConfigKey::CdcTtlScanInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::CdcTruncate]
	}

	#[instrument(name = "lifecycle::cdc::ttl::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		match self.evict_slice() {
			Ok(progress) => progress,
			Err(e) => {
				error!(error = ?e, "CDC TTL eviction failed");
				Progress::Exhausted
			}
		}
	}
}
