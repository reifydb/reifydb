// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::{
	consume::{host::CdcHost, watermark::compute_watermark},
	storage::CdcStorage,
};
use reifydb_core::{
	common::CommitVersion,
	event::{EventBus, metric::CdcEvictedEvent},
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{debug, error, instrument};

pub struct CdcTtlTask<S, H> {
	storage: Arc<S>,
	host: H,
	event_bus: EventBus,
	clock: Clock,
}

impl<S, H> CdcTtlTask<S, H>
where
	S: CdcStorage + Send + Sync + 'static,
	H: CdcHost,
{
	pub fn new(storage: S, host: H, event_bus: EventBus, clock: Clock) -> Self {
		Self {
			storage: Arc::new(storage),
			host,
			event_bus,
			clock,
		}
	}

	#[instrument(name = "lifecycle::cdc::ttl::evict", level = "debug", skip_all)]
	fn evict_slice(&self) -> Result<Progress> {
		let Some(cutoff_version) = self.find_eviction_target()? else {
			return Ok(Progress::Exhausted);
		};
		let batch_size = (self.host.catalog().get_config_uint8(ConfigKey::CdcTtlScanBatchSize) as usize).max(1);
		let result = self.storage.drop_before(cutoff_version, batch_size)?;
		if result.count.as_u64() > 0 {
			debug!(
				cutoff = cutoff_version.0,
				deleted = result.count.as_u64(),
				"CDC TTL eviction batch completed"
			);
			self.event_bus.emit(CdcEvictedEvent::new(result.entries, cutoff_version));
		}
		Ok(if result.more_remaining {
			Progress::Yielded
		} else {
			Progress::Exhausted
		})
	}

	#[inline]
	#[instrument(name = "lifecycle::cdc::ttl::cutoff", level = "trace", skip_all)]
	fn find_eviction_target(&self) -> Result<Option<CommitVersion>> {
		let Some(ttl) = self.host.catalog().get_config_duration_opt(ConfigKey::CdcTtlDuration) else {
			return Ok(None);
		};
		let cutoff_nanos = self.clock.now_nanos().saturating_sub(ttl.to_std().as_nanos() as u64);
		let cutoff = DateTime::from_nanos(cutoff_nanos);
		let Some(ttl_cutoff) = self.storage.find_ttl_cutoff(cutoff)? else {
			return Ok(None);
		};
		let cutoff_version = match self.consumer_watermark()? {
			Some(watermark) => ttl_cutoff.min(CommitVersion(watermark.0.saturating_add(1))),
			None => ttl_cutoff,
		};
		if cutoff_version.0 == 0 {
			return Ok(None);
		}
		Ok(Some(cutoff_version))
	}

	#[inline]
	#[instrument(name = "lifecycle::cdc::ttl::consumer_watermark", level = "trace", skip_all)]
	fn consumer_watermark(&self) -> Result<Option<CommitVersion>> {
		let mut query = self.host.begin_query()?;
		compute_watermark(&mut Transaction::Query(&mut query))
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

#[cfg(test)]
mod tests {
	use reifydb_cdc::{
		consume::checkpoint::CdcCheckpoint,
		storage::memory::MemoryCdcStorage,
		testing::{TestCdcHost, make_key, make_row},
	};
	use reifydb_core::{
		common::CommitVersion,
		event::EventBus,
		interface::{
			catalog::config::ConfigKey,
			cdc::{Cdc, CdcConsumerId, SystemChange},
		},
	};
	use reifydb_runtime::{actor::system::ActorSystem, pool::Pools};
	use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

	use super::*;

	#[test]
	fn eviction_never_passes_the_consumer_watermark() {
		let storage = MemoryCdcStorage::new();
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		let host = TestCdcHost::new();
		let clock = host.clock.clone();

		// Aggressive TTL: every already-written (old-timestamped) entry is TTL-eligible for eviction.
		host.catalog()
			.cache()
			.set_config(
				ConfigKey::CdcTtlDuration,
				CommitVersion(1),
				Value::Duration(Duration::from_milliseconds(1).unwrap()),
			)
			.unwrap();

		// CDC entries 1..=10, all timestamped far in the past relative to the host's mock clock.
		for v in 1..=10u64 {
			let cdc = Cdc::new(
				CommitVersion(v),
				DateTime::from_nanos(1000),
				vec![],
				vec![SystemChange::Insert {
					key: make_key(&format!("k{v}")),
					post: make_row("v"),
				}],
			);
			storage.write(&cdc).unwrap();
		}

		// All flows have durably processed only through version 4.
		let mut cmd = host.begin_command().unwrap();
		CdcCheckpoint::persist(&mut cmd, &CdcConsumerId::new("flow"), CommitVersion(4)).unwrap();
		cmd.commit().unwrap();

		let task = CdcTtlTask::new(storage.clone(), host, event_bus, clock);

		// TTL alone would evict everything (cutoff = 11). The consumer watermark caps the cutoff at
		// 5 (= 4 + 1), so versions 5..=10 - not yet processed by all flows - are never dropped.
		assert_eq!(
			task.find_eviction_target().unwrap(),
			Some(CommitVersion(5)),
			"eviction cutoff must never pass the minimum consumer checkpoint + 1"
		);
	}
}
