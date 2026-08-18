// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::{
	consume::{host::CdcHost, watermark::compute_pinning_watermark},
	storage::CdcStorage,
};
use reifydb_core::{
	common::CommitVersion,
	event::{EventBus, metric::CdcEvictedEvent},
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask, watermark::CheckpointFloor},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, value::duration::Duration};
use tracing::{debug, error, instrument};

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
		let cutoff = self.clock.now().saturating_sub(ttl);
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
		compute_pinning_watermark(&mut Transaction::Query(&mut query), self.checkpoint_floor.as_deref())
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
		testing::{TestCdcHost, make_bytes, make_key},
	};
	use reifydb_core::{
		common::CommitVersion,
		event::EventBus,
		interface::{
			catalog::config::ConfigKey,
			cdc::{Cdc, CdcConsumerId, ConsumerClass, SystemChange},
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
				vec![SystemChange::Insert {
					key: make_key(&format!("k{v}")),
					post: make_bytes("v"),
				}],
			);
			storage.write(&cdc).unwrap();
		}

		// All flows have durably processed only through version 4. A lagging Ephemeral consumer
		// sits even further behind at version 2 and must NOT hold the cutoff: only Pinning
		// checkpoints bound retention.
		let mut cmd = host.begin_command().unwrap();
		CdcCheckpoint::persist(&mut cmd, &CdcConsumerId::new("flow"), CommitVersion(4), ConsumerClass::Pinning)
			.unwrap();
		CdcCheckpoint::persist(
			&mut cmd,
			&CdcConsumerId::new("laggard"),
			CommitVersion(2),
			ConsumerClass::Ephemeral,
		)
		.unwrap();
		cmd.commit().unwrap();

		let task = CdcTtlTask::new(storage.clone(), host, event_bus, clock, None);

		// The ttl alone would evict everything (cutoff = 11); the Pinning watermark caps it at 5 so
		// versions no flow has processed survive. Folding in the Ephemeral checkpoint would give 3,
		// so asserting 5 proves ephemeral lag cannot stall cdc truncation.
		assert_eq!(
			task.find_eviction_target().unwrap(),
			Some(CommitVersion(5)),
			"eviction cutoff must be bounded by Pinning checkpoints only"
		);
	}

	struct FixedFloor(Option<CommitVersion>);

	impl CheckpointFloor for FixedFloor {
		fn floor(&self) -> Option<CommitVersion> {
			self.0
		}
	}

	#[test]
	fn the_pinning_watermark_is_the_lower_of_the_consumer_scan_and_the_durable_flow_floor() {
		// the two pins guard different resumes, so retention has to respect whichever is further behind
		let host = TestCdcHost::new();
		let mut cmd = host.begin_command().unwrap();
		CdcCheckpoint::persist(&mut cmd, &CdcConsumerId::new("ddl"), CommitVersion(40), ConsumerClass::Pinning)
			.unwrap();
		cmd.commit().unwrap();

		let mut query = host.begin_query().unwrap();
		let mut txn = Transaction::Query(&mut query);

		assert_eq!(
			compute_pinning_watermark(&mut txn, None).unwrap(),
			Some(CommitVersion(40)),
			"with no floor supplied the pin must still be the consumer scan; regressing here would let a \
			 memory-only or floor-less deployment reap everything the ddl consumer has not read"
		);
		assert_eq!(
			compute_pinning_watermark(&mut txn, Some(&FixedFloor(Some(CommitVersion(12))))).unwrap(),
			Some(CommitVersion(12)),
			"a flow checkpoint behind the consumer scan must win; taking the scan alone reaps versions \
			 12..40 that the flow still has to replay after a crash"
		);
		assert_eq!(
			compute_pinning_watermark(&mut txn, Some(&FixedFloor(Some(CommitVersion(90))))).unwrap(),
			Some(CommitVersion(40)),
			"a flow ahead of the consumer scan must not raise the pin, otherwise the ddl consumer loses \
			 the entries it has not scanned"
		);
		assert_eq!(
			compute_pinning_watermark(&mut txn, Some(&FixedFloor(None))).unwrap(),
			Some(CommitVersion(40)),
			"a store with no checkpoint rows contributes no pin at all, matching a database whose flows \
			 have never committed"
		);
	}

	#[test]
	fn a_flow_floor_alone_pins_retention_when_no_consumer_checkpoint_exists() {
		// flow checkpoints left the consumer keyspace, so without the floor this scan returns nothing to pin on
		let host = TestCdcHost::new();
		let mut query = host.begin_query().unwrap();
		let mut txn = Transaction::Query(&mut query);

		assert_eq!(
			compute_pinning_watermark(&mut txn, None).unwrap(),
			None,
			"the multi-store scan alone sees no flow progress at all now that checkpoints live in the \
			 operator store"
		);
		assert_eq!(
			compute_pinning_watermark(&mut txn, Some(&FixedFloor(Some(CommitVersion(5))))).unwrap(),
			Some(CommitVersion(5)),
			"the operator store floor has to be the whole pin in that case; dropping it truncates cdc up \
			 to the ttl cutoff and the flow can never resume"
		);
	}
}
