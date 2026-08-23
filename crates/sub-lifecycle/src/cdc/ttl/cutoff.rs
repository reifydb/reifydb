// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::consume::{host::CdcHost, watermark::compute_pinning_watermark};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::watermark::CheckpointFloor,
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_cdc::storage::{CdcStorage, Cutoff};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;
use tracing::instrument;

#[inline]
#[instrument(name = "lifecycle::cdc::ttl::cutoff", level = "trace", skip_all)]
pub fn find_eviction_target<S, H>(
	storage: &S,
	host: &H,
	clock: &Clock,
	floor: Option<&dyn CheckpointFloor>,
) -> Result<Option<Cutoff>>
where
	S: CdcStorage,
	H: CdcHost,
{
	let Some(ttl) = host.catalog().get_config_duration_opt(ConfigKey::CdcTtlDuration) else {
		return Ok(None);
	};
	let cutoff = clock.now().saturating_sub(ttl);
	let Some(ttl_cutoff) = storage.find_ttl_cutoff(cutoff)? else {
		return Ok(None);
	};
	let pinned = consumer_watermark(host, floor)?.map(|watermark| CommitVersion(watermark.0.saturating_add(1)));
	let bounded = match (ttl_cutoff, pinned) {
		(cutoff, None) => cutoff,
		(Cutoff::Unbounded, Some(pinned)) => Cutoff::Version(pinned),
		(Cutoff::Version(version), Some(pinned)) => Cutoff::Version(version.min(pinned)),
	};
	if bounded == Cutoff::Version(CommitVersion(0)) {
		return Ok(None);
	}
	Ok(Some(bounded))
}

#[inline]
#[instrument(name = "lifecycle::cdc::ttl::consumer_watermark", level = "trace", skip_all)]
pub fn consumer_watermark<H>(host: &H, floor: Option<&dyn CheckpointFloor>) -> Result<Option<CommitVersion>>
where
	H: CdcHost,
{
	let mut query = host.begin_query()?;
	compute_pinning_watermark(&mut Transaction::Query(&mut query), floor)
}

#[cfg(test)]
mod tests {
	use reifydb_cdc::{
		consume::checkpoint::CdcCheckpoint,
		testing::{TestCdcHost, make_bytes, make_key},
	};
	use reifydb_core::interface::cdc::{Cdc, CdcChange, CdcConsumerId, ConsumerClass};
	use reifydb_runtime::{actor::system::ActorSystem, pool::Pools};
	use reifydb_store_cdc::{config::CdcStoreConfig, store::CdcStore};
	use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

	use super::*;

	#[test]
	fn eviction_never_passes_the_consumer_watermark() {
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let storage = CdcStore::new(CdcStoreConfig::memory(actor_system.spawner().clone(), Clock::Real));
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

		// Ttl only sees sealed blocks and drops them whole, so each entry is sealed on its own.
		for v in 1..=10u64 {
			let cdc = Cdc::new(
				CommitVersion(v),
				DateTime::from_nanos(1000),
				vec![CdcChange::Insert {
					key: make_key(&format!("k{v}")),
					post: make_bytes("v"),
				}],
			);
			storage.write(&cdc).unwrap();
			assert!(storage.flush_pending());
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

		// The ttl alone would evict everything (cutoff = 11); the Pinning watermark caps it at 5 so
		// versions no flow has processed survive. Folding in the Ephemeral checkpoint would give 3,
		// so asserting 5 proves ephemeral lag cannot stall cdc truncation.
		assert_eq!(
			find_eviction_target(&storage, &host, &clock, None).unwrap(),
			Some(Cutoff::Version(CommitVersion(5))),
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

		assert_eq!(
			consumer_watermark(&host, None).unwrap(),
			Some(CommitVersion(40)),
			"with no floor supplied the pin must still be the consumer scan; regressing here would let a \
			 memory-only or floor-less deployment reap everything the ddl consumer has not read"
		);
		assert_eq!(
			consumer_watermark(&host, Some(&FixedFloor(Some(CommitVersion(12))))).unwrap(),
			Some(CommitVersion(12)),
			"a flow checkpoint behind the consumer scan must win; taking the scan alone reaps versions \
			 12..40 that the flow still has to replay after a crash"
		);
		assert_eq!(
			consumer_watermark(&host, Some(&FixedFloor(Some(CommitVersion(90))))).unwrap(),
			Some(CommitVersion(40)),
			"a flow ahead of the consumer scan must not raise the pin, otherwise the ddl consumer loses \
			 the entries it has not scanned"
		);
		assert_eq!(
			consumer_watermark(&host, Some(&FixedFloor(None))).unwrap(),
			Some(CommitVersion(40)),
			"a store with no checkpoint rows contributes no pin at all, matching a database whose flows \
			 have never committed"
		);
	}

	#[test]
	fn a_flow_floor_alone_pins_retention_when_no_consumer_checkpoint_exists() {
		// flow checkpoints left the consumer keyspace, so without the floor this scan returns nothing to pin on
		let host = TestCdcHost::new();

		assert_eq!(
			consumer_watermark(&host, None).unwrap(),
			None,
			"the multi-store scan alone sees no flow progress at all now that checkpoints live in the \
			 operator store"
		);
		assert_eq!(
			consumer_watermark(&host, Some(&FixedFloor(Some(CommitVersion(5))))).unwrap(),
			Some(CommitVersion(5)),
			"the operator store floor has to be the whole pin in that case; dropping it truncates cdc up \
			 to the ttl cutoff and the flow can never resume"
		);
	}
}
