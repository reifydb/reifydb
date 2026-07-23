// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	lifecycle::{
		class::{FloorTerm, RetentionClass},
		watermark::EvictionWatermark,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::version_epoch::VersionEpoch;
use reifydb_value::value::{datetime::DateTime, duration::Duration};

pub trait FloorSource: Send + Sync + 'static {
	fn query_done_until(&self) -> CommitVersion;

	fn lease_min(&self) -> CommitVersion;

	fn consumer_checkpoint(&self) -> CommitVersion;

	fn subscription_snapshot(&self) -> CommitVersion;

	fn flush_watermark(&self) -> CommitVersion;
}

pub struct HorizonLedger {
	source: Arc<dyn FloorSource>,
	epoch: VersionEpoch,
}

impl HorizonLedger {
	pub fn new(source: Arc<dyn FloorSource>, epoch: VersionEpoch) -> Self {
		Self {
			source,
			epoch,
		}
	}

	pub fn expiry_cutoff(&self, now: DateTime, ttl: Duration) -> Option<CommitVersion> {
		let expires_before = now.checked_sub(ttl)?;
		self.epoch.floor_version_at(expires_before.to_nanos()).map(CommitVersion)
	}

	pub fn term(&self, term: FloorTerm, now: DateTime, ttl: Option<Duration>) -> Option<CommitVersion> {
		match term {
			FloorTerm::RowExpiry | FloorTerm::OperatorExpiry | FloorTerm::RetentionHorizon => {
				self.expiry_cutoff(now, ttl?)
			}
			FloorTerm::QueryDoneUntil => Some(self.source.query_done_until()),
			FloorTerm::LeaseMin => Some(self.source.lease_min()),
			FloorTerm::ConsumerCheckpoint => Some(self.source.consumer_checkpoint()),
			FloorTerm::SubscriptionSnapshot => Some(self.source.subscription_snapshot()),
			FloorTerm::FlushWatermark => Some(self.source.flush_watermark()),
			FloorTerm::OwningFlowCheckpoint => None,
		}
	}

	pub fn cutoff(&self, class: RetentionClass, now: DateTime, ttl: Option<Duration>) -> Option<CommitVersion> {
		self.cutoff_with_binding(class, now, ttl).map(|(version, _)| version)
	}

	pub fn cutoff_with_binding(
		&self,
		class: RetentionClass,
		now: DateTime,
		ttl: Option<Duration>,
	) -> Option<(CommitVersion, FloorTerm)> {
		let mut floor: Option<(CommitVersion, FloorTerm)> = None;
		for term in class.floor_terms() {
			let resolved = self.term(*term, now, ttl)?;
			floor = Some(match floor {
				Some((current, binding)) if current <= resolved => (current, binding),
				Some(_) | None => (resolved, *term),
			});
		}
		floor
	}
}

pub struct EngineFloors {
	engine: StandardEngine,
}

impl EngineFloors {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}
}

impl FloorSource for EngineFloors {
	fn query_done_until(&self) -> CommitVersion {
		self.engine.query_done_until()
	}

	fn lease_min(&self) -> CommitVersion {
		self.engine.multi().leases().min_active().unwrap_or(CommitVersion(u64::MAX))
	}

	fn consumer_checkpoint(&self) -> CommitVersion {
		self.engine.consumer_watermark()
	}

	fn subscription_snapshot(&self) -> CommitVersion {
		self.engine.multi().consumer_watermark()
	}

	fn flush_watermark(&self) -> CommitVersion {
		EvictionWatermark::watermark(&self.engine)
	}
}
