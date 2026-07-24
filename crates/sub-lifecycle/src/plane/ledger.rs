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

	fn owning_flow_checkpoint(&self) -> CommitVersion;
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
			FloorTerm::OwningFlowCheckpoint => Some(self.source.owning_flow_checkpoint()),
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

	fn owning_flow_checkpoint(&self) -> CommitVersion {
		self.engine.flow_watermark()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	const HOUR_NANOS: u64 = 3_600 * 1_000_000_000;

	struct ScriptedFlow {
		flow: CommitVersion,
	}

	impl FloorSource for ScriptedFlow {
		fn query_done_until(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn lease_min(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn consumer_checkpoint(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn subscription_snapshot(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn flush_watermark(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn owning_flow_checkpoint(&self) -> CommitVersion {
			self.flow
		}
	}

	fn ledger(flow: u64) -> HorizonLedger {
		let epoch = VersionEpoch::new();
		epoch.backfill(HOUR_NANOS, 1_000);
		epoch.record(2 * HOUR_NANOS, 5_000);
		HorizonLedger::new(
			Arc::new(ScriptedFlow {
				flow: CommitVersion(flow),
			}),
			epoch,
		)
	}

	fn now() -> DateTime {
		DateTime::from_nanos(2 * HOUR_NANOS)
	}

	fn one_hour() -> Duration {
		Duration::from_hours(1).expect("one hour is representable")
	}

	#[test]
	fn a_group_class_resolves_to_a_cutoff_instead_of_declining_to_answer() {
		// OwningFlowCheckpoint used to resolve to None, and cutoff_with_binding propagates a None term
		// to the whole class. Any class naming the term therefore reclaimed NOTHING, forever, while
		// reporting healthy. Both group phases name it, so this is the difference between the plane
		// working and the plane being decorative.
		for class in [RetentionClass::OperatorGroupData, RetentionClass::OperatorGroupIdentity] {
			let cutoff = ledger(u64::MAX).cutoff(class, now(), Some(one_hour()));

			assert!(cutoff.is_some(), "{class} still declines to produce a cutoff");
		}
	}

	#[test]
	fn a_flow_that_has_not_processed_its_input_holds_the_group_cutoff_down() {
		// The floor is a min over terms, so the lagging term must win. A flow parked at version 10 has
		// input it has not yet applied; reclaiming group state above that point discards state its own
		// unprocessed changes still refer to. The expiry term alone would have allowed version 1000.
		let (cutoff, binding) = ledger(10)
			.cutoff_with_binding(RetentionClass::OperatorGroupData, now(), Some(one_hour()))
			.expect("both terms resolve");

		assert_eq!(cutoff, CommitVersion(10));
		assert_eq!(
			binding,
			FloorTerm::OwningFlowCheckpoint,
			"the report must name the flow as the thing holding reclamation back, or a stalled \
			 group class looks like an idle one"
		);
	}

	#[test]
	fn a_caught_up_flow_lets_the_declared_horizon_bind_instead() {
		// The mirror image: once the flow is current it stops constraining anything, and the class
		// falls back to its own horizon. If the flow term still bound here, declaring a ttl would have
		// no effect on when state actually leaves.
		let (cutoff, binding) = ledger(u64::MAX)
			.cutoff_with_binding(RetentionClass::OperatorGroupData, now(), Some(one_hour()))
			.expect("both terms resolve");

		assert_eq!(cutoff, CommitVersion(1_000), "the hour-old epoch sample is the expiry floor");
		assert_eq!(binding, FloorTerm::OperatorExpiry);
	}
}
