// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	lifecycle::{
		class::{Floor, FloorTerm, RetentionClass},
		watermark::{ConsumerPositions, EvictionWatermark},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::version_epoch::{EpochSeconds, VersionEpoch};
use reifydb_value::{
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration},
};

pub trait FloorSource: Send + Sync + 'static {
	fn query_done_until(&self) -> CommitVersion;

	fn lease_min(&self) -> CommitVersion;

	fn consumer_checkpoint(&self) -> CommitVersion;

	fn consumer_position(&self) -> CommitVersion;

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
		self.epoch.floor_version_at(EpochSeconds::from_datetime(expires_before)).map(CommitVersion)
	}

	pub fn expiry_instant(&self, now: DateTime, ttl: Duration) -> Option<DateTime> {
		now.checked_sub(ttl)
	}

	pub fn term(&self, term: FloorTerm, now: DateTime, ttl: Option<Duration>) -> Option<Floor> {
		match term {
			FloorTerm::RowExpiry | FloorTerm::OperatorExpiry => {
				self.expiry_instant(now, ttl?).map(Floor::Instant)
			}
			FloorTerm::RetentionHorizon => self.expiry_cutoff(now, ttl?).map(Floor::Version),
			FloorTerm::QueryDoneUntil => Some(Floor::Version(self.source.query_done_until())),
			FloorTerm::LeaseMin => Some(Floor::Version(self.source.lease_min())),
			FloorTerm::ConsumerCheckpoint => Some(Floor::Version(self.source.consumer_checkpoint())),
			FloorTerm::ConsumerPosition => Some(Floor::Version(self.source.consumer_position())),
			FloorTerm::FlushWatermark => Some(Floor::Version(self.source.flush_watermark())),
			FloorTerm::OwningFlowCheckpoint => Some(Floor::Version(self.source.owning_flow_checkpoint())),
		}
	}

	pub fn cutoff(&self, class: RetentionClass, now: DateTime, ttl: Option<Duration>) -> Option<Floor> {
		self.cutoff_with_binding(class, now, ttl).map(|(floor, _)| floor)
	}

	pub fn cutoff_with_binding(
		&self,
		class: RetentionClass,
		now: DateTime,
		ttl: Option<Duration>,
	) -> Option<(Floor, FloorTerm)> {
		let mut floor: Option<(Floor, FloorTerm)> = None;
		for term in class.floor_terms() {
			let resolved = self.term(*term, now, ttl)?;
			floor = Some(match floor {
				Some((current, binding)) => {
					reifydb_assertions! {
						assert!(
							current.is_same_domain(&resolved),
							"class {class} mixes a version floor with an instant floor; a min \
							 across domains is meaningless and would silently pick whichever \
							 raw integer happened to be smaller"
						);
					}
					match current.monotonic_key() <= resolved.monotonic_key() {
						true => (current, binding),
						false => (resolved, *term),
					}
				}
				None => (resolved, *term),
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

	fn consumer_position(&self) -> CommitVersion {
		self.engine
			.ioc()
			.try_resolve::<Arc<dyn ConsumerPositions>>()
			.and_then(|positions| positions.min_position())
			.unwrap_or(CommitVersion(u64::MAX))
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

	const ONE_HOUR_IN: EpochSeconds = EpochSeconds::new(3_600);
	const TWO_HOURS_IN: EpochSeconds = EpochSeconds::new(7_200);

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

		fn consumer_position(&self) -> CommitVersion {
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
		epoch.backfill(ONE_HOUR_IN, 1_000);
		epoch.record(TWO_HOURS_IN, 5_000);
		HorizonLedger::new(
			Arc::new(ScriptedFlow {
				flow: CommitVersion(flow),
			}),
			epoch,
		)
	}

	fn now() -> DateTime {
		TWO_HOURS_IN.to_datetime()
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

		assert_eq!(cutoff, Floor::Version(CommitVersion(10)));
		assert_eq!(
			binding,
			FloorTerm::OwningFlowCheckpoint,
			"the report must name the flow as the thing holding reclamation back, or a stalled \
			 group class looks like an idle one"
		);
	}
}
