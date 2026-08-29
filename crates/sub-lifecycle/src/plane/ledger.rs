// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	lifecycle::{
		class::{Floor, FloorTerm, RetentionClass},
		watermark::ConsumerPositions,
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
			FloorTerm::RowExpiry => self.expiry_instant(now, ttl?).map(Floor::Instant),
			FloorTerm::RetentionHorizon => self.expiry_cutoff(now, ttl?).map(Floor::Version),
			FloorTerm::QueryDoneUntil => Some(Floor::Version(self.source.query_done_until())),
			FloorTerm::LeaseMin => Some(Floor::Version(self.source.lease_min())),
			FloorTerm::ConsumerCheckpoint => Some(Floor::Version(self.source.consumer_checkpoint())),
			FloorTerm::ConsumerPosition => Some(Floor::Version(self.source.consumer_position())),
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
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	const ONE_HOUR_IN: EpochSeconds = EpochSeconds::new(3_600);
	const TWO_HOURS_IN: EpochSeconds = EpochSeconds::new(7_200);

	struct ScriptedFloors;

	impl FloorSource for ScriptedFloors {
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
	}

	fn ledger() -> HorizonLedger {
		let epoch = VersionEpoch::new();
		epoch.backfill(ONE_HOUR_IN, 1_000);
		epoch.record(TWO_HOURS_IN, 5_000);
		HorizonLedger::new(Arc::new(ScriptedFloors), epoch)
	}

	fn now() -> DateTime {
		TWO_HOURS_IN.to_datetime()
	}

	fn one_hour() -> Duration {
		Duration::from_hours(1).expect("one hour is representable")
	}

	#[test]
	fn a_clock_driven_term_is_exactly_one_that_resolves_to_an_instant() {
		// The retention alarm splits on is_clock_driven: a term classified that way can never be
		// reported as pinned, because `now - ttl` is arithmetic no other party contributes to. A
		// version floor is somebody else's progress - a reader, a lease, a consumer, an unflushed
		// write - and classifying one of those as clock-driven would silently retire the only alarm
		// that catches it being held down. This ties the classification to what the ledger actually
		// returns, so the two cannot drift apart.
		let ledger = ledger();

		for term in FloorTerm::all() {
			let resolved = ledger.term(*term, now(), Some(one_hour()));
			match resolved {
				Some(Floor::Instant(_)) => assert!(
					term.is_clock_driven(),
					"{term} resolves to an instant off the clock but is not classified \
					 clock-driven, so a floor that cannot be pinned would still be alarmed on"
				),
				Some(Floor::Version(_)) => assert!(
					!term.is_clock_driven(),
					"{term} resolves to a version derived from another party's progress, so \
					 classifying it clock-driven retires the alarm that catches it wedged"
				),
				None => panic!("{term} resolved no floor; the fixture must place every term"),
			}
		}
	}
}
