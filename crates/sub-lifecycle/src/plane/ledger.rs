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
		durable_frontier(
			EvictionWatermark::watermark(&self.engine),
			self.engine.multi().store().commit().oldest_pending_version(),
		)
	}

	fn owning_flow_checkpoint(&self) -> CommitVersion {
		self.engine.flow_watermark()
	}
}

fn durable_frontier(permitted: CommitVersion, oldest_pending: Option<CommitVersion>) -> CommitVersion {
	match oldest_pending {
		Some(oldest) => permitted.min(CommitVersion(oldest.0.saturating_sub(1))),
		None => permitted,
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
	fn a_clock_driven_term_is_exactly_one_that_resolves_to_an_instant() {
		// The retention alarm splits on is_clock_driven: a term classified that way can never be
		// reported as pinned, because `now - ttl` is arithmetic no other party contributes to. A
		// version floor is somebody else's progress - a reader, a lease, a consumer, an unflushed
		// write - and classifying one of those as clock-driven would silently retire the only alarm
		// that catches it being held down. This ties the classification to what the ledger actually
		// returns, so the two cannot drift apart.
		let ledger = ledger(10);

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

	#[test]
	fn an_unflushed_write_holds_the_frontier_below_the_permitted_watermark() {
		// This is the whole point of the term: FlushWatermark protects "a write that has not yet
		// reached the persistent tier". The permitted watermark only says how far the flusher is
		// allowed to go, and it is budgeted, so it runs ahead of what is actually durable. Returning
		// the permitted value here lets TombstoneReap physically delete rows that a still-buffered
		// write at version 1013 is about to rewrite - a resurrected removal.
		let frontier = durable_frontier(CommitVersion(1178), Some(CommitVersion(1013)));

		assert_eq!(
			frontier,
			CommitVersion(1012),
			"the frontier must stop one version below the oldest un-flushed write"
		);
	}

	#[test]
	fn a_drained_buffer_does_not_lift_the_frontier_above_the_permitted_watermark() {
		// With nothing buffered it is tempting to call everything durable, but a commit that has
		// already been allocated a version and has not yet reached the buffer is invisible here. The
		// permitted watermark trails those in-flight commits, so it stays the ceiling.
		let frontier = durable_frontier(CommitVersion(500), None);

		assert_eq!(frontier, CommitVersion(500));
	}

	#[test]
	fn a_buffer_holding_only_recent_writes_does_not_lower_the_frontier() {
		// Steady state: the flusher has drained everything it is allowed to touch and the buffer only
		// holds versions above the watermark. Clamping here would stall reclamation on a healthy
		// system, so the permitted watermark must win.
		let frontier = durable_frontier(CommitVersion(500), Some(CommitVersion(900)));

		assert_eq!(frontier, CommitVersion(500));
	}

	#[test]
	fn a_pending_write_at_the_first_version_floors_the_frontier_at_zero() {
		// Version 0 is the "nothing reclaimable" sentinel every class already understands. Wrapping
		// to u64::MAX here would invert the guard into unrestricted deletion.
		let frontier = durable_frontier(CommitVersion(500), Some(CommitVersion(0)));

		assert_eq!(frontier, CommitVersion(0));
	}
}
