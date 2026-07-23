// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Per-class horizon floors (decision B3).
//!
//! Each class asks the ledger for its cutoff, and the ledger composes it from ONLY the terms that class declares in
//! [`RetentionClass::floor_terms`]. That is the whole mechanism: a reader can pin a class only if the class named
//! that reader, so a wedged CDC consumer stalls CDC truncation and nothing else, while version history - which that
//! consumer really does read - stays protected.
//!
//! A term that cannot be resolved yields no cutoff rather than an unbounded one. None means "no safe floor known,
//! reclaim nothing", never "reclaim everything"; every executor treats it that way, and the classes whose floor
//! sits at zero simply do no work.
//!
//! Two terms look alike and are not. A CDC LOG consumer reads cdc.db and constrains only CDC truncation. A
//! SUBSCRIPTION worker leases its own lag position and reads rows out of the multi store at that version, so it
//! constrains buffer history and flush. They were one term until the difference was traced through the dispatch
//! path; keeping them separate is what lets a stalled subscription stop exactly the two classes it really pins.

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

pub struct HorizonLedger<S: FloorSource> {
	source: S,
	epoch: VersionEpoch,
}

impl<S: FloorSource> HorizonLedger<S> {
	pub fn new(source: S, epoch: VersionEpoch) -> Self {
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

	/// Returns the cutoff together with the term that produced it. A stuck class is only actionable once the
	/// binding constraint is named: "buffer-historical-gc is stuck on subscription-snapshot" points at a lagging
	/// subscription, while the same class stuck on lease-min points at a leaked operator lease. Reporting only
	/// that a class is stuck leaves an operator to guess between them.
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
		self.engine.cdc_consumer_watermark()
	}

	fn subscription_snapshot(&self) -> CommitVersion {
		self.engine.multi().consumer_watermark()
	}

	fn flush_watermark(&self) -> CommitVersion {
		EvictionWatermark::watermark(&self.engine)
	}
}
