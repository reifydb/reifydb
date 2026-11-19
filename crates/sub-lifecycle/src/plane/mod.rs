// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod horizon;
pub mod ledger;
pub mod measured;

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	lifecycle::{
		class::{Floor, FloorTerm, LifecycleClass},
		floor::FlushFloor,
		metrics::{ClassSnapshot, LifecycleMetrics, StuckOnset},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};
use reifydb_value::value::{datetime::DateTime, duration::Duration};
use tracing::warn;

use crate::plane::ledger::{EngineFloors, FloorSource, HorizonLedger};

#[derive(Clone)]
pub struct LifecyclePlane {
	inner: Arc<Inner>,
}

struct Inner {
	ledger: HorizonLedger,
	metrics: LifecycleMetrics,
}

impl LifecyclePlane {
	pub fn new(source: Arc<dyn FloorSource>, epoch: VersionEpoch) -> Self {
		Self::with_metrics(source, epoch, LifecycleMetrics::new())
	}

	pub fn with_metrics(source: Arc<dyn FloorSource>, epoch: VersionEpoch, metrics: LifecycleMetrics) -> Self {
		Self {
			inner: Arc::new(Inner {
				ledger: HorizonLedger::new(source, epoch),
				metrics,
			}),
		}
	}

	pub fn for_engine(engine: &StandardEngine, metrics: LifecycleMetrics) -> Self {
		Self::with_metrics(Arc::new(EngineFloors::new(engine.clone())), engine.version_epoch().clone(), metrics)
	}

	pub fn cutoff(&self, class: LifecycleClass, now: DateTime, ttl: Option<Duration>) -> Option<Floor> {
		self.inner.ledger.cutoff(class, now, ttl)
	}

	pub fn cutoff_with_binding(
		&self,
		class: LifecycleClass,
		now: DateTime,
		ttl: Option<Duration>,
	) -> Option<(Floor, FloorTerm)> {
		self.inner.ledger.cutoff_with_binding(class, now, ttl)
	}

	pub fn expiry_cutoff(&self, now: DateTime, ttl: Duration) -> Option<CommitVersion> {
		self.inner.ledger.expiry_cutoff(now, ttl)
	}

	pub fn record_liveness(&self, class: LifecycleClass) {
		self.inner.metrics.record_liveness(class);
	}

	pub fn record_reclamation(
		&self,
		class: LifecycleClass,
		floor: Option<(Floor, FloorTerm)>,
		work_done: u64,
		backlog_hint: u64,
	) {
		match self.inner.metrics.record_reclamation(class, floor, work_done, backlog_hint) {
			StuckOnset::Quiet => {}
			StuckOnset::FloorPinned {
				floor,
				binding,
				backlog_hint,
			} => warn!(
				class = class.name(),
				floor = %floor,
				binding = %binding,
				protects = binding.protects(),
				backlog = backlog_hint,
				"lifecycle class has eligible work but its floor will not advance"
			),
			StuckOnset::Starved {
				binding,
				backlog_hint,
			} => warn!(
				class = class.name(),
				binding = %binding,
				ages_by = binding.protects(),
				backlog = backlog_hint,
				"lifecycle class has not reclaimed anything while its backlog persisted"
			),
			StuckOnset::FloorUnresolvable => warn!(
				class = class.name(),
				"lifecycle class has no resolvable floor; it can reclaim nothing"
			),
		}
	}

	pub fn record_budget_exhausted(&self, class: LifecycleClass) {
		self.inner.metrics.record_budget_exhausted(class);
	}

	pub fn record_gated(&self, class: LifecycleClass) {
		self.inner.metrics.record_gated(class);
	}

	pub fn snapshot(&self, class: LifecycleClass) -> ClassSnapshot {
		self.inner.metrics.snapshot(class)
	}

	pub fn report(&self) -> Vec<(LifecycleClass, ClassSnapshot)> {
		self.inner.metrics.report()
	}

	pub fn flush_floor(&self, clock: Clock) -> Arc<dyn FlushFloor> {
		Arc::new(PlaneFlushFloor {
			plane: self.clone(),
			clock,
		})
	}
}

struct PlaneFlushFloor {
	plane: LifecyclePlane,
	clock: Clock,
}

impl FlushFloor for PlaneFlushFloor {
	fn floor(&self) -> CommitVersion {
		let now = self.clock.now();
		match self.plane.cutoff(LifecycleClass::PersistentFlush, now, None) {
			Some(Floor::Version(version)) => version,
			Some(Floor::Instant(_)) | None => CommitVersion(0),
		}
	}
}
